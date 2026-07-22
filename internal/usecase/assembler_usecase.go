package usecase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type AssemblerUseCase struct {
	storage     *storage.Client
	segments    *cache.SegmentRegistry
	sessions    *cache.SessionRegistry
	publisher   domain.EventPublisher
	gracePeriod time.Duration
	logger      *zap.Logger
	workDir     string
	sem         chan struct{}
	inFlight    sync.Map // streamID -> struct{}, guards against completion+timeout racing on the same jobDir
}

var errRecordingStatePublish = errors.New("publish recording state")

func NewAssemblerUseCase(
	storage *storage.Client,
	segments *cache.SegmentRegistry,
	sessions *cache.SessionRegistry,
	publisher domain.EventPublisher,
	gracePeriod time.Duration,
	logger *zap.Logger,
) *AssemblerUseCase {
	workDir := os.Getenv("ASSEMBLER_WORK_DIR")
	if workDir == "" {
		workDir = "/var/tmp/vox-assembly"
	}
	maxConcurrent := 3
	return &AssemblerUseCase{
		storage:     storage,
		segments:    segments,
		sessions:    sessions,
		publisher:   publisher,
		gracePeriod: gracePeriod,
		logger:      logger,
		workDir:     workDir,
		sem:         make(chan struct{}, maxConcurrent),
	}
}

// OnStreamEnded is the completion/timeout trigger for the assembler consumer
// (see main.go's handleAssembly). It never blocks the caller for the full
// grace period — the Kafka consumer this feeds is a single sequential
// goroutine, and blocking it would queue up every other student's
// stream.ended behind this one.
func (u *AssemblerUseCase) OnStreamEnded(ctx context.Context, event domain.StreamEndedEvent) error {
	complete, err := u.segments.IsComplete(ctx, event.StreamID)
	if err != nil {
		return err // infra error - let Kafka retry
	}
	if complete {
		return u.AssembleRequested(ctx, recordingRequestFromStreamEnded(event))
	}

	time.AfterFunc(u.gracePeriod, func() {
		if err := u.AssembleRequested(context.Background(), recordingRequestFromStreamEnded(event)); err != nil {
			u.logger.Error("fallback assembly failed",
				zap.String("streamId", event.StreamID),
				zap.Error(err),
			)
		}
	})
	return nil
}

func (u *AssemblerUseCase) Assemble(ctx context.Context, scheduleID, sessionID, streamID string) error {
	session, _ := u.sessions.LookupUpload(ctx, streamID)
	event := domain.RecordingAssemblyRequestedEvent{
		StreamID: streamID, ScheduleID: scheduleID, SessionID: sessionID,
		Source: "DESKTOP_SEGMENT_UPLOAD", RequestedAt: time.Now().UTC(),
	}
	if session != nil {
		event.ParticipantID = session.CandidateID
		event.StreamType = session.StreamType
	}
	return u.AssembleRequested(ctx, event)
}

func (u *AssemblerUseCase) AssembleRequested(ctx context.Context, event domain.RecordingAssemblyRequestedEvent) error {
	err := u.assemble(ctx, event)
	if err == nil {
		return nil
	}
	if errors.Is(err, errRecordingStatePublish) {
		return err
	}
	publishErr := u.publishRecordingState(ctx, event, "FAILED", "", 0, false, err.Error())
	if publishErr != nil {
		return fmt.Errorf("assemble recording: %v; publish failure state: %w", err, publishErr)
	}
	return err
}

func (u *AssemblerUseCase) assemble(ctx context.Context, event domain.RecordingAssemblyRequestedEvent) error {
	scheduleID, sessionID, streamID := event.ScheduleID, event.SessionID, event.StreamID
	if _, alreadyRunning := u.inFlight.LoadOrStore(streamID, struct{}{}); alreadyRunning {
		return nil // completion and timeout triggers raced - the other one owns this jobDir
	}
	defer u.inFlight.Delete(streamID)

	metas, err := u.segments.List(ctx, streamID)
	if err != nil {
		return fmt.Errorf("list segments: %w", err)
	}
	if len(metas) == 0 {
		return fmt.Errorf("no segments uploaded for stream %s", streamID)
	}

	select {
	case u.sem <- struct{}{}:
		defer func() { <-u.sem }()
	case <-ctx.Done():
		return ctx.Err()
	}

	log := u.logger.With(
		zap.String("streamId", streamID),
		zap.String("scheduleId", scheduleID),
		zap.Int("segmentCount", len(metas)),
	)

	// Idempotency check
	exists, err := u.storage.RecordingExists(ctx, scheduleID, sessionID, streamID)
	if err != nil {
		return fmt.Errorf("check existing recording: %w", err)
	}
	if exists {
		durationSecs, hasGaps := recordingSummary(metas)
		if err := u.publishRecordingState(ctx, event, recordingStatus(hasGaps), storage.FinalRecordingKey(scheduleID, sessionID, streamID), durationSecs, hasGaps, ""); err != nil {
			return fmt.Errorf("%w: %v", errRecordingStatePublish, err)
		}
		log.Info("recording already assembled; state republished")
		return nil
	}

	gaps, _ := auditGaps(metas)
	if len(gaps) > 0 {
		log.Warn("segment gaps detected, assembling best-effort anyway", zap.Int("gapCount", len(gaps)))
	}

	jobDir := filepath.Join(u.workDir, streamID)
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		return fmt.Errorf("create job dir: %w", err)
	}
	defer os.RemoveAll(jobDir) // cleanup for both fail and success

	estimatedBytes := uint64(len(metas)) * 20 * 1024 * 1024 * 2 // 20MB x 2 for output
	if err := u.checkDiskSpace(u.workDir, estimatedBytes); err != nil {
		return fmt.Errorf("pre-flight disk check: %w", err)
	}

	log.Info("starting assembly")

	keys := make([]string, len(metas))
	for i, m := range metas {
		keys[i] = m.S3Key
	}

	// Download segments parallel
	if err := u.downloadSegments(ctx, keys, jobDir); err != nil {
		return fmt.Errorf("download segments: %w", err)
	}

	localFiles, err := filepath.Glob(filepath.Join(jobDir, "*.mp4"))
	if err != nil {
		return fmt.Errorf("glob segments: %w", err)
	}
	if len(localFiles) == 0 {
		return fmt.Errorf("no segments downloaded for stream %s", streamID)
	}
	sort.Strings(localFiles)

	// write concat list
	concatPath := filepath.Join(jobDir, "concat_list.txt")
	if err := writeConcatList(concatPath, localFiles); err != nil {
		return fmt.Errorf("write concat list: %w", err)
	}

	outputPath := filepath.Join(jobDir, "recording.mp4")
	if err := u.concat(ctx, concatPath, outputPath); err != nil {
		return fmt.Errorf("ffmpeg concat: %w", err)
	}

	f, err := os.Open(outputPath)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer f.Close()

	recordingKey, err := u.storage.UploadFinalRecording(ctx, scheduleID, sessionID, streamID, f)
	if err != nil {
		return fmt.Errorf("upload final recording: %w", err)
	}

	durationSecs, hasGaps := recordingSummary(metas)
	if err := u.publishRecordingState(ctx, event, recordingStatus(hasGaps), recordingKey, durationSecs, hasGaps, ""); err != nil {
		return fmt.Errorf("%w: %v", errRecordingStatePublish, err)
	}

	log.Info("assembly completed", zap.String("recordingKey", recordingKey))
	return nil
}

func recordingSummary(metas []cache.SegmentMeta) (int64, bool) {
	gaps, _ := auditGaps(metas)
	return int64(metas[len(metas)-1].EndedAt.Sub(metas[0].StartedAt).Seconds()), len(gaps) > 0
}

func recordingStatus(hasGaps bool) string {
	if hasGaps {
		return "PARTIAL"
	}
	return "READY"
}

func (u *AssemblerUseCase) publishRecordingState(ctx context.Context, request domain.RecordingAssemblyRequestedEvent, status, objectKey string, durationSecs int64, hasGaps bool, errorMessage string) error {
	eventID, err := uuid.NewV7()
	if err != nil {
		return err
	}
	return u.publisher.PublishRecordingPartChanged(ctx, domain.RecordingPartChangedEvent{
		EventID: eventID.String(), StreamID: request.StreamID, ScheduleID: request.ScheduleID,
		SessionID: request.SessionID, ParticipantID: request.ParticipantID,
		StreamType: request.StreamType, Source: request.Source, Status: status,
		ObjectKey: objectKey, DurationSecs: durationSecs, HasGaps: hasGaps,
		ErrorMessage: errorMessage, OccurredAt: time.Now().UTC(),
	})
}

func recordingRequestFromStreamEnded(event domain.StreamEndedEvent) domain.RecordingAssemblyRequestedEvent {
	return domain.RecordingAssemblyRequestedEvent{
		EventID: event.EventID, StreamID: event.StreamID, ScheduleID: event.ScheduleID,
		SessionID: event.SessionID, ParticipantID: event.ParticipantID,
		StreamType: event.StreamType, Source: "SERVER_WEBRTC", RequestedAt: event.EndedAt,
	}
}

func (u *AssemblerUseCase) downloadSegments(ctx context.Context, keys []string, dir string) error {
	sem := make(chan struct{}, 6) // max 6 concurrent downloads
	g, ctx := errgroup.WithContext(ctx)

	for i, key := range keys {
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			dstPath := filepath.Join(dir, fmt.Sprintf("%04d.mp4", i))
			return u.storage.DownloadSegmentToFile(ctx, key, dstPath)
		})
	}
	return g.Wait()
}

func (u *AssemblerUseCase) concat(ctx context.Context, concatPath, outputPath string) error {
	var errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-loglevel", "error",
		"-f", "concat", "-safe", "0",
		"-i", concatPath,
		"-c", "copy",
		"-movflags", "faststart",
		"-y", outputPath,
	)
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, errBuf.String())
	}
	return nil
}

func (u *AssemblerUseCase) checkDiskSpace(dir string, requiredBytes uint64) error {
	available, err := util.AvailableDiskSpace(dir)
	if err != nil {
		return nil
	}
	if available < requiredBytes {
		return fmt.Errorf("insufficient disk space: need %dMB, have %dMB", requiredBytes/1024/1024, available/1024/1024)
	}
	return nil
}

func writeConcatList(path string, files []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, file := range files {
		// The concat demuxer resolves each entry relative to the directory of this
		// list file, and the list + all segments live in the same job dir. Write
		// basenames only: writing absolute paths breaks on Windows, where a
		// drive-less path (\var\tmp\...) is treated as relative and the list dir is
		// prepended, producing a doubled, non-existent path.
		name := filepath.Base(file)
		escaped := strings.ReplaceAll(name, "'", "'\\''")
		fmt.Fprintf(f, "file '%s'\n", escaped)
	}
	return nil
}
