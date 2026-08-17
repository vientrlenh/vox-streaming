package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/media"
	"github.com/vientrlenh/vox-streaming/internal/stream"
	"github.com/vientrlenh/vox-streaming/internal/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type AssemblerUseCase struct {
	storage   *storage.Client
	segments  *cache.SegmentRegistry
	inventory *cache.InventoryRegistry
	sessions  *cache.SessionRegistry
	// Where the grace period is served out. Required, not optional: it is the only reason a WebRTC
	// recording survives this process being restarted or this consumer being dead.
	pendingAssembly *cache.PendingAssemblyRegistry
	publisher       domain.EventPublisher
	gracePeriod     time.Duration
	logger          *zap.Logger
	workDir         string
	sem             chan struct{}
	inFlight        sync.Map // streamID -> struct{}, guards against completion+timeout racing on the same jobDir
}

var errRecordingStatePublish = errors.New("publish recording state")

// Only ever applied to audio that has to be re-encoded anyway (see concat). Matches the HLS
// output's bitrate in internal/recorder, so the two AAC copies of the same stream sound alike.
const assemblyAudioBitrateK = 128

func NewAssemblerUseCase(
	storage *storage.Client,
	segments *cache.SegmentRegistry,
	inventory *cache.InventoryRegistry,
	sessions *cache.SessionRegistry,
	pendingAssembly *cache.PendingAssemblyRegistry,
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
		storage:         storage,
		segments:        segments,
		inventory:       inventory,
		sessions:        sessions,
		pendingAssembly: pendingAssembly,
		publisher:       publisher,
		gracePeriod:     gracePeriod,
		logger:          logger,
		workDir:         workDir,
		sem:             make(chan struct{}, maxConcurrent),
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
		if err := u.AssembleRequested(ctx, recordingRequestFromStreamEnded(event)); err != nil {
			return err
		}
		// Disarm only after the recording exists. Doing it earlier would open a window in which
		// neither this call nor the watchdog owns the stream; doing it late is harmless because
		// assemble short-circuits on a recording that is already there.
		u.disarmWatchdog(ctx, event.StreamID)
		return nil
	}

	// Not every segment has landed yet, so wait out the grace period -- but wait for it in Redis
	// rather than in a time.AfterFunc. An in-process timer is discarded by every deploy, restart and
	// crash, silently stranding whichever recordings were mid-grace at that moment, and it also does
	// nothing at all if this consumer is not the one that ends up alive. Handing the deadline to the
	// watchdog's due-set makes the wait survive all three.
	if err := u.pendingAssembly.Schedule(ctx, cache.PendingAssembly{
		StreamID:      event.StreamID,
		ScheduleID:    event.ScheduleID,
		SessionID:     event.SessionID,
		ParticipantID: event.ParticipantID,
		StreamType:    event.StreamType,
		DueAt:         time.Now().UTC().Add(u.gracePeriod),
		Source:        cache.AssemblySourceWebRTC,
	}); err != nil {
		// Returned, not swallowed: Kafka retrying this message is exactly the right recovery, and
		// the entry armed when the peer connected is still in place as the outer floor.
		return fmt.Errorf("schedule grace-period assembly for %s: %w", event.StreamID, err)
	}
	return nil
}

func (u *AssemblerUseCase) disarmWatchdog(ctx context.Context, streamID string) {
	if err := u.pendingAssembly.Cancel(ctx, streamID); err != nil {
		u.logger.Warn("disarm assembly watchdog failed; it may assemble this stream again later, which is idempotent",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
	}
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
		durationSecs, hasGaps, _ := u.loadSummary(ctx, streamID, metas)
		if err := u.publishRecordingState(ctx, event, recordingStatus(hasGaps), storage.FinalRecordingKey(scheduleID, sessionID, streamID), durationSecs, hasGaps, ""); err != nil {
			return fmt.Errorf("%w: %v", errRecordingStatePublish, err)
		}
		log.Info("recording already assembled; state republished")
		return nil
	}

	durationSecs, hasGaps, coverage := u.loadSummary(ctx, streamID, metas)
	logCoverage(log, coverage)
	if hasGaps {
		log.Warn("segment gaps detected, assembling best-effort anyway",
			zap.Int("missingCount", len(coverage.MissingSeqs)))
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

	// Probed before concat, not after, because the answer decides how concat is invoked. Any
	// failure here is non-fatal -- needsAudioTranscode falls back to the copy-everything behaviour
	// this had before.
	audioCodec := ""
	if inputReport, err := media.Probe(ctx, localFiles[0]); err != nil {
		log.Warn("could not probe segment audio codec; copying audio through unchanged",
			zap.String("segment", filepath.Base(localFiles[0])),
			zap.Error(err),
		)
	} else {
		audioCodec = inputReport.Audio.Codec
	}

	outputPath := filepath.Join(jobDir, "recording.mp4")
	if err := u.concat(ctx, concatPath, outputPath, audioCodec); err != nil {
		return fmt.Errorf("ffmpeg concat: %w", err)
	}

	// Best-effort: LookupUpload deletes and errors on an expired session, which is the normal
	// state by the time a watchdog-driven assembly runs. Its absence costs the duration check and
	// the stop reason, never the recording.
	uploadSession, sessionErr := u.sessions.LookupUpload(ctx, streamID)
	if sessionErr != nil {
		uploadSession = nil
	}
	if uploadSession != nil && uploadSession.StopReason != "" {
		log = log.With(zap.String("stopReason", uploadSession.StopReason))
	}

	quality := u.inspectRecording(ctx, log, outputPath, coverage, uploadSession)

	f, err := os.Open(outputPath)
	if err != nil {
		return fmt.Errorf("open output: %w", err)
	}
	defer f.Close()

	recordingKey, err := u.storage.UploadFinalRecording(ctx, scheduleID, sessionID, streamID, f)
	if err != nil {
		return fmt.Errorf("upload final recording: %w", err)
	}

	u.storeQualityReport(ctx, log, event, recordingKey, quality, coverage, uploadSession)

	if err := u.publishRecordingState(ctx, event, recordingStatus(hasGaps), recordingKey, durationSecs, hasGaps, ""); err != nil {
		return fmt.Errorf("%w: %v", errRecordingStatePublish, err)
	}

	log.Info("assembly completed", zap.String("recordingKey", recordingKey))
	return nil
}

// recordingSummary reports how long the recording covers and whether anything is missing from it.
//
// Both figures come from the client's declared inventory when there is one. Deriving them from the
// received segments alone -- last.EndedAt minus first.StartedAt, plus interior timestamp gaps --
// silently launders away exactly the failures worth reporting: segments missing from the start or
// the end leave no interval to measure, so a recording truncated by a client that died mid-upload
// reports itself as complete and correctly sized.
func recordingSummary(
	inventory *cache.StreamInventory,
	metas []cache.SegmentMeta,
) (int64, bool, stream.StreamCoverage) {
	coverage := stream.Reconcile(inventory, metas)
	return int64(stream.RecordedDuration(inventory, metas).Seconds()), coverage.HasGaps(), coverage
}

// loadSummary is recordingSummary plus the cache read, kept apart so the decision logic above stays
// a pure function that can be tested without Redis.
func (u *AssemblerUseCase) loadSummary(
	ctx context.Context,
	streamID string,
	metas []cache.SegmentMeta,
) (int64, bool, stream.StreamCoverage) {
	inventory, err := u.inventory.Get(ctx, streamID)
	if err != nil {
		// Not fatal: fall back to the weaker heuristic rather than refuse to assemble a recording
		// over a cache read.
		u.logger.Warn("could not load segment inventory; falling back to timestamp-based gap detection",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
		inventory = nil
	}
	return recordingSummary(inventory, metas)
}

// logCoverage surfaces what the reconciliation found. Reported, never acted on: a low frame rate or
// an empty segment has an innocent reading (a still screen, a paused exam) as readily as a damning
// one, and choosing between them is a human's job, not the assembler's.
func logCoverage(log *zap.Logger, coverage stream.StreamCoverage) {
	if len(coverage.MissingSeqs) > 0 {
		log.Warn("recording is missing segments the client declared",
			zap.Int("declaredSegments", coverage.DeclaredSegments),
			zap.Int("receivedSegments", coverage.ReceivedSegments),
			zap.Int64s("missingSeqs", coverage.MissingSeqs),
		)
	}

	for _, anomaly := range coverage.Anomalies {
		if anomaly.Kind == stream.AnomalyMissing {
			continue // already reported in aggregate above
		}
		log.Warn("recording capture anomaly",
			zap.Int64("seq", anomaly.Seq),
			zap.String("kind", anomaly.Kind),
			zap.String("detail", anomaly.Detail),
		)
	}

	if !coverage.HasInventory {
		log.Info("stream had no client inventory; gaps at the start or end of this recording cannot be detected")
	}
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

// concat joins the downloaded segments into the single recording.
//
// Video is always stream-copied: re-encoding the evidence a grade is based on would be both slow
// and lossy. Audio is copied too whenever it is already AAC, which covers every desktop-uploaded
// recording since the client's Media Foundation sink writer emits AAC.
//
// WebRTC-ingested segments carry Opus instead. Opus-in-MP4 is legal but Safari and QuickTime refuse
// to play it -- and that is the copy a teacher opens first, because it is the provisional recording
// available immediately after the exam while the client is still uploading. Transcoding only that
// case, once per stream at assembly, costs far less than transcoding the live ingest continuously,
// and leaves the authoritative desktop recording untouched by a needless second encode.
func (u *AssemblerUseCase) concat(ctx context.Context, concatPath, outputPath, audioCodec string) error {
	// Kept as a blanket "-c copy" with the audio overridden after it, rather than spelling out
	// "-c:v copy -c:a ...": a later option wins for the same stream specifier, so the copy path
	// stays byte-for-byte the command this ran before, including for any stream that is neither
	// video nor audio.
	args := []string{
		"-hide_banner", "-loglevel", "error",
		"-f", "concat", "-safe", "0",
		"-i", concatPath,
		"-c", "copy",
	}
	if needsAudioTranscode(audioCodec) {
		args = append(args, "-c:a", "aac", "-b:a", strconv.Itoa(assemblyAudioBitrateK)+"k")
	}
	args = append(args, "-movflags", "faststart", "-y", outputPath)

	var errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%w: %s", err, errBuf.String())
	}
	return nil
}

// needsAudioTranscode decides between copying and re-encoding the audio track.
//
// An unknown codec ("" -- the probe failed, or there is no audio at all) copies, deliberately: the
// previous behaviour was to copy unconditionally, so falling back to it cannot make a recording
// worse than it already was, whereas defaulting to a transcode would put a lossy generation on
// every recording whenever ffprobe happened to be unavailable.
func needsAudioTranscode(codec string) bool {
	return codec != "" && !strings.EqualFold(codec, "aac")
}

// RecordingQuality is everything the post-assembly checks could determine about a finished
// recording. It is returned rather than only logged so the source-selection work that follows can
// decide on these signals instead of re-deriving them; today the only consumer is the log.
//
// Every field has an explicit "was this measurable" companion, because absent and bad are different
// answers and collapsing them would make an unprobed recording look flawless.
type RecordingQuality struct {
	Probed       bool
	DurationSecs float64
	HasVideo     bool
	HasAudio     bool

	AudioMeasured bool
	AudioPeakDBFS float64
	AudioMeanDBFS float64
	Silent        bool

	// FramesCompared is false when there was no inventory to compare against, or the packet count
	// failed. DeclaredFrames/ActualPackets are only meaningful when it is true.
	FramesCompared bool
	DeclaredFrames int64
	ActualPackets  int64

	// WindowKnown is false when the upload session had already expired out of the cache by the
	// time assembly ran, which is normal for a watchdog-driven assembly.
	WindowKnown    bool
	WindowSecs     float64
	OverrunsWindow bool
}

const qualityReportSchemaVersion = 1

// RecordingQualityReport is the durable form of RecordingQuality: the same measurements plus enough
// identity to stand on their own, written beside the recording as quality.json.
//
// Kept as its own type rather than tagging RecordingQuality, for two reasons. The in-process struct
// is free to carry -Inf for a level that has no finite value, which encoding/json refuses outright
// -- one silent recording would fail the marshal and take the whole report with it. And a stored
// document is a contract with whoever reads it months later: it needs a schema version and stable
// field names, neither of which should pin the shape of a struct the assembler passes around.
type RecordingQualityReport struct {
	SchemaVersion int       `json:"schemaVersion"`
	MeasuredAt    time.Time `json:"measuredAt"`

	StreamID     string `json:"streamId"`
	ScheduleID   string `json:"scheduleId"`
	SessionID    string `json:"sessionId"`
	StreamType   string `json:"streamType,omitempty"`
	Source       string `json:"source"`
	RecordingKey string `json:"recordingKey"`
	// Absent means nobody reported a clean end to this stream, which is itself a finding -- see
	// cache.UploadSession.StopReason.
	StopReason string `json:"stopReason,omitempty"`

	Probed       bool     `json:"probed"`
	DurationSecs *float64 `json:"durationSecs"`
	HasVideo     bool     `json:"hasVideo"`
	HasAudio     bool     `json:"hasAudio"`

	AudioMeasured bool     `json:"audioMeasured"`
	AudioPeakDBFS *float64 `json:"audioPeakDbfs"`
	AudioMeanDBFS *float64 `json:"audioMeanDbfs"`
	Silent        bool     `json:"silent"`
	// Stored next to the verdict it produced, so an old report stays readable after the threshold
	// is retuned rather than silently meaning something else.
	SilenceThresholdDBFS float64 `json:"silenceThresholdDbfs"`

	FramesCompared bool  `json:"framesCompared"`
	DeclaredFrames int64 `json:"declaredFrames"`
	ActualPackets  int64 `json:"actualPackets"`

	WindowKnown    bool     `json:"windowKnown"`
	WindowSecs     *float64 `json:"windowSecs"`
	OverrunsWindow bool     `json:"overrunsWindow"`

	// The reconciliation the frame comparison was made against: what the client declared, what
	// arrived, and what looked wrong. Carried along because the frame numbers above mean nothing
	// without it.
	Coverage stream.StreamCoverage `json:"coverage"`
}

func newQualityReport(
	event domain.RecordingAssemblyRequestedEvent,
	recordingKey string,
	quality RecordingQuality,
	coverage stream.StreamCoverage,
	session *cache.UploadSession,
	measuredAt time.Time,
) RecordingQualityReport {
	report := RecordingQualityReport{
		SchemaVersion: qualityReportSchemaVersion,
		MeasuredAt:    measuredAt,

		StreamID:     event.StreamID,
		ScheduleID:   event.ScheduleID,
		SessionID:    event.SessionID,
		StreamType:   event.StreamType,
		Source:       event.Source,
		RecordingKey: recordingKey,

		Probed:   quality.Probed,
		HasVideo: quality.HasVideo,
		HasAudio: quality.HasAudio,

		AudioMeasured:        quality.AudioMeasured,
		Silent:               quality.Silent,
		SilenceThresholdDBFS: media.SilentPeakDBFS,

		FramesCompared: quality.FramesCompared,
		DeclaredFrames: quality.DeclaredFrames,
		ActualPackets:  quality.ActualPackets,

		WindowKnown:    quality.WindowKnown,
		OverrunsWindow: quality.OverrunsWindow,

		Coverage: coverage,
	}

	if session != nil {
		report.StopReason = session.StopReason
	}
	// Each measurement is emitted only when its own "was this measurable" flag says so, so an
	// unprobed recording reports null rather than a zero that reads as a real reading of zero.
	if quality.Probed {
		report.DurationSecs = jsonFloat(quality.DurationSecs)
	}
	if quality.AudioMeasured {
		report.AudioPeakDBFS = jsonFloat(quality.AudioPeakDBFS)
		report.AudioMeanDBFS = jsonFloat(quality.AudioMeanDBFS)
	}
	if quality.WindowKnown {
		report.WindowSecs = jsonFloat(quality.WindowSecs)
	}
	return report
}

// jsonFloat renders a measurement as a JSON number, or null when it has no finite value.
//
// volumedetect reports digital silence as -inf, and encoding/json fails the entire document on an
// infinity rather than dropping the one field -- so without this, the recordings most worth having
// a report about are exactly the ones that would not get one. null is also the more honest
// encoding: any number substituted for it would be read back as a real measured level.
func jsonFloat(v float64) *float64 {
	if math.IsInf(v, 0) || math.IsNaN(v) {
		return nil
	}
	return &v
}

// storeQualityReport writes the measured signals beside the recording they describe.
//
// Runs after the recording is safely uploaded, and never returns an error. The ordering and the
// silence are the same judgement: a report is an explanation of the evidence, so failing an
// assembly to protect it would trade the evidence for the explanation. A recording with no report
// is worth far more than a report with no recording.
func (u *AssemblerUseCase) storeQualityReport(
	ctx context.Context,
	log *zap.Logger,
	event domain.RecordingAssemblyRequestedEvent,
	recordingKey string,
	quality RecordingQuality,
	coverage stream.StreamCoverage,
	session *cache.UploadSession,
) {
	report := newQualityReport(event, recordingKey, quality, coverage, session, time.Now().UTC())

	// Indented because this is read by people, one file at a time, while working out what happened
	// to a single exam -- never bulk-parsed where the extra bytes would matter.
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Warn("could not encode recording quality report", zap.Error(err))
		return
	}

	key, err := u.storage.UploadQualityReport(ctx, event.ScheduleID, event.SessionID, event.StreamID, data)
	if err != nil {
		log.Warn("could not store recording quality report; the signals survive only in this log",
			zap.Error(err))
		return
	}
	log.Info("recording quality report stored", zap.String("qualityKey", key))
}

const (
	// A recording may legitimately hold slightly fewer packets than the client counted frames:
	// concat drops a duplicated parameter-set packet at each segment boundary, and the client
	// counts a frame as written the moment it hands it to the sink writer, which may still have
	// one in flight when the segment closes. Ten percent is far above that and far below the
	// scale of a real capture stall.
	frameShortfallTolerance = 0.10

	// The recorded span is compared against the time the candidate actually had. Sixty seconds of
	// slack absorbs clock skew between client and server plus the segment that was mid-write when
	// the window closed.
	durationOverrunGrace = 60 * time.Second

	// Below this fraction of the available window the duration is reported but not judged: a
	// candidate who finished early is indistinguishable from one who was cut off, and only the
	// first of those is common.
	durationUnderrunNotice = 0.5
)

// inspectRecording runs the quality checks on the finished file, immediately before it is uploaded.
//
// Nothing here fails the assembly. A recording that is silent or missing a track is still the only
// record of that exam, so destroying it over a quality verdict would be strictly worse than keeping
// it and saying so loudly.
//
// Silence and a missing audio track are logged at Error while the rest are Warn, because the
// innocent-reading argument that keeps capture anomalies advisory (see logCoverage) does not apply
// to them: a still screen or a paused exam explains a low frame rate, but nothing explains twenty
// minutes of an oral exam with no sound in it.
func (u *AssemblerUseCase) inspectRecording(
	ctx context.Context,
	log *zap.Logger,
	path string,
	coverage stream.StreamCoverage,
	session *cache.UploadSession,
) RecordingQuality {
	quality := RecordingQuality{}

	report, err := media.Probe(ctx, path)
	if err != nil {
		log.Warn("could not probe assembled recording; quality signals unavailable", zap.Error(err))
		return quality
	}

	quality.Probed = true
	quality.DurationSecs = report.DurationSecs
	quality.HasVideo = report.Video.Present
	quality.HasAudio = report.Audio.Present

	log.Info("assembled recording probed",
		zap.Float64("durationSecs", report.DurationSecs),
		zap.String("videoCodec", report.Video.Codec),
		zap.Int("width", report.Video.Width),
		zap.Int("height", report.Video.Height),
		zap.Float64("avgFrameRate", report.Video.AvgFrameRate),
		zap.String("audioCodec", report.Audio.Codec),
	)

	if !report.Video.Present {
		log.Warn("assembled recording has no video track")
	}
	if report.DurationSecs <= 0 {
		log.Warn("assembled recording reports no duration; it may be unplayable",
			zap.Float64("durationSecs", report.DurationSecs))
	}

	u.checkFrameCount(ctx, log, path, coverage, &quality)
	checkDurationWindow(log, session, &quality)

	if !report.Audio.Present {
		// For an oral exam this is total loss, not a degradation: there is nothing left to grade.
		log.Error("assembled recording has no audio track; nothing in it can be graded")
		return quality
	}

	level, err := media.MeasureAudioLevel(ctx, path)
	if err != nil {
		// This pass decodes every sample, so a failure here also means the audio track is present
		// but not actually decodable -- worth more than a missing measurement.
		log.Warn("could not measure audio level; the audio track may be undecodable", zap.Error(err))
		return quality
	}

	quality.AudioMeasured = true
	quality.AudioPeakDBFS = level.PeakDBFS
	quality.AudioMeanDBFS = level.MeanDBFS
	quality.Silent = level.Silent()

	if quality.Silent {
		log.Error("assembled recording is silent; the microphone captured nothing",
			zap.Float64("peakDBFS", level.PeakDBFS),
			zap.Float64("meanDBFS", level.MeanDBFS),
			zap.Float64("silenceThresholdDBFS", media.SilentPeakDBFS),
		)
		return quality
	}

	log.Info("assembled recording audio level",
		zap.Float64("peakDBFS", level.PeakDBFS),
		zap.Float64("meanDBFS", level.MeanDBFS),
	)
	return quality
}

// checkFrameCount compares the frames the client counted writing against what is actually in the
// file, which is the only way to catch a capture that froze: the segments are all present, all
// correctly sized and all the right duration, and the picture in them does not move.
func (u *AssemblerUseCase) checkFrameCount(
	ctx context.Context,
	log *zap.Logger,
	path string,
	coverage stream.StreamCoverage,
	quality *RecordingQuality,
) {
	if coverage.DeclaredFrames <= 0 {
		// No inventory, or a client old enough not to send frame counts. Nothing to compare
		// against, and counting packets would cost a full file walk to learn nothing.
		return
	}

	packets, err := media.CountPackets(ctx, path)
	if err != nil {
		log.Warn("could not count packets in assembled recording; frame check skipped", zap.Error(err))
		return
	}

	quality.FramesCompared = true
	quality.DeclaredFrames = coverage.DeclaredFrames
	quality.ActualPackets = packets

	shortfall := float64(coverage.DeclaredFrames-packets) / float64(coverage.DeclaredFrames)
	if shortfall > frameShortfallTolerance {
		log.Warn("assembled recording holds fewer frames than the client counted writing",
			zap.Int64("declaredFrames", coverage.DeclaredFrames),
			zap.Int64("actualPackets", packets),
			zap.Float64("shortfall", shortfall),
		)
	}
}

// checkDurationWindow compares the recording against the time the candidate actually had.
//
// Only the overrun is treated as a finding. A recording longer than the window the upload
// credentials covered cannot happen legitimately, so it always means something is wrong -- a
// mis-set clock, segments from another attempt, a bad concat. The underrun has the opposite
// character: finishing early is the ordinary case, so it is reported as a number and left alone.
func checkDurationWindow(log *zap.Logger, session *cache.UploadSession, quality *RecordingQuality) {
	if session == nil || session.CreatedAt.IsZero() || session.ExpiresAt.IsZero() {
		return
	}
	window := session.ExpiresAt.Sub(session.CreatedAt)
	if window <= 0 {
		return
	}

	quality.WindowKnown = true
	quality.WindowSecs = window.Seconds()

	recorded := time.Duration(quality.DurationSecs * float64(time.Second))
	if recorded > window+durationOverrunGrace {
		quality.OverrunsWindow = true
		log.Warn("assembled recording is longer than the window its upload credentials covered",
			zap.Float64("durationSecs", quality.DurationSecs),
			zap.Float64("windowSecs", window.Seconds()),
		)
		return
	}

	if recorded < time.Duration(float64(window)*durationUnderrunNotice) {
		log.Info("assembled recording is well short of the available window; this is normal when a candidate finishes early",
			zap.Float64("durationSecs", quality.DurationSecs),
			zap.Float64("windowSecs", window.Seconds()),
		)
	}
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
