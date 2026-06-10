package usecase

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"

	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	examgrpc "github.com/vientrlenh/vox-streaming/internal/transport/grpc/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type AssemblerUseCase struct {
	storage *storage.Client
	examClient *examgrpc.ExamClient
	logger *zap.Logger
	workDir string
}

func NewAssemblerUseCase(storage *storage.Client, examClient *examgrpc.ExamClient, logger *zap.Logger) *AssemblerUseCase {
	workDir := os.Getenv("ASSEMBLER_WORK_DIR")
	if workDir == "" {
		workDir = filepath.Join(os.TempDir(), "vox-assembly")
	}
	return &AssemblerUseCase{
		storage: storage, 
		examClient: examClient, 
		logger: logger,
		workDir: workDir,
	}
}

func (u *AssemblerUseCase) Assemble(ctx context.Context, event domain.StreamEndedEvent) error {
	if len(event.SegmentKeys) == 0 {
		u.logger.Debug("no server segments, skipping assembly", zap.String("stream_id", event.StreamID))
		return nil
	}

	log := u.logger.With(
		zap.String("stream_id", event.StreamID), 
		zap.String("room_id", event.RoomID), 
		zap.Int("segment_count", len(event.SegmentKeys)),
	)

	// Idempotency check
	exists, err := u.storage.RecordingExists(ctx, event.RoomID, event.StreamID)
	if err != nil {
		return fmt.Errorf("check existing recording: %w", err)
	}
	if exists {
		log.Info("recording already assembled, skipping")
		return nil
	}

	jobDir := filepath.Join(u.workDir, event.StreamID)
	if err := os.MkdirAll(jobDir, 0700); err != nil {
		return fmt.Errorf("create job dir: %w", err)
	}
	defer os.RemoveAll(jobDir) // cleanup for both fail and success

	log.Info("starting assembly")

	// Download segments parallel
	if err := u.downloadSegments(ctx, event.SegmentKeys, jobDir); err != nil {
		return fmt.Errorf("download segments: %w", err)
	}

	localFiles, err := filepath.Glob(filepath.Join(jobDir, "*.mp4"))
	if err != nil {
		return fmt.Errorf("glob segments: %w", err)
	}
	if len(localFiles) == 0 {
		return fmt.Errorf("no segments downloaded for stream %s", event.StreamID)
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

	recordingKey, err := u.storage.UploadFinalRecording(ctx, event.RoomID, event.StreamID, f)
	if err != nil {
		return fmt.Errorf("upload final recording: %w", err)
	}

	if err := u.examClient.UpdateRecording(ctx, event.StreamID, event.RoomID, recordingKey, event.Duration); err != nil {
		return fmt.Errorf("notify exam service: %w", err)
	}

	log.Info("assembly completed", zap.String("recording_key", recordingKey))
	return nil
}

func (u *AssemblerUseCase) downloadSegments(ctx context.Context, keys []string, dir string) error {
	sem := make(chan struct{}, 6) // max 6 concurrent downloads
	g, ctx := errgroup.WithContext(ctx)

	for i, key := range keys {
		g.Go(func() error {
			sem <-struct{}{}
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

func writeConcatList(path string, files []string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	for _, file := range files {
		fmt.Fprintf(f, "file '%s'\n", file)
	}
	return nil
}