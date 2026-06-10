package recorder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"go.uber.org/zap"
)

const defaultSegmentDuration = 30 * time.Second

type SegmentMeta struct {
	Seq int64
	S3Key string
	StartedAt time.Time
	EndedAt time.Time
}

// a short-lived ffmpeg process (30s) + OGG writer for audio
type segmentWriter struct {
	ffmpegCmd *exec.Cmd
	videoIn io.WriteCloser
	videoPath string
	startedAt time.Time

	audioFile *os.File
	audioPath string
	oggWriter *OGGWriter
	hasAudio bool
}

func newSegmentWriter(withAudio bool, channels uint8) (*segmentWriter, error) {
	vf, err := os.CreateTemp("", "vox-segment-video-*.mp4")
	if err != nil {
		return nil, err
	}
	vf.Close()
	videoPath := vf.Name()

	cmd := exec.Command("ffmpeg", 
		"-hide_banner", "-loglevel", "error", 
		"-f", "h264", "-i", "pipe:0", 
		"-c:v", "copy", 
		"-movflags", "frag_keyframe+empty_moov", // fragmented MP4 for segment
		"-y", videoPath,
	)

	pipe, err := cmd.StdinPipe()
	if err != nil {
		os.Remove(videoPath)
		return nil, err
	}

	if err := cmd.Start(); err != nil {
		os.Remove(videoPath)
		return nil, fmt.Errorf("ffmpeg start: %w", err)
	}

	sw := &segmentWriter{
		ffmpegCmd: cmd,
		videoIn: pipe, 
		videoPath: videoPath, 
		startedAt: time.Now().UTC(),
	}
	if withAudio {
		_ = sw.startAudio(channels)
	}
	return sw, nil
}

func (sw *segmentWriter) startAudio(channels uint8) error {
	af, err := os.CreateTemp("", "vox-seg-audio-*.ogg")
	if err != nil {
		return err
	}
	ow, err := NewOGGWriter(af, channels)
	if err != nil {
		af.Close()
		os.Remove(af.Name())
		return err
	}
	sw.audioFile = af
	sw.audioPath = af.Name()
	sw.oggWriter = ow
	sw.hasAudio = true
	return nil
}


func (sw *segmentWriter) writeVideo(nals [][]byte) error {
	if sw.videoIn == nil {
		return nil
	}
	for _, nal := range nals {
		if _, err := sw.videoIn.Write(annexBStartCode); err != nil {
			sw.videoIn = nil // mark dead, stop future writes
			return err
		}
		if _, err := sw.videoIn.Write(nal); err != nil {
			sw.videoIn = nil
			return err
		}
	}
	return nil
}

func (sw *segmentWriter) writeAudio(payload []byte, sampleCount uint32) {
	if !sw.hasAudio || sw.oggWriter == nil {
		return
	}
	if err := sw.oggWriter.WritePacket(payload, sampleCount); err != nil {
		sw.oggWriter = nil
	}
}

// close ffmpeg and OGG, run mux, return path to the finalized Mp4 file 
func (sw *segmentWriter) closeAndMux(ctx context.Context, logger *zap.Logger) (string, error) {
	if sw.videoIn != nil {
		sw.videoIn.Close()
		sw.videoIn = nil
	}
	if sw.ffmpegCmd != nil && sw.ffmpegCmd.Process != nil {
		if err := sw.ffmpegCmd.Wait(); err != nil {
			logger.Warn("segment ffmpeg exited with error", zap.Error(err))
		}
	}
	if sw.oggWriter != nil {
		sw.oggWriter.Close()
		sw.oggWriter = nil
	}
	if sw.audioFile != nil {
		sw.audioFile.Close()
		sw.audioFile = nil
	}

	stat, err := os.Stat(sw.videoPath)
	if err != nil || stat.Size() < 100 {
		sw.cleanup()
		return "", nil
	}

	out, err := os.CreateTemp("", "vox-segment-final-*.mp4")
	if err != nil {
		sw.cleanup()
		return "", err
	}
	outPath := out.Name()
	out.Close()

	args := []string{"-hide_banner", "-loglevel", "error", "-i", sw.videoPath}
	if sw.hasAudio && sw.audioPath != "" {
		args = append(args, "-i", sw.audioPath)
	}
	args = append(args, "-c:v", "copy")
	if sw.hasAudio {
		args = append(args, "-c:a", "aac", "-b:a", "128k")
	}
	args = append(args, 
		"-movflags", "faststart",
		"-y", outPath)

	var errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		os.Remove(outPath)
		sw.cleanup()
		return "", fmt.Errorf("segment mux: %w: %s", err, errBuf.String())
	}
	sw.cleanup()
	return outPath, nil
}

func (sw *segmentWriter) cleanup() {
	if sw.videoPath != "" {
		os.Remove(sw.videoPath)
	}
	if sw.audioPath != "" {
		os.Remove(sw.audioPath)
	}
}

type SegmentedRecorder struct {
	roomID string
	streamID string
	storage *storage.Client
	logger *zap.Logger

	segmentDuration time.Duration
	mu sync.Mutex
	current *segmentWriter
	segmentSeq int64
	audioChannels uint8
	audioEnabled bool
	hasVideo bool

	uploadWg sync.WaitGroup
	resultMu sync.Mutex
	segments []SegmentMeta
	ticker *time.Ticker
	stopOnce sync.Once
	stopCh chan struct{}
	rotateCh chan struct{}
}

func NewSegmentedRecorder(roomID, streamID string, storage *storage.Client, logger *zap.Logger) (*SegmentedRecorder, error) {
	first, err := newSegmentWriter(false, 0)
	if err != nil {
		return nil, fmt.Errorf("init first segment: %w", err)
	}
	r := &SegmentedRecorder{
		roomID: roomID, 
 		streamID: streamID, 
		storage: storage, 
		logger: logger, 
		segmentDuration: defaultSegmentDuration, 
		current: first, 
		stopCh: make(chan struct{}),
		rotateCh: make(chan struct{}, 1),
	}
	r.ticker = time.NewTicker(r.segmentDuration)
	go r.rotationLoop()
	return r, nil
}

func (r *SegmentedRecorder) rotationLoop() {
	for {
		select {
		case <-r.stopCh:
			return
		case <-r.ticker.C:
			r.rotateSegment()
		case <-r.rotateCh:
			r.logger.Warn("forced rotation due to write failure")
			r.rotateSegment()
			r.ticker.Reset(r.segmentDuration) // reset timer, prevent 2 times rotations in a row
		}
	}
}

func (r *SegmentedRecorder) rotateSegment() {
	r.mu.Lock()
	old := r.current
	seq := r.segmentSeq
	r.segmentSeq++
	endedAt := time.Now().UTC()

	next, err := newSegmentWriter(r.audioEnabled, r.audioChannels)
	if err != nil {
		r.logger.Warn("start next segment failed, keeping current", zap.Error(err))
		r.mu.Unlock()
		return
	}
	r.current = next
	r.mu.Unlock()

	r.uploadWg.Go(func() {
		r.finalizeAndUpload(context.Background(), old, seq, endedAt)
	})
}

func (r *SegmentedRecorder) finalizeAndUpload(ctx context.Context, sw *segmentWriter, seq int64, endedAt time.Time) {
	startedAt := sw.startedAt
	tmpPath, err := sw.closeAndMux(ctx, r.logger)
	if err != nil {
		r.logger.Warn("segment finalize failed", 
			zap.Int64("seq", seq), 
			zap.Error(err),
		)
		return
	}
	if tmpPath == "" {
		return // empty segment, skipping
	}
	defer os.Remove(tmpPath)

	f, err := os.Open(tmpPath)
	if err != nil {
		r.logger.Warn("open segment for upload failed", 
			zap.Int64("seq", seq),
			zap.Error(err),
		)
		return
	}
	defer f.Close()

	s3Key, err := r.storage.UploadServerSegment(ctx, r.roomID, r.streamID, seq, f)
	if err != nil {
		r.logger.Warn("segment upload failed", 
			zap.Int64("seq", seq),
			zap.Error(err),
		)
		return
	}

	r.resultMu.Lock()
	r.segments = append(r.segments, SegmentMeta{
		Seq: seq, 
		S3Key: s3Key, 
		StartedAt: startedAt,
		EndedAt: endedAt,
	})
	r.resultMu.Unlock()

	r.logger.Info("segment uploaded", 
		zap.Int64("seq", seq), 
		zap.String("s3_key", s3Key), 
		zap.Duration("duration", endedAt.Sub(startedAt)),
	)
}


func (r *SegmentedRecorder) WriteVideoNALs(nals [][]byte) {
	r.mu.Lock()
	if r.current == nil {
		r.mu.Unlock()
		return
	}
	r.hasVideo = true
	err := r.current.writeVideo(nals)
	r.mu.Unlock()

	if err != nil {
		r.logger.Warn("ffmpeg pipe broken, triggering emergency rotation", zap.Error(err))
		select {
		case r.rotateCh <-struct{}{}:
		default: // rotation has been scheduled
		}
	}
	
}

func (r *SegmentedRecorder) StartAudio(channels uint8) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.audioEnabled {
		return nil
	}

	r.audioEnabled = true
	r.audioChannels = channels
	if r.current != nil {
		return r.current.startAudio(channels)
	}
	return nil
}

func (r *SegmentedRecorder) WriteAudioPacket(payload []byte, sampleCount uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == nil {
		return
	}
	r.current.writeAudio(payload, sampleCount)
}

// close finalized segment, wait for all segments uploaded, return segment list
// called when stream ended properly
func (r *SegmentedRecorder) Finalize(ctx context.Context) ([]SegmentMeta, error) {
	r.stopOnce.Do(func() {
		r.ticker.Stop()
		close(r.stopCh)
	})

	r.mu.Lock()
	last := r.current
	seq := r.segmentSeq
	r.segmentSeq++
	r.current = nil
	r.mu.Unlock()

	if last != nil && r.hasVideo {
		r.uploadWg.Go(func() {
			r.finalizeAndUpload(ctx, last, seq, time.Now().UTC())
		})
	} else if last != nil {
		last.cleanup()
	}

	r.uploadWg.Wait()

	r.resultMu.Lock()
	result := make([]SegmentMeta, len(r.segments))
	copy(result, r.segments)
	r.resultMu.Unlock()

	sort.Slice(result, func(i int, j int) bool {
		return result[i].Seq < result[j].Seq
	})

	return result, nil
}

// discard recording, used when stream failed
func (r *SegmentedRecorder) Close() {
	r.stopOnce.Do(func() {
		r.ticker.Stop()
		close(r.stopCh)
	})

	r.mu.Lock()
	current := r.current
	r.current = nil 
	r.mu.Unlock()

	if current != nil {
		current.discard()
	}
}

func (sw *segmentWriter) discard() {
	if sw.videoIn != nil {
		sw.videoIn.Close()
		sw.videoIn = nil
	}
	if sw.ffmpegCmd != nil && sw.ffmpegCmd.Process != nil {
		sw.ffmpegCmd.Wait()
	}
	if sw.oggWriter != nil {
		sw.oggWriter.Close()
		sw.oggWriter = nil
	}
	if sw.audioFile != nil {
		sw.audioFile.Close()
		sw.audioFile = nil
	}
	sw.cleanup()
}



