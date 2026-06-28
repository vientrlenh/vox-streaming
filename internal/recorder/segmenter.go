package recorder

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"go.uber.org/zap"
)

const defaultSegmentDuration = 30 * time.Second

type SegmentMeta struct {
	Seq       int64
	S3Key     string
	StartedAt time.Time
	EndedAt   time.Time
}

// write one 30 sec fMp4 segment with no external processes
type segmentWriter struct {
	outFile   *os.File
	outPath   string
	startedAt time.Time
	mw        *fMP4Writer

	initWritten bool
	sps, pps    []byte

	// pending GOP buffer - flushed as one fragment on next IDR or rotation
	gopVideo    []fragSample
	gopAudio    []fragSample
	gopVideoDTS uint64
	gopAudioDTS uint64

	// running DTS counter (updated after each flush)
	videoDTS uint64
	audioDTS uint64
}

func newSegmentWriter(tempDir string) (*segmentWriter, error) {
	f, err := os.CreateTemp(tempDir, "vox-segment-video-*.mp4")
	if err != nil {
		return nil, err
	}

	return &segmentWriter{
		outFile:   f,
		outPath:   f.Name(),
		startedAt: time.Now().UTC(),
		mw:        newFMP4Writer(f),
	}, nil
}

// buffer a picture into the current GDP
// On a new IDR, the previous GOP is flushed as one moof+mdat fragment first
// hasIDR must be true when nals contains an IDR frame (type 5)
func (sw *segmentWriter) writeVideo(nals [][]byte, hasIDR bool, dur uint32) error {
	// Extract SPS/PPS whenever they appear (typically in every IDR picture)
	for _, nal := range nals {
		if len(nal) == 0 {
			continue
		}
		switch nal[0] & 0x1F {
		case 7:
			sw.sps = nal
		case 8:
			sw.pps = nal
		}
	}

	if !sw.initWritten {
		if hasIDR && len(sw.sps) > 0 && len(sw.pps) > 0 {
			if err := sw.mw.WriteInit(sw.sps, sw.pps); err != nil {
				return fmt.Errorf("write init: %w", err)
			}
			sw.initWritten = true
		} else {
			return nil // wait for first IDR with parameter sets
		}
	}

	if hasIDR && len(sw.gopVideo) > 0 {
		if err := sw.flushGOP(); err != nil {
			return err
		}
	}

	if len(sw.gopVideo) == 0 {
		sw.gopVideoDTS = sw.videoDTS
		sw.gopAudioDTS = sw.audioDTS
	}

	avcc := nalsToAVCC(nals)
	if len(avcc) == 0 {
		return nil // picture had only SPS/PPS, nothing to store
	}
	sw.gopVideo = append(sw.gopVideo, fragSample{
		data:  avcc,
		dur:   dur,
		isKey: hasIDR,
	})
	return nil
}

func (sw *segmentWriter) writeAudio(payload []byte, sampleCount uint32) {
	if !sw.initWritten || len(sw.gopVideo) == 0 {
		return // align audio start with first GOP
	}
	sw.gopAudio = append(sw.gopAudio, fragSample{
		data: append([]byte(nil), payload...),
		dur:  sampleCount, // 48kHz ticks - comes from RTP timestamp diff
	})
}

func (sw *segmentWriter) flushGOP() error {
	if len(sw.gopVideo) == 0 {
		return nil
	}
	vf := videoFragment{
		videoSamples:  sw.gopVideo,
		audioSamples:  sw.gopAudio,
		videoDTSStart: sw.gopVideoDTS,
		audioDTSStart: sw.gopAudioDTS,
	}
	if err := sw.mw.WriteFragment(vf); err != nil {
		return fmt.Errorf("write fragment: %w", err)
	}
	for _, s := range sw.gopVideo {
		sw.videoDTS += uint64(s.dur)
	}
	for _, s := range sw.gopAudio {
		sw.audioDTS += uint64(s.dur)
	}
	sw.gopVideo = nil
	sw.gopAudio = nil
	return nil
}

// flush the last GOP and closes the output file
// return the path of the final mp4, or "" if the segment has no video
func (sw *segmentWriter) closeAndFinalize() (string, error) {
	_ = sw.flushGOP()
	sw.outFile.Close()

	if !sw.initWritten {
		os.Remove(sw.outPath)
		return "", nil
	}
	stat, err := os.Stat(sw.outPath)
	if err != nil || stat.Size() < 100 {
		os.Remove(sw.outPath)
		return "", nil
	}
	return sw.outPath, nil
}

func (sw *segmentWriter) discard() {
	sw.outFile.Close()
	os.Remove(sw.outPath)
}

type SegmentedRecorder struct {
	roomID   string
	streamID string
	tempDir  string
	storage  *storage.Client
	logger   *zap.Logger

	segmentDuration time.Duration
	mu              sync.Mutex
	current         *segmentWriter
	segmentSeq      int64
	audioEnabled    bool
	hasVideo        bool

	uploadWg sync.WaitGroup
	resultMu sync.Mutex
	segments []SegmentMeta
	ticker   *time.Ticker
	stopOnce sync.Once
	stopCh   chan struct{}
	rotateCh chan struct{}
}

func NewSegmentedRecorder(roomID, streamID, tempDir string, storage *storage.Client, logger *zap.Logger) (*SegmentedRecorder, error) {
	first, err := newSegmentWriter(tempDir)
	if err != nil {
		return nil, fmt.Errorf("init first segment: %w", err)
	}
	r := &SegmentedRecorder{
		roomID:          roomID,
		streamID:        streamID,
		tempDir:         tempDir,
		storage:         storage,
		logger:          logger,
		segmentDuration: defaultSegmentDuration,
		current:         first,
		stopCh:          make(chan struct{}),
		rotateCh:        make(chan struct{}, 1),
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

	next, err := newSegmentWriter(r.tempDir)
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
	tmpPath, err := sw.closeAndFinalize()
	if err != nil {
		r.logger.Warn("segment finalize failed", zap.Int64("seq", seq), zap.Error(err))
		return
	}
	if tmpPath == "" {
		return // empty segment, skipping
	}
	defer os.Remove(tmpPath)

	s3Key, err := r.uploadWithRetry(ctx, tmpPath, seq)
	if err != nil {
		r.logger.Error("segment upload failed permanently",
			zap.Int64("seq", seq),
			zap.Error(err),
		)
		return
	}

	r.resultMu.Lock()
	r.segments = append(r.segments, SegmentMeta{
		Seq:       seq,
		S3Key:     s3Key,
		StartedAt: startedAt,
		EndedAt:   endedAt,
	})
	r.resultMu.Unlock()

	r.logger.Info("segment uploaded",
		zap.Int64("seq", seq),
		zap.String("s3Key", s3Key),
		zap.Duration("duration", endedAt.Sub(startedAt)),
	)
}

const segmentUploadMaxAttempts = 3

func (r *SegmentedRecorder) uploadWithRetry(ctx context.Context, tmpPath string, seq int64) (string, error) {
	var lastErr error
	for attempt := range segmentUploadMaxAttempts {
		if attempt > 0 {
			delay := time.Duration(attempt) * 2 * time.Second // 2s, 4s
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}

		f, err := os.Open(tmpPath)
		if err != nil {
			return "", fmt.Errorf("open segment: %w", err)
		}
		key, err := r.storage.UploadServerSegment(ctx, r.roomID, r.streamID, seq, f)
		f.Close()
		if err == nil {
			return key, nil
		}
		lastErr = err
		r.logger.Warn("segment upload attempt failed",
			zap.Int64("seq", seq),
			zap.Int("attempt", attempt+1),
			zap.Error(err),
		)
	}
	return "", fmt.Errorf("upload failed after %d attempts: %w", segmentUploadMaxAttempts, lastErr)
}

// receives all NAL units of one complete picture
// hasIDR must be true when nals contain an IDR frame
func (r *SegmentedRecorder) WriteVideoNALs(nals [][]byte, hasIDR bool, dur uint32) {
	r.mu.Lock()
	if r.current == nil {
		r.mu.Unlock()
		return
	}
	r.hasVideo = true
	err := r.current.writeVideo(nals, hasIDR, dur)
	r.mu.Unlock()

	if err != nil {
		r.logger.Warn("mp4 write error, triggering emergency rotation", zap.Error(err))
		select {
		case r.rotateCh <- struct{}{}:
		default: // rotation has been scheduled
		}
	}
}

// enable audio writing for subsequent segments
func (r *SegmentedRecorder) StartAudio(_ uint8) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.audioEnabled = true
	return nil
}

// write one Opus RTP payload with its 48kHz sample count
func (r *SegmentedRecorder) WriteAudioPacket(payload []byte, sampleCount uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.audioEnabled || r.current == nil {
		return
	}
	r.current.writeAudio(payload, sampleCount)
}

// stop rotation, uploads the last segment, and returns all segment metadata
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
		last.discard()
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

// discard the current segment without uploading (stream failed)
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
