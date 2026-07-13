package recorder

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
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
	audioEnabled    atomic.Bool
	hasVideo        atomic.Bool

	// Writes are handed to writerLoop over writeCh so the RTP read goroutine
	// never blocks on box-building or disk I/O (which would overflow the socket
	// buffer and drop packets -> corrupt macroblocks, worst on high-bitrate
	// screen share). A full queue drops a whole picture (a clean freeze) rather
	// than losing mid-frame RTP packets.
	writeCh    chan writeCmd
	writerStop chan struct{}
	writerDone chan struct{}
	dropped    atomic.Int64

	uploadWg sync.WaitGroup
	resultMu sync.Mutex
	segments []SegmentMeta
	ticker   *time.Ticker
	stopOnce sync.Once
	stopCh   chan struct{}
	rotateCh chan struct{}
}

// writeCmd is a queued recorder write (video picture or audio packet).
type writeCmd struct {
	video   bool
	nals    [][]byte
	hasIDR  bool
	dur     uint32
	payload []byte
	samples uint32
}

// Bounds the queue between the RTP read path and the writer goroutine. Big
// enough to absorb disk-write stalls (tens of frames), small enough to cap
// memory — on sustained overload whole pictures are dropped instead.
const writeQueueSize = 512

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
		writeCh:         make(chan writeCmd, writeQueueSize),
		writerStop:      make(chan struct{}),
		writerDone:      make(chan struct{}),
		stopCh:          make(chan struct{}),
		rotateCh:        make(chan struct{}, 1),
	}
	r.ticker = time.NewTicker(r.segmentDuration)
	go r.rotationLoop()
	go r.writerLoop()
	return r, nil
}

// writerLoop owns all segment writes (box building + disk I/O), off the RTP read
// path. It drains any queued writes on stop so the final segment is complete.
func (r *SegmentedRecorder) writerLoop() {
	defer close(r.writerDone)
	for {
		select {
		case cmd := <-r.writeCh:
			r.applyWrite(cmd)
		case <-r.writerStop:
			for {
				select {
				case cmd := <-r.writeCh:
					r.applyWrite(cmd)
				default:
					return
				}
			}
		}
	}
}

func (r *SegmentedRecorder) applyWrite(cmd writeCmd) {
	r.mu.Lock()
	if r.current == nil {
		r.mu.Unlock()
		return
	}
	if !cmd.video {
		r.current.writeAudio(cmd.payload, cmd.samples)
		r.mu.Unlock()
		return
	}
	r.hasVideo.Store(true)
	err := r.current.writeVideo(cmd.nals, cmd.hasIDR, cmd.dur)
	r.mu.Unlock()
	if err != nil {
		r.logger.Warn("mp4 write error, triggering emergency rotation", zap.Error(err))
		select {
		case r.rotateCh <- struct{}{}:
		default: // rotation already scheduled
		}
	}
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
	if old == nil {
		r.mu.Unlock() // recorder is finalizing; nothing to rotate
		return
	}
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
//
// Non-blocking: the picture is queued for writerLoop. The caller (RTP read loop)
// never blocks. NAL byte slices are already owned copies from the extractor, so
// they are safe to hand off. On a full queue the whole picture is dropped.
func (r *SegmentedRecorder) WriteVideoNALs(nals [][]byte, hasIDR bool, dur uint32) {
	select {
	case r.writeCh <- writeCmd{video: true, nals: nals, hasIDR: hasIDR, dur: dur}:
	default:
		if n := r.dropped.Add(1); n%100 == 1 {
			r.logger.Warn("recorder write queue full, dropping picture(s)", zap.Int64("droppedTotal", n))
		}
	}
}

// enable audio writing for subsequent segments
func (r *SegmentedRecorder) StartAudio(_ uint8) error {
	r.audioEnabled.Store(true)
	return nil
}

// write one Opus RTP payload with its 48kHz sample count
//
// Non-blocking: payload is copied (the RTP read buffer is reused) and queued.
func (r *SegmentedRecorder) WriteAudioPacket(payload []byte, sampleCount uint32) {
	if !r.audioEnabled.Load() {
		return
	}
	cp := append([]byte(nil), payload...)
	select {
	case r.writeCh <- writeCmd{payload: cp, samples: sampleCount}:
	default:
		if n := r.dropped.Add(1); n%100 == 1 {
			r.logger.Warn("recorder write queue full, dropping audio packet(s)", zap.Int64("droppedTotal", n))
		}
	}
}

// stop halts rotation and the writer goroutine, waiting for all queued writes to
// be flushed into the current segment before returning.
func (r *SegmentedRecorder) stop() {
	r.stopOnce.Do(func() {
		r.ticker.Stop()
		close(r.stopCh)
		close(r.writerStop)
	})
	<-r.writerDone
}

// stop rotation, uploads the last segment, and returns all segment metadata
func (r *SegmentedRecorder) Finalize(ctx context.Context) ([]SegmentMeta, error) {
	r.stop()

	r.mu.Lock()
	last := r.current
	seq := r.segmentSeq
	r.segmentSeq++
	r.current = nil
	r.mu.Unlock()

	if last != nil && r.hasVideo.Load() {
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
	r.stop()

	r.mu.Lock()
	current := r.current
	r.current = nil
	r.mu.Unlock()

	if current != nil {
		current.discard()
	}
}
