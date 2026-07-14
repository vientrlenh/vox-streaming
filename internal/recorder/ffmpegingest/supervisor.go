package ffmpegingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)


const defaultAttemptStopTimeout = 5 * time.Second


type RecorderSupervisor struct {
	sdpPath        string
	baseOutDir     string
	segmentSeconds int
	maxAttempts    int
	logger         *zap.Logger

	segCh  chan string
	stopCh chan struct{}
	doneCh chan struct{}
	gaveUp atomic.Bool

	mu   sync.Mutex
	cur  *Recorder
	once sync.Once
}

func StartRecorderSupervisor(sdpPath, baseOutDir string, segmentSeconds, maxAttempts int, logger *zap.Logger) (*RecorderSupervisor, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	rec, err := StartRecorder(sdpPath, filepath.Join(baseOutDir, "attempt-1"), segmentSeconds, logger)
	if err != nil {
		return nil, err
	}

	s := &RecorderSupervisor{
		sdpPath:        sdpPath,
		baseOutDir:     baseOutDir,
		segmentSeconds: segmentSeconds,
		maxAttempts:    maxAttempts,
		logger:         logger,
		segCh:          make(chan string),
		stopCh:         make(chan struct{}),
		doneCh:         make(chan struct{}),
		cur:            rec,
	}
	go s.run(rec, 1)
	return s, nil
}

func (s *RecorderSupervisor) run(rec *Recorder, attempt int) {
	defer close(s.doneCh)
	defer close(s.segCh)

	for {
		s.mu.Lock()
		s.cur = rec
		s.mu.Unlock()

		select {
		case <-s.stopCh:
			rec.Stop(defaultAttemptStopTimeout)
		default:
		}

		s.forwardSegments(rec)

		if rec.WasStopped() {
			return
		}
		select {
		case <-s.stopCh:
			return
		default:
		}

		if attempt >= s.maxAttempts {
			s.gaveUp.Store(true)
			s.logger.Error("ffmpeg recorder exhausted restart attempts, giving up",
				zap.Int("attempts", attempt),
				zap.String("baseOutDir", s.baseOutDir),
			)
			return
		}

		backoff := time.Duration(attempt) * 2 * time.Second
		select {
		case <-time.After(backoff):
		case <-s.stopCh:
			return
		}

		attempt++
		outDir := filepath.Join(s.baseOutDir, fmt.Sprintf("attempt-%d", attempt))
		next, err := StartRecorder(s.sdpPath, outDir, s.segmentSeconds, s.logger)
		if err != nil {
			s.logger.Warn("ffmpeg recorder restart failed", zap.Int("attempt", attempt), zap.Error(err))
			s.gaveUp.Store(true)
			return
		}
		s.logger.Info("ffmpeg recorder restarted after unexpected exit", zap.Int("attempt", attempt))
		rec = next
	}
}


func (s *RecorderSupervisor) forwardSegments(rec *Recorder) {
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for path := range WatchSegments(watchCtx, rec.SegmentListPath(), rec.OutputDir()) {
			s.segCh <- path
		}
	}()

	<-rec.Done()
	watchCancel()
	<-drained
}

// Segments returns completed segment paths across every attempt, in order.
// Closes once the supervisor has stopped for good (deliberately or after
// exhausting maxAttempts) — callers should range over it unconditionally.
func (s *RecorderSupervisor) Segments() <-chan string {
	return s.segCh
}

// GaveUp reports whether the supervisor stopped because it exhausted
// maxAttempts (or a restart itself failed to start), as opposed to a
// deliberate Stop() — the two must be told apart at the call site (peer.go's
// close()) since only a permanent give-up should fall back away from ffmpeg.
func (s *RecorderSupervisor) GaveUp() bool {
	return s.gaveUp.Load()
}

// Stop asks the current attempt to quit gracefully and waits for run() to
// fully finish, guaranteeing Segments() is already closed by the time Stop
// returns.
func (s *RecorderSupervisor) Stop(timeout time.Duration) {
	s.once.Do(func() {
		close(s.stopCh)
		s.mu.Lock()
		cur := s.cur
		s.mu.Unlock()
		if cur != nil {
			cur.Stop(timeout)
		}
	})
	<-s.doneCh
}

// Cleanup removes every attempt's output directory. Call only after the
// caller's own consumption of Segments() has fully finished (e.g. its
// upload loop has returned) — segment files are otherwise still being read
// off disk by that consumer even though Segments() has already closed.
func (s *RecorderSupervisor) Cleanup() {
	if err := os.RemoveAll(s.baseOutDir); err != nil {
		s.logger.Warn("ffmpeg ingest cleanup failed", zap.String("baseOutDir", s.baseOutDir), zap.Error(err))
	}
}
