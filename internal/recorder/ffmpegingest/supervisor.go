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
	sdpPath         string
	baseOutDir      string
	segmentSeconds  int
	maxAttempts     int
	stopTimeout     time.Duration // grace period for a soft "q" quit before force-killing; see StartRecorderSupervisor
	requestKeyframe func()        // see StartRecorderSupervisor
	logger          *zap.Logger

	segCh  chan string
	stopCh chan struct{}
	doneCh chan struct{}
	gaveUp atomic.Bool

	mu   sync.Mutex
	cur  *Recorder
	once sync.Once
}

func StartRecorderSupervisor(sdpPath, baseOutDir string, segmentSeconds, maxAttempts int, stopTimeout time.Duration, requestKeyframe func(), logger *zap.Logger) (*RecorderSupervisor, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if stopTimeout <= 0 {
		stopTimeout = defaultAttemptStopTimeout
	}
	rec, err := StartRecorder(sdpPath, filepath.Join(baseOutDir, "attempt-1"), segmentSeconds, logger)
	if err != nil {
		return nil, err
	}

	s := &RecorderSupervisor{
		sdpPath:         sdpPath,
		baseOutDir:      baseOutDir,
		segmentSeconds:  segmentSeconds,
		maxAttempts:     maxAttempts,
		stopTimeout:     stopTimeout,
		requestKeyframe: requestKeyframe,
		logger:          logger,
		segCh:           make(chan string),
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		cur:             rec,
	}
	go s.run(rec, 1)
	return s, nil
}


func (s *RecorderSupervisor) primeKeyframe() {
	if s.requestKeyframe == nil {
		return
	}
	time.Sleep(200 * time.Millisecond)
	s.requestKeyframe()
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
			rec.Stop(s.stopTimeout)
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
		s.primeKeyframe()
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


func (s *RecorderSupervisor) Segments() <-chan string {
	return s.segCh
}


func (s *RecorderSupervisor) GaveUp() bool {
	return s.gaveUp.Load()
}


func (s *RecorderSupervisor) Stop() {
	s.once.Do(func() {
		close(s.stopCh)
		s.mu.Lock()
		cur := s.cur
		s.mu.Unlock()
		if cur != nil {
			cur.Stop(s.stopTimeout)
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
