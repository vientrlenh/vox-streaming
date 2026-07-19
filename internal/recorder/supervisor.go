package recorder

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

// supervisor timeout after seconds
const defaultAttemptStopTimeout = 5 * time.Second

// bounds total restarts independently of
// failStreak. failStreak alone doesn't catch a "produces exactly one segment
// then crashes" pattern — every attempt counts as productive, so failStreak
// never grows and GaveUp() never trips, letting the supervisor restart
// forever (spawning a new ffmpeg process + attempt-N directory each time).
// This caps the total regardless of whether individual attempts "succeeded".
const maxTotalRestartMultiplier = 5

// restart max times
const maxRestartBackoff = 30 * time.Second


type RecorderSupervisor struct {
	sdpPath          string
	baseOutDir       string
	segmentSeconds   int
	reorderQueueSize int
	maxDelayMicros   int
	maxAttempts      int
	stopTimeout      time.Duration // grace period for a soft "q" quit before force-killing; see StartRecorderSupervisor
	requestKeyframe  func()        // see StartRecorderSupervisor
	logger           *zap.Logger

	segCh  chan string
	stopCh chan struct{}
	doneCh chan struct{}
	gaveUp atomic.Bool

	mu            sync.Mutex
	cur           *Recorder
	stopProcessOnce sync.Once
}

func StartRecorderSupervisor(sdpPath, baseOutDir string, segmentSeconds, reorderQueueSize, maxDelayMicros, maxAttempts int, stopTimeout time.Duration, requestKeyframe func(), logger *zap.Logger) (*RecorderSupervisor, error) {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	if stopTimeout <= 0 {
		stopTimeout = defaultAttemptStopTimeout
	}
	rec, err := StartRecorder(sdpPath, filepath.Join(baseOutDir, "attempt-1"), segmentSeconds, reorderQueueSize, maxDelayMicros, logger)
	if err != nil {
		return nil, err
	}

	s := &RecorderSupervisor{
		sdpPath:          sdpPath,
		baseOutDir:       baseOutDir,
		segmentSeconds:   segmentSeconds,
		reorderQueueSize: reorderQueueSize,
		maxDelayMicros:   maxDelayMicros,
		maxAttempts:      maxAttempts,
		stopTimeout:      stopTimeout,
		requestKeyframe:  requestKeyframe,
		logger:           logger,
		segCh:            make(chan string),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
		cur:              rec,
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

	failStreak := 0
	totalRestarts := 0
	maxTotalRestarts := s.maxAttempts * maxTotalRestartMultiplier

	for {
		s.mu.Lock()
		s.cur = rec
		s.mu.Unlock()

		select {
		case <-s.stopCh:
			rec.Stop(s.stopTimeout)
		default:
		}

		producedAny := s.forwardSegments(rec)

		if rec.WasStopped() {
			return
		}
		select {
		case <-s.stopCh:
			return
		default:
		}

		if producedAny {
			failStreak = 0
		} else {
			failStreak++
		}
		totalRestarts++

		if failStreak >= s.maxAttempts {
			s.gaveUp.Store(true)
			s.logger.Error("ffmpeg recorder had too many consecutive unproductive restarts, giving up",
				zap.Int("consecutiveFailures", failStreak),
				zap.String("baseOutDir", s.baseOutDir),
			)
			return
		}
		if totalRestarts >= maxTotalRestarts {
			s.gaveUp.Store(true)
			s.logger.Error("ffmpeg recorder restarted too many times overall, giving up (each attempt produced some output but kept crashing)",
				zap.Int("totalRestarts", totalRestarts),
				zap.String("baseOutDir", s.baseOutDir),
			)
			return
		}

		// failStreak+1 so a productive-but-crashing attempt (failStreak reset
		// to 0) still backs off instead of respawning ffmpeg immediately.
		backoff := min(time.Duration(failStreak+1)*2*time.Second, maxRestartBackoff)
		select {
		case <-time.After(backoff):
		case <-s.stopCh:
			return
		}

		attempt++
		outDir := filepath.Join(s.baseOutDir, fmt.Sprintf("attempt-%d", attempt))
		next, err := StartRecorder(s.sdpPath, outDir, s.segmentSeconds, s.reorderQueueSize, s.maxDelayMicros, s.logger)
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


func (s *RecorderSupervisor) forwardSegments(rec *Recorder) (producedAny bool) {
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for path := range WatchSegments(watchCtx, rec.SegmentListPath(), rec.OutputDir()) {
			producedAny = true
			s.segCh <- path
		}
	}()

	<-rec.Done()
	watchCancel()
	<-drained
	return producedAny
}


func (s *RecorderSupervisor) Segments() <-chan string {
	return s.segCh
}


func (s *RecorderSupervisor) GaveUp() bool {
	return s.gaveUp.Load()
}

func (s *RecorderSupervisor) BaseOutDir() string {
	return s.baseOutDir
}


// synchronously stops the currently-running ffmpeg attempt (soft
// "q" quit, force-killing the process group after stopTimeout if it doesn't
// exit gracefully) and returns once the OS process is confirmed dead. Bound
// by stopTimeout alone — it does not wait for the supervisor's internal
// forward loop to finish draining segments, so callers that only need to
// know "is the process gone, can I release capacity for it" don't have to
// wait on that too. Call Stop (or LastAttemptForceKilled) for the rest.
func (s *RecorderSupervisor) StopProcess() {
	s.stopProcessOnce.Do(func() {
		close(s.stopCh)
		s.mu.Lock()
		cur := s.cur
		s.mu.Unlock()
		if cur != nil {
			cur.Stop(s.stopTimeout)
		}
	})
}

// reports whether the most recent (or current) ffmpeg
// attempt had to be force-killed rather than exiting gracefully. Meaningful
// after StopProcess has returned; s.cur is stable at that point since a
// closed stopCh prevents run() from starting another restart.
func (s *RecorderSupervisor) LastAttemptForceKilled() bool {
	s.mu.Lock()
	cur := s.cur
	s.mu.Unlock()
	if cur == nil {
		return false
	}
	return cur.WasForceKilled()
}

func (s *RecorderSupervisor) Stop() {
	s.StopProcess()
	<-s.doneCh
}

// removes every attempt's output directory. Call only after the
// caller's own consumption of Segments() has fully finished (e.g. its
// upload loop has returned) — segment files are otherwise still being read
// off disk by that consumer even though Segments() has already closed.
func (s *RecorderSupervisor) Cleanup() {
	if err := os.RemoveAll(s.baseOutDir); err != nil {
		s.logger.Warn("ffmpeg ingest cleanup failed", zap.String("baseOutDir", s.baseOutDir), zap.Error(err))
	}
}
