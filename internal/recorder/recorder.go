package recorder

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"

	"os/exec"

	"go.uber.org/zap"
)

const (
	hangCheckInterval  = 10 * time.Second
	defaultHangTimeout = 90 * time.Second // generous: 3x a 30s segment, unvalidated by Test C yet
)

type Recorder struct {
	cmd             *exec.Cmd
	stdin           io.WriteCloser
	outDir          string
	segmentListPath string
	logger          *zap.Logger

	done chan struct{}
	err  error

	hangCancel    context.CancelFunc
	stopRequested atomic.Bool // true only when Stop was called deliberately (not the hang-watchdog)
	forceKilled   atomic.Bool // true if the process didn't quit gracefully within its stop timeout and had to be SIGKILL'd — the segment it was mid-write on is likely truncated/corrupt
}



const defaultReorderQueueSize = 256

const defaultMaxDelayMicros = 500000

func StartRecorder(sdpPath, outDir string, segmentSeconds, reorderQueueSize, maxDelayMicros int, logger *zap.Logger) (*Recorder, error) {
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return nil, fmt.Errorf("create ffmpeg ingest output dir: %w", err)
	}
	if reorderQueueSize <= 0 {
		reorderQueueSize = defaultReorderQueueSize
	}
	if maxDelayMicros <= 0 {
		maxDelayMicros = defaultMaxDelayMicros
	}

	segmentListPath := filepath.Join(outDir, "segment_list.txt")

	cmd := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "warning",
		"-protocol_whitelist", "file,udp,rtp",
		"-reorder_queue_size", strconv.Itoa(reorderQueueSize),
		"-max_delay", strconv.Itoa(maxDelayMicros),
		"-i", sdpPath,
		"-map", "0:v:0", "-map", "0:a:0",
		"-c", "copy",
		"-f", "segment",
		"-segment_time", strconv.Itoa(segmentSeconds),
		"-reset_timestamps", "1",
		"-segment_format", "mp4",
		"-segment_list", segmentListPath,
		"-segment_list_type", "flat",
		"-movflags", "+frag_keyframe+empty_moov+default_base_moof",
		filepath.Join(outDir, "%04d.mp4"),
	)
	setProcessGroup(cmd)

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("open ffmpeg stdin: %w", err)
	}
	var stderr bytes.Buffer
	cmd.Stdout = io.Discard
	cmd.Stderr = &stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start ffmpeg recorder: %w", err)
	}

	r := &Recorder{
		cmd:             cmd,
		stdin:           stdin,
		outDir:          outDir,
		segmentListPath: segmentListPath,
		logger:          logger,
		done:            make(chan struct{}),
	}
	go func() {
		r.err = cmd.Wait()
		close(r.done)
		switch {
		case r.err != nil:
			logger.Warn("ffmpeg recorder exited",
				zap.String("outDir", outDir),
				zap.Error(r.err),
				zap.String("stderr", stderr.String()),
			)
		case stderr.Len() > 0:
			// Exited with code 0 (e.g. after a graceful "q" quit triggered by
			// the hang watchdog) but still said something on stderr — this is
			// otherwise the only place ffmpeg's own diagnostics for this run
			// are visible, and a clean exit can still follow a stall (see
			// watchForHang), so surface it instead of silently discarding it.
			logger.Info("ffmpeg recorder exited cleanly with stderr output",
				zap.String("outDir", outDir),
				zap.String("stderr", stderr.String()),
			)
		}
	}()

	hangCtx, hangCancel := context.WithCancel(context.Background())
	r.hangCancel = hangCancel
	go r.watchForHang(hangCtx)

	logger.Info("ffmpeg recorder started", zap.String("outDir", outDir))
	return r, nil
}

func (r *Recorder) OutputDir() string {
	return r.outDir
}

func (r *Recorder) SegmentListPath() string {
	return r.segmentListPath
}


func (r *Recorder) Stop(timeout time.Duration) {
	r.stopRequested.Store(true)
	r.shutdown(timeout)
}


func (r *Recorder) WasStopped() bool {
	return r.stopRequested.Load()
}

func (r *Recorder) WasForceKilled() bool {
	return r.forceKilled.Load()
}


func (r *Recorder) Done() <-chan struct{} {
	return r.done
}


func (r *Recorder) shutdown(timeout time.Duration) {
	r.hangCancel()
	select {
	case <-r.done:
		return
	default:
	}

	quitSentAt := time.Now()
	_, _ = io.WriteString(r.stdin, "q\n")
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.done:
		r.logger.Info("ffmpeg recorder exited gracefully",
			zap.String("outDir", r.outDir),
			zap.Duration("tookAfterQuit", time.Since(quitSentAt)),
		)
	case <-timer.C:
		r.logger.Warn("ffmpeg recorder did not exit gracefully within timeout, killing process group",
			zap.String("outDir", r.outDir),
			zap.Duration("timeout", timeout),
		)
		if err := killProcessGroup(r.cmd); err != nil {
			r.logger.Warn("kill ffmpeg recorder process group failed", zap.Error(err))
		}
		r.forceKilled.Store(true)
		<-r.done
		r.logger.Info("ffmpeg recorder exited after force-kill",
			zap.String("outDir", r.outDir),
			zap.Duration("tookAfterQuit", time.Since(quitSentAt)),
		)
	}
}


func (r *Recorder) watchForHang(ctx context.Context) {
	startedAt := time.Now()
	lastName := ""
	lastSize := int64(-1)
	lastChange := time.Now()
	ticker := time.NewTicker(hangCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.done:
			return
		case <-ticker.C:
			name, size, ok := newestFileSize(r.outDir)
			if !ok {
				// ffmpeg is running but hasn't produced even a first segment
				// yet (e.g. stuck waiting on a keyframe/SPS-PPS that never
				// arrives, or SDP/codec mismatch) — without this, that case
				// was never caught: the loop just skipped the stall check
				// forever since there was no size to compare.
				if time.Since(startedAt) >= defaultHangTimeout {
					r.logger.Warn("ffmpeg recorder produced no output within startup timeout",
						zap.String("outDir", r.outDir),
						zap.Duration("waited", time.Since(startedAt)),
					)
					go r.shutdown(5 * time.Second)
					return
				}
				continue
			}
			// compare (name, size) together, not size alone — a same-size
			// coincidence right as ffmpeg rotates to a new segment file would
			// otherwise read as "no growth" even though it just rotated.
			if name != lastName || size != lastSize {
				lastName = name
				lastSize = size
				lastChange = time.Now()
				continue
			}
			if time.Since(lastChange) >= defaultHangTimeout {
				r.logger.Warn("ffmpeg recorder appears hung, no output growth",
					zap.String("outDir", r.outDir),
					zap.Duration("stalledFor", time.Since(lastChange)),
				)
				go r.shutdown(5 * time.Second)
				return
			}
		}
	}
}


func newestFileSize(dir string) (name string, size int64, ok bool) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		return "", 0, false
	}
	var newestName string
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".mp4" {
			continue
		}
		if e.Name() > newestName {
			newestName = e.Name()
		}
	}
	if newestName == "" {
		return "", 0, false
	}

	f, err := os.Open(filepath.Join(dir, newestName))
	if err != nil {
		return "", 0, false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", 0, false
	}
	return newestName, info.Size(), true
}
