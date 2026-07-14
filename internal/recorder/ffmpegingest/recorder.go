package ffmpegingest

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
}


func StartRecorder(sdpPath, outDir string, segmentSeconds int, logger *zap.Logger) (*Recorder, error) {
	if err := os.MkdirAll(outDir, 0o700); err != nil {
		return nil, fmt.Errorf("create ffmpeg ingest output dir: %w", err)
	}

	segmentListPath := filepath.Join(outDir, "segment_list.txt")

	cmd := exec.Command("ffmpeg",
		"-hide_banner", "-loglevel", "warning",
		"-protocol_whitelist", "file,udp,rtp",
		"-reorder_queue_size", "256",
		"-max_delay", "500000",
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
		if r.err != nil {
			logger.Warn("ffmpeg recorder exited",
				zap.String("outDir", outDir),
				zap.Error(r.err),
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

	_, _ = io.WriteString(r.stdin, "q\n")
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-r.done:
	case <-timer.C:
		r.logger.Warn("ffmpeg recorder did not exit gracefully, killing process group",
			zap.String("outDir", r.outDir),
		)
		if err := killProcessGroup(r.cmd); err != nil {
			r.logger.Warn("kill ffmpeg recorder process group failed", zap.Error(err))
		}
		<-r.done
	}
}


func (r *Recorder) watchForHang(ctx context.Context) {
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
			size, ok := newestFileSize(r.outDir)
			if !ok {
				continue
			}
			if size != lastSize {
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

func newestFileSize(dir string) (int64, bool) {
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) == 0 {
		return 0, false
	}
	var newest os.FileInfo
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if newest == nil || info.ModTime().After(newest.ModTime()) {
			newest = info
		}
	}
	if newest == nil {
		return 0, false
	}
	return newest.Size(), true
}
