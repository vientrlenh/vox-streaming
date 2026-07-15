package recorder

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
)

const tempDirSweepInterval = 30 * time.Minute

// periodically removes per-stream ffmpeg-ingest temp
// directories under baseDir older than ttl. Under normal operation these are
// already cleaned up by RecorderSupervisor.Cleanup() once a stream finishes
// — this exists only to bound disk growth from directories deliberately left
// behind because they contained a segment that failed to upload (see
// ffmpegIngestState.incomplete in the webrtc transport package), which have
// no other cleanup path and would otherwise accumulate forever.
// ttl <= 0 disables the sweep.
func StartTempDirSweep(ctx context.Context, baseDir string, ttl time.Duration, logger *zap.Logger) {
	if ttl <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(tempDirSweepInterval)
		defer ticker.Stop()
		sweepTempDir(baseDir, ttl, logger)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sweepTempDir(baseDir, ttl, logger)
			}
		}
	}()
}

func sweepTempDir(baseDir string, ttl time.Duration, logger *zap.Logger) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return // doesn't exist yet, or transient error — next tick retries
	}
	cutoff := time.Now().Add(-ttl)
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil || info.ModTime().After(cutoff) {
			continue
		}
		path := filepath.Join(baseDir, e.Name())
		if err := os.RemoveAll(path); err != nil {
			logger.Warn("ffmpeg ingest temp dir sweep: remove failed",
				zap.String("path", path), zap.Error(err),
			)
			continue
		}
		logger.Warn("ffmpeg ingest temp dir sweep: removed stale directory (an incomplete recording left for manual recovery that nobody retrieved in time)",
			zap.String("path", path),
			zap.Duration("age", time.Since(info.ModTime())),
		)
	}
}
