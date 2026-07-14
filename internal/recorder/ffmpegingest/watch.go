package ffmpegingest

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const watchSegmentsInterval = 2 * time.Second


func WatchSegments(ctx context.Context, listPath, outDir string) <-chan string {
	out := make(chan string)
	go func() {
		defer close(out)
		sent := 0
		ticker := time.NewTicker(watchSegmentsInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				emitNewLines(listPath, outDir, &sent, out)
				return
			case <-ticker.C:
				emitNewLines(listPath, outDir, &sent, out)
			}
		}
	}()
	return out
}

func emitNewLines(listPath, outDir string, sent *int, out chan<- string) {
	f, err := os.Open(listPath)
	if err != nil {
		return
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return
	}
	if *sent >= len(lines) {
		return
	}
	for _, line := range lines[*sent:] {
		if !filepath.IsAbs(line) {
			line = filepath.Join(outDir, line)
		}
		out <- line
	}
	*sent = len(lines)
}
