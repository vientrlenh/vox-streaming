package recorder

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const watchHLSInterval = 1 * time.Second // shorter than watchSegmentsInterval — this is the live-edge latency a monitor sees

// HLSFragmentEvent is a locally-written HLS asset (the fMP4 init segment or a
// media fragment) that ffmpeg has finished writing and is ready to upload.
type HLSFragmentEvent struct {
	Path   string // local file path
	IsInit bool
	Epoch  int // == the RecorderSupervisor attempt number this asset belongs to

	// Duration is ffmpeg's own reported media duration for this fragment, parsed from the
	// #EXTINF line that precedes it in live.m3u8 -- zero for init segments. The caller (peer.go's
	// runHLSFragmentUploader) must use this instead of measuring wall-clock time around the
	// upload, since sequential S3 upload latency for one fragment would otherwise bleed into the
	// "duration" computed for the next one, corrupting the EXTINF values buildLiveManifest later
	// emits to the client and, with them, hls.js's whole notion of the DVR timeline.
	Duration time.Duration
}

// WatchHLSFragments tails ffmpeg's own local hlsDir/live.m3u8, the same way
// WatchSegments tails segment_list.txt: ffmpeg only appends a line once a
// fragment is closed and rotated away from, so a line appearing there is the
// completion signal — never glob the directory directly, a fragment file can
// still be mid-write when its filename would otherwise appear.
func WatchHLSFragments(ctx context.Context, hlsDir string, epoch int) <-chan HLSFragmentEvent {
	out := make(chan HLSFragmentEvent)
	go func() {
		defer close(out)
		playlistPath := filepath.Join(hlsDir, "live.m3u8")
		sentInit := false
		sentFrags := 0
		ticker := time.NewTicker(watchHLSInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				emitNewHLSEntries(playlistPath, hlsDir, epoch, &sentInit, &sentFrags, out)
				return
			case <-ticker.C:
				emitNewHLSEntries(playlistPath, hlsDir, epoch, &sentInit, &sentFrags, out)
			}
		}
	}()
	return out
}

func emitNewHLSEntries(playlistPath, hlsDir string, epoch int, sentInit *bool, sentFrags *int, out chan<- HLSFragmentEvent) {
	f, err := os.Open(playlistPath)
	if err != nil {
		return
	}
	defer f.Close()

	type fragEntry struct {
		name     string
		duration time.Duration
	}

	var initURI string
	var frags []fragEntry
	var pendingDuration time.Duration
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if initURI == "" && strings.HasPrefix(line, "#EXT-X-MAP:") {
			initURI = parseHLSMapURI(line)
			continue
		}
		if strings.HasPrefix(line, "#EXTINF:") {
			// #EXTINF always immediately precedes the fragment filename it describes -- ffmpeg's
			// own measured media duration for that fragment, not to be confused with (and far more
			// accurate than) however long it later takes something else to notice/upload the file.
			pendingDuration = parseHLSExtinfDuration(line)
			continue
		}
		if strings.HasPrefix(line, "#") {
			continue // other tags (#EXT-X-VERSION, #EXT-X-ENDLIST, ...)
		}
		frags = append(frags, fragEntry{name: line, duration: pendingDuration})
		pendingDuration = 0
	}
	if err := scanner.Err(); err != nil {
		return
	}

	if !*sentInit && initURI != "" {
		initPath := filepath.Join(hlsDir, initURI)
		// ffmpeg writes the playlist's #EXT-X-MAP reference to init.mp4 slightly ahead of actually
		// closing that file to disk (unlike media fragments, which it only references after
		// rotating away from them) -- observed in practice as "cannot find the file specified" on
		// the very first tick the reference appeared. Only emit once the file really exists;
		// leaving sentInit false lets the next 1s tick retry instead of failing this epoch's
		// live-rewind permanently.
		if _, statErr := os.Stat(initPath); statErr == nil {
			out <- HLSFragmentEvent{Path: initPath, IsInit: true, Epoch: epoch}
			*sentInit = true
		}
	}
	if *sentFrags >= len(frags) {
		return
	}
	for _, entry := range frags[*sentFrags:] {
		out <- HLSFragmentEvent{Path: filepath.Join(hlsDir, entry.name), Epoch: epoch, Duration: entry.duration}
	}
	*sentFrags = len(frags)
}

// parseHLSMapURI extracts the quoted URI value out of an #EXT-X-MAP tag line,
// e.g. `#EXT-X-MAP:URI="init.mp4"` -> "init.mp4".
func parseHLSMapURI(line string) string {
	const marker = `URI="`
	start := strings.Index(line, marker)
	if start == -1 {
		return ""
	}
	start += len(marker)
	end := strings.Index(line[start:], `"`)
	if end == -1 {
		return ""
	}
	return line[start : start+end]
}

// parseHLSExtinfDuration extracts the numeric duration out of an #EXTINF tag line, e.g.
// `#EXTINF:4.000000,` -> 4s. Returns 0 if the line is malformed (never fails the caller --
// worst case a fragment's reported duration is 0, which manifests as a visibly wrong EXTINF
// downstream rather than blocking upload of an otherwise-good fragment).
func parseHLSExtinfDuration(line string) time.Duration {
	const prefix = "#EXTINF:"
	rest := strings.TrimPrefix(line, prefix)
	if comma := strings.IndexByte(rest, ','); comma != -1 {
		rest = rest[:comma]
	}
	secs, err := strconv.ParseFloat(rest, 64)
	if err != nil {
		return 0
	}
	return time.Duration(secs * float64(time.Second))
}
