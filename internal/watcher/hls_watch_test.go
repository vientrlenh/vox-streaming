package watcher

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writePlaylist(t *testing.T, dir, content string) string {
	t.Helper()
	path := filepath.Join(dir, "live.m3u8")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write playlist: %v", err)
	}
	return path
}

func TestEmitNewHLSEntries(t *testing.T) {
	dir := t.TempDir()

	t.Run("missing playlist file is a no-op, not an error", func(t *testing.T) {
		sentInit := false
		sentFrags := 0
		out := make(chan HLSFragmentEvent, 8)
		emitNewHLSEntries(filepath.Join(dir, "does-not-exist.m3u8"), dir, 1, &sentInit, &sentFrags, out)
		close(out)
		var events []HLSFragmentEvent
		for e := range out {
			events = append(events, e)
		}
		if len(events) != 0 {
			t.Errorf("got %v, want no events", events)
		}
	})

	t.Run("init.mp4 not yet on disk is skipped, retried once it appears", func(t *testing.T) {
		subdir := t.TempDir()
		playlistPath := writePlaylist(t, subdir, `#EXTM3U
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4.000000,
000000.m4s
`)
		sentInit := false
		sentFrags := 0
		out := make(chan HLSFragmentEvent, 8)

		// init.mp4 is referenced in the playlist but not actually written yet -- must not be
		// emitted, and sentInit must stay false so the next tick retries it.
		emitNewHLSEntries(playlistPath, subdir, 5, &sentInit, &sentFrags, out)
		if sentInit {
			t.Fatal("got sentInit=true, want false: init.mp4 does not exist on disk yet")
		}
		if sentFrags != 1 {
			t.Fatalf("got sentFrags=%d, want 1: fragments must still be emitted even if init isn't ready", sentFrags)
		}

		if err := os.WriteFile(filepath.Join(subdir, "init.mp4"), []byte("x"), 0o600); err != nil {
			t.Fatalf("write init.mp4: %v", err)
		}
		emitNewHLSEntries(playlistPath, subdir, 5, &sentInit, &sentFrags, out)
		close(out)

		var events []HLSFragmentEvent
		for e := range out {
			events = append(events, e)
		}
		// 1 from the first call (000000.m4s only, init skipped) + 1 from the second (init.mp4,
		// now that it exists; no fragment re-sent since sentFrags already caught up).
		if len(events) != 2 {
			t.Fatalf("got %d events, want 2: %+v", len(events), events)
		}
		if events[0].IsInit || events[0].Path != filepath.Join(subdir, "000000.m4s") {
			t.Errorf("event 0: got %+v, want 000000.m4s IsInit=false", events[0])
		}
		if !events[1].IsInit || events[1].Path != filepath.Join(subdir, "init.mp4") {
			t.Errorf("event 1: got %+v, want init.mp4 IsInit=true", events[1])
		}
		if !sentInit {
			t.Error("got sentInit=false after init.mp4 appeared, want true")
		}
	})

	t.Run("emits init once then only newly-appended fragments", func(t *testing.T) {
		subdir := filepath.Join(dir, "attempt")
		if err := os.MkdirAll(subdir, 0o700); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		if err := os.WriteFile(filepath.Join(subdir, "init.mp4"), []byte("x"), 0o600); err != nil {
			t.Fatalf("write init.mp4: %v", err)
		}
		playlistPath := writePlaylist(t, subdir, `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4.000000,
000000.m4s
`)

		sentInit := false
		sentFrags := 0
		out := make(chan HLSFragmentEvent, 8)

		emitNewHLSEntries(playlistPath, subdir, 3, &sentInit, &sentFrags, out)
		if !sentInit || sentFrags != 1 {
			t.Fatalf("got sentInit=%v sentFrags=%d, want true/1", sentInit, sentFrags)
		}

		// Simulate ffmpeg appending a second fragment before the next poll tick. Durations
		// deliberately differ (4.0s vs 3.5s) so a misaligned EXTINF-to-fragment pairing would
		// show up as a wrong value rather than accidentally matching either way.
		writePlaylist(t, subdir, `#EXTM3U
#EXT-X-VERSION:7
#EXT-X-TARGETDURATION:4
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:EVENT
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4.000000,
000000.m4s
#EXTINF:3.500000,
000001.m4s
`)
		emitNewHLSEntries(playlistPath, subdir, 3, &sentInit, &sentFrags, out)
		close(out)

		var events []HLSFragmentEvent
		for e := range out {
			events = append(events, e)
		}
		// 2 from the first call (init + 000000.m4s) + 1 from the second
		// (000001.m4s only — 000000.m4s and init must not be resent).
		if len(events) != 3 {
			t.Fatalf("got %d events, want 3: %+v", len(events), events)
		}
		if !events[0].IsInit || events[0].Path != filepath.Join(subdir, "init.mp4") || events[0].Epoch != 3 {
			t.Errorf("event 0: got %+v, want init.mp4 IsInit=true Epoch=3", events[0])
		}
		if events[1].IsInit || events[1].Path != filepath.Join(subdir, "000000.m4s") || events[1].Duration != 4*time.Second {
			t.Errorf("event 1: got %+v, want 000000.m4s IsInit=false Duration=4s", events[1])
		}
		if events[2].IsInit || events[2].Path != filepath.Join(subdir, "000001.m4s") || events[2].Duration != 3500*time.Millisecond {
			t.Errorf("event 2: got %+v, want 000001.m4s IsInit=false Duration=3.5s (only the newly-appended fragment)", events[2])
		}
	})

	t.Run("EXT-X-ENDLIST is ignored, not treated as a fragment", func(t *testing.T) {
		subdir := t.TempDir()
		if err := os.WriteFile(filepath.Join(subdir, "init.mp4"), []byte("x"), 0o600); err != nil {
			t.Fatalf("write init.mp4: %v", err)
		}
		playlistPath := writePlaylist(t, subdir, `#EXTM3U
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4.000000,
000000.m4s
#EXT-X-ENDLIST
`)
		sentInit := false
		sentFrags := 0
		out := make(chan HLSFragmentEvent, 8)
		emitNewHLSEntries(playlistPath, subdir, 1, &sentInit, &sentFrags, out)
		close(out)
		var events []HLSFragmentEvent
		for e := range out {
			events = append(events, e)
		}
		if len(events) != 2 { // init + the one real fragment, ENDLIST excluded
			t.Fatalf("got %d events, want 2: %+v", len(events), events)
		}
	})
}

func TestParseHLSMapURI(t *testing.T) {
	tests := []struct {
		name string
		line string
		want string
	}{
		{"basic", `#EXT-X-MAP:URI="init.mp4"`, "init.mp4"},
		{"no uri attribute", `#EXT-X-MAP:BYTERANGE="100@0"`, ""},
		{"not a map line", `#EXTINF:4.000000,`, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseHLSMapURI(tt.line); got != tt.want {
				t.Errorf("parseHLSMapURI(%q) = %q, want %q", tt.line, got, tt.want)
			}
		})
	}
}

func TestParseHLSExtinfDuration(t *testing.T) {
	tests := []struct {
		name string
		line string
		want time.Duration
	}{
		{"basic", "#EXTINF:4.000000,", 4 * time.Second},
		{"fractional", "#EXTINF:3.500000,", 3500 * time.Millisecond},
		{"no trailing comma", "#EXTINF:4.000000", 4 * time.Second},
		{"malformed", "#EXTINF:oops,", 0},
		{"not an extinf line", `#EXT-X-MAP:URI="init.mp4"`, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseHLSExtinfDuration(tt.line); got != tt.want {
				t.Errorf("parseHLSExtinfDuration(%q) = %v, want %v", tt.line, got, tt.want)
			}
		})
	}
}

func TestWatchHLSFragments_EmitsOnContextDone(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "init.mp4"), []byte("x"), 0o600); err != nil {
		t.Fatalf("write init.mp4: %v", err)
	}
	writePlaylist(t, dir, `#EXTM3U
#EXT-X-MAP:URI="init.mp4"
#EXTINF:4.000000,
000000.m4s
`)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled — WatchHLSFragments should still do one final emit before returning

	var events []HLSFragmentEvent
	for e := range WatchHLSFragments(ctx, dir, 7) {
		events = append(events, e)
	}
	if len(events) != 2 {
		t.Fatalf("got %d events, want 2 (init + fragment)", len(events))
	}
	if events[0].Epoch != 7 || events[1].Epoch != 7 {
		t.Errorf("got events %+v, want Epoch=7 on both", events)
	}
}
