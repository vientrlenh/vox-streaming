package webrtc

import (
	"strings"
	"testing"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

func hlsFrag(seq int64, epoch int, startedAt time.Time, dur time.Duration) cache.HLSFragmentMeta {
	return cache.HLSFragmentMeta{
		Seq:       seq,
		Epoch:     epoch,
		S3Key:     "frag-key",
		StartedAt: startedAt,
		EndedAt:   startedAt.Add(dur),
	}
}

// Mirrors what the handler does: playlist-relative name + the per-stream asset
// signature.
func assetURIEcho(name string) string {
	return name + "?sig=tok"
}

func TestBuildLiveManifest_NoFragments(t *testing.T) {
	_, err := buildLiveManifest(nil, nil, time.Minute, false, assetURIEcho)
	if err == nil {
		t.Fatal("expected error when no fragments are available yet")
	}
}

func TestBuildLiveManifest_Basic(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	frags := []cache.HLSFragmentMeta{
		hlsFrag(5, 1, base, 4*time.Second),
		hlsFrag(6, 1, base.Add(4*time.Second), 4*time.Second),
	}

	manifest, err := buildLiveManifest(inits, frags, time.Hour, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.HasPrefix(manifest, "#EXTM3U\n") {
		t.Errorf("manifest should start with #EXTM3U, got: %q", manifest)
	}
	if strings.Contains(manifest, "#EXT-X-PLAYLIST-TYPE") {
		t.Error("served manifest must be true in-progress live, not tagged EVENT/VOD")
	}
	if !strings.Contains(manifest, "#EXT-X-MEDIA-SEQUENCE:5") {
		t.Errorf("expected media sequence to be the first windowed fragment's Seq (5), got: %q", manifest)
	}
	if !strings.Contains(manifest, `#EXT-X-MAP:URI="init-01.mp4?sig=tok"`) {
		t.Errorf("expected playlist-relative init URI in manifest, got: %q", manifest)
	}
	if !strings.Contains(manifest, "seg-000005.m4s?sig=tok") || !strings.Contains(manifest, "seg-000006.m4s?sig=tok") {
		t.Errorf("expected playlist-relative fragment URIs keyed by Seq, got: %q", manifest)
	}
	if strings.Contains(manifest, "frag-key") {
		t.Errorf("manifest must never leak stored object keys, got: %q", manifest)
	}
	if strings.Count(manifest, "#EXTINF:") != 2 {
		t.Errorf("expected 2 #EXTINF entries, got: %q", manifest)
	}
}

// A live playlist must never carry #EXT-X-ENDLIST: the player would stop reloading it and the
// stream would appear to freeze at whatever fragment happened to be last.
func TestBuildLiveManifest_LivePlaylistHasNoEndList(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	frags := []cache.HLSFragmentMeta{hlsFrag(0, 1, base, 4*time.Second)}

	manifest, err := buildLiveManifest(inits, frags, time.Hour, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(manifest, "#EXT-X-ENDLIST") {
		t.Errorf("a live playlist must stay open, got: %q", manifest)
	}
}

// And a finished stream must carry it. Without ENDLIST the player keeps treating the playlist as
// live: it polls forever for fragments that will never arrive and parks the playhead behind a live
// edge that no longer moves, so the seek bar can never fill and the behind-live warning never clears.
func TestBuildLiveManifest_EndedPlaylistClosesWithEndList(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	frags := []cache.HLSFragmentMeta{
		hlsFrag(0, 1, base, 4*time.Second),
		hlsFrag(1, 1, base.Add(4*time.Second), 4*time.Second),
	}

	manifest, err := buildLiveManifest(inits, frags, time.Hour, true, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasSuffix(manifest, "#EXT-X-ENDLIST\n") {
		t.Errorf("ended playlist must close with #EXT-X-ENDLIST, got: %q", manifest)
	}
	// Everything else stays identical -- ENDLIST changes how the playlist is consumed, not what it
	// lists, and a replay that dropped fragments would be worse than no replay at all.
	if strings.Count(manifest, "#EXTINF:") != 2 {
		t.Errorf("expected both fragments to survive, got: %q", manifest)
	}
}

// The whole point of the absolute anchor: a client attaching at any moment can
// map media time back to wall-clock time, so tearing the player down and
// re-attaching does not re-base the timeline.
func TestBuildLiveManifest_ProgramDateTimeAnchorsFirstFragment(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 30, 15, 500*int(time.Millisecond), time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	frags := []cache.HLSFragmentMeta{
		hlsFrag(0, 1, base, 4*time.Second),
		hlsFrag(1, 1, base.Add(4*time.Second), 4*time.Second),
	}

	manifest, err := buildLiveManifest(inits, frags, time.Hour, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(manifest, "#EXT-X-PROGRAM-DATE-TIME:2026-01-01T10:30:15.500Z") {
		t.Errorf("expected millisecond-precision PDT anchored on the first windowed fragment, got: %q", manifest)
	}
	// One per epoch head only — the client accumulates #EXTINF for the rest.
	if got := strings.Count(manifest, "#EXT-X-PROGRAM-DATE-TIME:"); got != 1 {
		t.Errorf("expected exactly 1 PDT tag for a single-epoch playlist, got %d: %q", got, manifest)
	}
}

// A window that has slid forward must re-anchor PDT on whatever fragment is now
// first, otherwise the client's wall-clock mapping is off by however much was
// trimmed.
func TestBuildLiveManifest_ProgramDateTimeFollowsWindow(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	var frags []cache.HLSFragmentMeta
	for i := int64(0); i < 10; i++ {
		frags = append(frags, hlsFrag(i, 1, base.Add(time.Duration(i)*time.Minute), time.Minute))
	}

	manifest, err := buildLiveManifest(inits, frags, 3*time.Minute, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Window keeps seq 7,8,9 -> first windowed fragment starts at 10:07:00.
	if !strings.Contains(manifest, "#EXT-X-PROGRAM-DATE-TIME:2026-01-01T10:07:00.000Z") {
		t.Errorf("expected PDT of the first windowed fragment (10:07), got: %q", manifest)
	}
}

func TestBuildLiveManifest_Windowing(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	// 10 fragments, 1 minute each, spanning 10 minutes total.
	var frags []cache.HLSFragmentMeta
	for i := int64(0); i < 10; i++ {
		frags = append(frags, hlsFrag(i, 1, base.Add(time.Duration(i)*time.Minute), time.Minute))
	}

	manifest, err := buildLiveManifest(inits, frags, 3*time.Minute, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// A 3-minute trailing window over 1-minute fragments should keep the last 3
	// (seq 7, 8, 9) and drop the rest.
	if strings.Count(manifest, "#EXTINF:") != 3 {
		t.Errorf("expected window to keep exactly 3 fragments, got: %q", manifest)
	}
	if !strings.Contains(manifest, "#EXT-X-MEDIA-SEQUENCE:7") {
		t.Errorf("expected media sequence 7 (first windowed fragment), got: %q", manifest)
	}
}

// window <= 0 means "full DVR from the stream's start", which is what makes the
// monitor's scrub bar span the whole elapsed stream instead of a sliding tail.
func TestBuildLiveManifest_ZeroWindowKeepsEverything(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{{Epoch: 1, S3Key: "init-1"}}
	var frags []cache.HLSFragmentMeta
	for i := int64(0); i < 50; i++ {
		frags = append(frags, hlsFrag(i, 1, base.Add(time.Duration(i)*time.Minute), time.Minute))
	}

	manifest, err := buildLiveManifest(inits, frags, 0, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Count(manifest, "#EXTINF:") != 50 {
		t.Errorf("expected all 50 fragments with a zero window, got: %q", manifest)
	}
	if !strings.Contains(manifest, "#EXT-X-MEDIA-SEQUENCE:0") {
		t.Errorf("expected media sequence 0, got: %q", manifest)
	}
	if !strings.Contains(manifest, "#EXT-X-PROGRAM-DATE-TIME:2026-01-01T10:00:00.000Z") {
		t.Errorf("expected PDT anchored on the stream's very first fragment, got: %q", manifest)
	}
}

func TestBuildLiveManifest_DiscontinuityOnEpochChange(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{
		{Epoch: 1, S3Key: "init-1"},
		{Epoch: 2, S3Key: "init-2"},
	}
	frags := []cache.HLSFragmentMeta{
		hlsFrag(0, 1, base, 4*time.Second),
		hlsFrag(1, 1, base.Add(4*time.Second), 4*time.Second),
		// ffmpeg restarted here -> new epoch, new init segment
		hlsFrag(2, 2, base.Add(20*time.Second), 4*time.Second),
	}

	manifest, err := buildLiveManifest(inits, frags, time.Hour, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if strings.Count(manifest, "#EXT-X-DISCONTINUITY\n") != 1 {
		t.Errorf("expected exactly 1 discontinuity marker at the epoch change, got: %q", manifest)
	}
	if strings.Count(manifest, "#EXT-X-MAP:") != 2 {
		t.Errorf("expected 2 init-segment references (one per epoch), got: %q", manifest)
	}
	if !strings.Contains(manifest, `URI="init-01.mp4?sig=tok"`) || !strings.Contains(manifest, `URI="init-02.mp4?sig=tok"`) {
		t.Errorf("expected both epoch's init segments referenced, got: %q", manifest)
	}
	// A restart is a real wall-clock gap (backoff + ffmpeg startup), so the new
	// epoch has to re-anchor rather than let the client accumulate across it.
	if got := strings.Count(manifest, "#EXT-X-PROGRAM-DATE-TIME:"); got != 2 {
		t.Errorf("expected a PDT anchor per epoch, got %d: %q", got, manifest)
	}
	if !strings.Contains(manifest, "#EXT-X-DISCONTINUITY-SEQUENCE:0") {
		t.Errorf("expected discontinuity sequence 0 while nothing has been trimmed, got: %q", manifest)
	}
}

// Once the window has scrolled past a restart, the discontinuity is no longer in
// the playlist but the client still needs to be told it happened.
func TestBuildLiveManifest_DiscontinuitySequenceCountsTrimmed(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	inits := []cache.HLSInitMeta{
		{Epoch: 1, S3Key: "init-1"},
		{Epoch: 2, S3Key: "init-2"},
	}
	frags := []cache.HLSFragmentMeta{
		hlsFrag(0, 1, base, time.Minute),
		hlsFrag(1, 1, base.Add(time.Minute), time.Minute),
		hlsFrag(2, 2, base.Add(2*time.Minute), time.Minute),
		hlsFrag(3, 2, base.Add(3*time.Minute), time.Minute),
	}

	// 1-minute window keeps only seq 3, trimming the epoch 1 -> 2 boundary away.
	manifest, err := buildLiveManifest(inits, frags, time.Minute, false, assetURIEcho)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(manifest, "#EXT-X-DISCONTINUITY-SEQUENCE:1") {
		t.Errorf("expected the trimmed-away epoch change to be counted, got: %q", manifest)
	}
}

func TestBuildLiveManifest_MissingInitForEpoch(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	frags := []cache.HLSFragmentMeta{hlsFrag(0, 1, base, 4*time.Second)}

	_, err := buildLiveManifest(nil, frags, time.Hour, false, assetURIEcho)
	if err == nil {
		t.Fatal("expected error when no init segment is registered for the fragment's epoch")
	}
}

func TestHLSAssetNameRoundTrip(t *testing.T) {
	initRef, err := parseHLSAssetName(hlsInitAssetName(2))
	if err != nil {
		t.Fatalf("init name should parse: %v", err)
	}
	if !initRef.IsInit || initRef.Epoch != 2 {
		t.Errorf("got %+v, want init epoch 2", initRef)
	}

	fragRef, err := parseHLSAssetName(hlsFragmentAssetName(123456))
	if err != nil {
		t.Fatalf("fragment name should parse: %v", err)
	}
	if fragRef.IsInit || fragRef.Seq != 123456 {
		t.Errorf("got %+v, want fragment seq 123456", fragRef)
	}

	// Seq is not zero-padding-bounded: the name is generated, not user-typed,
	// and a long stream really does exceed 6 digits.
	wideRef, err := parseHLSAssetName(hlsFragmentAssetName(12345678))
	if err != nil || wideRef.Seq != 12345678 {
		t.Errorf("got %+v (err %v), want fragment seq 12345678", wideRef, err)
	}
}

func TestParseHLSAssetNameRejectsJunk(t *testing.T) {
	for _, name := range []string{
		"", "playlist.m3u8", "seg-.m4s", "init-.mp4", "seg-abc.m4s", "init-01.m4s",
		"../../etc/passwd", "seg--1.m4s", "recording.mp4", "seg-000001.mp4",
	} {
		if _, err := parseHLSAssetName(name); err == nil {
			t.Errorf("expected %q to be rejected", name)
		}
	}
}
