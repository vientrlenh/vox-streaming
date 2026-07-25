package webrtc

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

// Millisecond-precision RFC 3339, the form #EXT-X-PROGRAM-DATE-TIME expects
// (RFC 8216 §4.3.2.6). Fragment boundaries land on keyframes, never on whole
// seconds, so second precision would quantize every fragment's wall-clock
// anchor by up to a second and make the client's media<->wall-clock mapping
// drift visibly over a long exam.
const programDateTimeLayout = "2006-01-02T15:04:05.000Z07:00"

// buildLiveManifest renders an HLS (fMP4) media playlist for the live-rewind
// window of an in-progress stream, from Redis-tracked fragment metadata. It is
// rebuilt fresh on every request (never cached/served as a static file): the
// fragment window grows as new fragments land, so every call must reflect the
// current state.
//
// Asset URIs are emitted as playlist-RELATIVE names (see hlsFragmentAssetName)
// resolved by assetURI into this service's own /live/{scheduleId}/{streamId}/
// route — deliberately NOT presigned S3 URLs, which this used to embed. Two
// reasons: a presigned URL is ~1KB and expires, so a full-length DVR playlist
// became megabytes of churn rebuilt every few seconds; and because the
// signature changed on every rebuild, hls.js saw a brand-new #EXT-X-MAP URI on
// each reload and re-fetched + re-appended the init segment for every single
// fragment, which is exactly the buffer churn its MEDIA_ERROR recovery path
// was papering over.
//
// Every fragment carries an absolute wall-clock anchor via
// #EXT-X-PROGRAM-DATE-TIME at the head of each epoch. Without it a live
// playlist has no absolute time at all: hls.js maps whichever fragment happens
// to be first when it attaches to media time 0, so tearing the player down and
// re-attaching (stop watching, then watch again) silently re-based the whole
// timeline and lost the stream's real elapsed duration.
//
// Deliberately omits #EXT-X-PLAYLIST-TYPE (this is an in-progress live
// playlist, not EVENT/VOD) and computes #EXT-X-MEDIA-SEQUENCE from the first
// windowed fragment's Seq (not always 0) — getting either wrong is a classic
// way to make hls.js think fragments were skipped across manifest refreshes.
func buildLiveManifest(inits []cache.HLSInitMeta, frags []cache.HLSFragmentMeta, window time.Duration, assetURI func(name string) string) (string, error) {
	if len(frags) == 0 {
		return "", fmt.Errorf("no hls fragments available yet")
	}

	windowed := windowFragments(frags, window)
	if len(windowed) == 0 {
		windowed = frags[len(frags)-1:]
	}

	initByEpoch := make(map[int]cache.HLSInitMeta, len(inits))
	for _, i := range inits {
		initByEpoch[i.Epoch] = i
	}

	var targetDuration float64
	for _, f := range windowed {
		if d := f.EndedAt.Sub(f.StartedAt).Seconds(); d > targetDuration {
			targetDuration = d
		}
	}

	var b strings.Builder
	b.WriteString("#EXTM3U\n")
	b.WriteString("#EXT-X-VERSION:7\n")
	fmt.Fprintf(&b, "#EXT-X-TARGETDURATION:%d\n", int(math.Ceil(targetDuration)))
	fmt.Fprintf(&b, "#EXT-X-MEDIA-SEQUENCE:%d\n", windowed[0].Seq)
	// Counts the discontinuities the window has already scrolled past. Without
	// it, a client that joins after an ffmpeg restart has been trimmed away
	// cannot tell its timeline apart from one that never had a restart.
	fmt.Fprintf(&b, "#EXT-X-DISCONTINUITY-SEQUENCE:%d\n", discontinuitiesBefore(frags, windowed[0].Seq))

	prevEpoch := -1
	for _, f := range windowed {
		if f.Epoch != prevEpoch {
			if prevEpoch != -1 {
				b.WriteString("#EXT-X-DISCONTINUITY\n")
			}
			initMeta, ok := initByEpoch[f.Epoch]
			if !ok {
				return "", fmt.Errorf("missing hls init segment for epoch %d", f.Epoch)
			}
			fmt.Fprintf(&b, "#EXT-X-MAP:URI=%q\n", assetURI(hlsInitAssetName(initMeta.Epoch)))
			// Only at an epoch head: within an epoch the client derives each
			// fragment's date by accumulating #EXTINF from here, which matches
			// StartedAt exactly (both come from the same cumulative sum of
			// ffmpeg's own durations — see peer.go's runHLSFragmentUploader).
			fmt.Fprintf(&b, "#EXT-X-PROGRAM-DATE-TIME:%s\n", f.StartedAt.UTC().Format(programDateTimeLayout))
			prevEpoch = f.Epoch
		}

		fmt.Fprintf(&b, "#EXTINF:%.6f,\n%s\n", f.EndedAt.Sub(f.StartedAt).Seconds(), assetURI(hlsFragmentAssetName(f.Seq)))
	}

	return b.String(), nil
}

// windowFragments returns the trailing slice of frags (already sorted by Seq)
// whose combined span is <= window, summed backward from the newest. A window
// of 0 (or less) means "no trimming": serve the whole stream from its start,
// which is what makes the monitor's DVR cover the full elapsed duration
// instead of a sliding tail.
func windowFragments(frags []cache.HLSFragmentMeta, window time.Duration) []cache.HLSFragmentMeta {
	if window <= 0 {
		return frags
	}
	var total time.Duration
	start := len(frags)
	for i := len(frags) - 1; i >= 0; i-- {
		total += frags[i].EndedAt.Sub(frags[i].StartedAt)
		start = i
		if total >= window {
			break
		}
	}
	return frags[start:]
}

// discontinuitiesBefore counts epoch changes among the fragments the window
// has trimmed off the front, i.e. those with Seq < firstWindowedSeq.
func discontinuitiesBefore(frags []cache.HLSFragmentMeta, firstWindowedSeq int64) int {
	count := 0
	prevEpoch := -1
	for _, f := range frags {
		if f.Seq >= firstWindowedSeq {
			break
		}
		if prevEpoch != -1 && f.Epoch != prevEpoch {
			count++
		}
		prevEpoch = f.Epoch
	}
	// The boundary between the last trimmed fragment and the first windowed one
	// is itself a discontinuity if they straddle an epoch change.
	if prevEpoch != -1 {
		for _, f := range frags {
			if f.Seq == firstWindowedSeq {
				if f.Epoch != prevEpoch {
					count++
				}
				break
			}
		}
	}
	return count
}

// ── stable asset names ──────────────────────────────────────────────────────
//
// These are the playlist-relative filenames the manifest references and
// GetLiveAsset resolves back to Redis metadata. They intentionally encode
// nothing but the identifiers needed to look the asset up (epoch / seq): the
// S3 key is never exposed to the client, and the name is stable for the life
// of the stream so a client can re-fetch the same fragment while rewinding.

const (
	hlsInitAssetPrefix     = "init-"
	hlsInitAssetSuffix     = ".mp4"
	hlsFragmentAssetPrefix = "seg-"
	hlsFragmentAssetSuffix = ".m4s"
)

func hlsInitAssetName(epoch int) string {
	return fmt.Sprintf("%s%02d%s", hlsInitAssetPrefix, epoch, hlsInitAssetSuffix)
}

func hlsFragmentAssetName(seq int64) string {
	return fmt.Sprintf("%s%06d%s", hlsFragmentAssetPrefix, seq, hlsFragmentAssetSuffix)
}

type hlsAssetRef struct {
	IsInit bool
	Epoch  int
	Seq    int64
}

// parseHLSAssetName is the inverse of hlsInitAssetName/hlsFragmentAssetName.
// Rejects anything else outright rather than trying to be lenient — this
// parses a path segment straight off an unauthenticated request.
func parseHLSAssetName(name string) (hlsAssetRef, error) {
	switch {
	case strings.HasPrefix(name, hlsInitAssetPrefix) && strings.HasSuffix(name, hlsInitAssetSuffix):
		raw := strings.TrimSuffix(strings.TrimPrefix(name, hlsInitAssetPrefix), hlsInitAssetSuffix)
		epoch, err := strconv.Atoi(raw)
		if err != nil || epoch < 0 {
			return hlsAssetRef{}, fmt.Errorf("invalid hls init asset name %q", name)
		}
		return hlsAssetRef{IsInit: true, Epoch: epoch}, nil

	case strings.HasPrefix(name, hlsFragmentAssetPrefix) && strings.HasSuffix(name, hlsFragmentAssetSuffix):
		raw := strings.TrimSuffix(strings.TrimPrefix(name, hlsFragmentAssetPrefix), hlsFragmentAssetSuffix)
		seq, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || seq < 0 {
			return hlsAssetRef{}, fmt.Errorf("invalid hls fragment asset name %q", name)
		}
		return hlsAssetRef{Seq: seq}, nil
	}
	return hlsAssetRef{}, fmt.Errorf("unknown hls asset name %q", name)
}
