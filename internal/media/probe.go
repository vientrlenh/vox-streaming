// Package media inspects finished recordings with ffprobe/ffmpeg so that a recording which is
// unplayable, missing a track, or captured in silence is caught while assembling it -- rather than
// discovered by whoever sits down to grade it, by which point the exam cannot be re-run.
package media

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os/exec"
	"strconv"
	"strings"
)

// SilentPeakDBFS is the peak level below which nothing resembling speech ever reached the
// microphone.
//
// Measured against real encodes (see TestMeasureAudioLevelSeparatesSilenceFromSpeech, which logs
// these): digital silence reports -91 dB, a full-scale tone -17.7 dB, and the same tone attenuated
// by 30 dB reports -47.4 dB. So the headroom above this threshold for a faint-but-real recording is
// only about 3 dB, not the comfortable margin it looks like -- treat it as tuned, not obvious, and
// re-measure before moving it.
//
// The asymmetry is what makes -50 defensible despite that: a false "silent" verdict costs one loud
// log line and nothing else, since inspectRecording never fails an assembly, while a missed one is
// only discovered by whoever sits down to grade an exam that cannot be re-run.
//
// Deliberately applied to the peak, not the mean, and a real recording off this system is what
// settles it: a 74-second screen capture with clearly audible speech measured -10.9 dB peak but
// only -39.7 dB mean. Thresholding the mean anywhere near the peak threshold would have failed
// that recording outright -- an oral exam is mostly silence between answers, and the mean measures
// the silence.
const SilentPeakDBFS = -50.0

// Report is what ffprobe can say about a file without decoding all of it.
type Report struct {
	DurationSecs float64
	Video        VideoTrack
	Audio        AudioTrack
}

type VideoTrack struct {
	Present bool
	Codec   string
	Width   int
	Height  int
	// AvgFrameRate comes from container metadata, not from counting, and on a concatenated
	// recording it is only indicative. Measured on one real session: the WebRTC-ingested copy of a
	// 30 fps screen capture reported 34.13 fps, which no 30 fps source can produce -- the RTP
	// timestamps survive the segment muxer and the concat badly enough to skew it. The
	// desktop-uploaded copy of the same screen reported 22.45, which is plausible on its own (a
	// static screen legitimately yields fewer frames) but is not evidence either.
	//
	// So: fine for logging, not sound enough to decide anything on. Anything that has to compare
	// against the client's declared FramesWritten needs a real count (ffprobe -count_frames ->
	// nb_read_frames), which decodes and therefore costs far more than this call.
	AvgFrameRate float64
	// Frames is 0 when the container does not carry a frame count, which is the normal case for
	// the fragmented MP4 written by both ingest paths. Do not read 0 as "no frames".
	Frames int64
}

type AudioTrack struct {
	Present bool
	Codec   string
}

// AudioLevel is the result of an actual decode pass over the audio.
type AudioLevel struct {
	MeanDBFS float64
	PeakDBFS float64
}

// Silent reports whether this recording is one nobody spoke into. See SilentPeakDBFS.
func (l AudioLevel) Silent() bool { return l.PeakDBFS <= SilentPeakDBFS }

// Probe reads container and stream metadata. It does not decode, so it is cheap enough to run on
// every recording, but for the same reason it cannot tell a valid file from one whose frames are
// corrupt -- MeasureAudioLevel's full decode pass is what catches that for the audio track.
func Probe(ctx context.Context, path string) (Report, error) {
	var out, errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-hide_banner", "-loglevel", "error",
		"-print_format", "json",
		"-show_format", "-show_streams",
		path,
	)
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return Report{}, fmt.Errorf("ffprobe %s: %w: %s", path, err, strings.TrimSpace(errBuf.String()))
	}
	return parseProbeJSON(out.Bytes())
}

// MeasureAudioLevel decodes the audio track in full and reports its mean and peak level.
//
// -vn skips video entirely and -f null discards the output, so the cost is one audio decode with no
// encode and no disk write. This is also the only check here that touches every sample, so a file
// that ffprobe describes happily but that cannot actually be decoded fails here.
func MeasureAudioLevel(ctx context.Context, path string) (AudioLevel, error) {
	var errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffmpeg",
		"-hide_banner", "-nostats",
		"-i", path,
		"-vn", "-af", "volumedetect",
		"-f", "null", "-",
	)
	// volumedetect reports on stderr alongside ffmpeg's ordinary logging; there is no way to get
	// it on stdout.
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return AudioLevel{}, fmt.Errorf("ffmpeg volumedetect %s: %w: %s", path, err, strings.TrimSpace(errBuf.String()))
	}
	return parseVolumeDetect(errBuf.String())
}

// CountPackets counts the video packets in a file.
//
// Deliberately -count_packets and not -count_frames: counting frames decodes every one of them,
// which on a 20-minute 1080p recording costs minutes of CPU per stream and would have to run for
// every stream of every candidate. Counting packets stops at the container layer, and for H.264 in
// MP4 one packet carries one access unit -- close enough to a frame for the only question being
// asked here, which is whether roughly the expected number of frames made it into the file.
//
// It is still an order of magnitude more expensive than Probe, since ffprobe must walk the whole
// file rather than read its header. Do not call it on a path where Probe would do.
func CountPackets(ctx context.Context, path string) (int64, error) {
	var out, errBuf bytes.Buffer
	cmd := exec.CommandContext(ctx, "ffprobe",
		"-hide_banner", "-loglevel", "error",
		"-select_streams", "v:0",
		"-count_packets",
		"-show_entries", "stream=nb_read_packets",
		"-print_format", "json",
		path,
	)
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("ffprobe -count_packets %s: %w: %s", path, err, strings.TrimSpace(errBuf.String()))
	}
	return parsePacketCount(out.Bytes())
}

type packetCountOutput struct {
	Streams []struct {
		NbReadPackets string `json:"nb_read_packets"`
	} `json:"streams"`
}

func parsePacketCount(data []byte) (int64, error) {
	var raw packetCountOutput
	if err := json.Unmarshal(data, &raw); err != nil {
		return 0, fmt.Errorf("parse ffprobe packet count: %w", err)
	}
	if len(raw.Streams) == 0 {
		return 0, fmt.Errorf("ffprobe reported no video stream to count")
	}
	count, err := strconv.ParseInt(raw.Streams[0].NbReadPackets, 10, 64)
	if err != nil {
		// An absent or unparseable count must not read as zero packets -- that is indistinguishable
		// from a recording whose video never started, which is a thing this is meant to detect.
		return 0, fmt.Errorf("ffprobe packet count %q is not a number: %w", raw.Streams[0].NbReadPackets, err)
	}
	return count, nil
}

type probeOutput struct {
	Streams []struct {
		CodecType    string `json:"codec_type"`
		CodecName    string `json:"codec_name"`
		Width        int    `json:"width"`
		Height       int    `json:"height"`
		AvgFrameRate string `json:"avg_frame_rate"`
		NbFrames     string `json:"nb_frames"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

func parseProbeJSON(data []byte) (Report, error) {
	var raw probeOutput
	if err := json.Unmarshal(data, &raw); err != nil {
		return Report{}, fmt.Errorf("parse ffprobe output: %w", err)
	}

	report := Report{}
	if secs, err := strconv.ParseFloat(raw.Format.Duration, 64); err == nil {
		report.DurationSecs = secs
	}

	for _, s := range raw.Streams {
		switch s.CodecType {
		case "video":
			if report.Video.Present {
				continue // first video track wins, matching ffmpeg's own default stream selection
			}
			frames, _ := strconv.ParseInt(s.NbFrames, 10, 64)
			report.Video = VideoTrack{
				Present:      true,
				Codec:        s.CodecName,
				Width:        s.Width,
				Height:       s.Height,
				AvgFrameRate: parseRational(s.AvgFrameRate),
				Frames:       frames,
			}
		case "audio":
			if report.Audio.Present {
				continue
			}
			report.Audio = AudioTrack{Present: true, Codec: s.CodecName}
		}
	}
	return report, nil
}

// parseRational reads ffprobe's "num/den" frame rates. A zero denominator is ffprobe's way of
// saying it could not determine a rate, not a malformed value, so it yields 0 rather than an error.
func parseRational(v string) float64 {
	num, den, ok := strings.Cut(v, "/")
	if !ok {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0
		}
		return f
	}
	n, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0
	}
	d, err := strconv.ParseFloat(den, 64)
	if err != nil || d == 0 {
		return 0
	}
	return n / d
}

// parseVolumeDetect pulls the two levels out of ffmpeg's stderr, which looks like:
//
//	[Parsed_volumedetect_0 @ 0x5..] n_samples: 57722880
//	[Parsed_volumedetect_0 @ 0x5..] mean_volume: -27.4 dB
//	[Parsed_volumedetect_0 @ 0x5..] max_volume: -3.1 dB
func parseVolumeDetect(stderr string) (AudioLevel, error) {
	level := AudioLevel{MeanDBFS: math.Inf(-1), PeakDBFS: math.Inf(-1)}
	foundPeak := false

	for _, line := range strings.Split(stderr, "\n") {
		switch {
		case strings.Contains(line, "mean_volume:"):
			if v, ok := parseDB(line, "mean_volume:"); ok {
				level.MeanDBFS = v
			}
		case strings.Contains(line, "max_volume:"):
			if v, ok := parseDB(line, "max_volume:"); ok {
				level.PeakDBFS = v
				foundPeak = true
			}
		}
	}

	if !foundPeak {
		// Without a peak there is no silence verdict to give, and reporting the -inf initialiser
		// would flag every recording as silent -- the exact false alarm that would get this check
		// switched off.
		return AudioLevel{}, fmt.Errorf("volumedetect reported no max_volume")
	}
	return level, nil
}

func parseDB(line, label string) (float64, bool) {
	_, after, ok := strings.Cut(line, label)
	if !ok {
		return 0, false
	}
	field := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(after), "dB"))
	if strings.EqualFold(field, "-inf") {
		return math.Inf(-1), true
	}
	v, err := strconv.ParseFloat(field, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}
