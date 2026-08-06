package media

import (
	"math"
	"testing"
)

// Fixtures below follow ffprobe's documented -print_format json shape, trimmed to the fields this
// package reads. They were written by hand rather than captured from a run, so they pin the
// parsing, not ffprobe's exact output for these particular files -- if a future ffprobe changes
// field names or types, these keep passing while Probe breaks. Anything depending on the real
// invocation has to be checked end to end (see the smoke test in .claude/RECORDING_PIPELINE.md).

// A desktop-uploaded segment: H.264 video, AAC audio from the client's Media Foundation writer.
const desktopSegmentProbe = `{
    "streams": [
        {
            "index": 0,
            "codec_name": "h264",
            "codec_type": "video",
            "width": 1280,
            "height": 720,
            "avg_frame_rate": "30000/1001",
            "nb_frames": "300"
        },
        {
            "index": 1,
            "codec_name": "aac",
            "codec_type": "audio",
            "avg_frame_rate": "0/0"
        }
    ],
    "format": {
        "duration": "10.016000"
    }
}`

// A WebRTC-ingested segment: Opus audio, and no frame count because the muxer writes fragmented
// MP4.
const webrtcSegmentProbe = `{
    "streams": [
        {
            "codec_name": "h264",
            "codec_type": "video",
            "width": 640,
            "height": 480,
            "avg_frame_rate": "15/1"
        },
        {
            "codec_name": "opus",
            "codec_type": "audio"
        }
    ],
    "format": {
        "duration": "1261.440000"
    }
}`

func TestParseProbeJSONDesktopSegment(t *testing.T) {
	got, err := parseProbeJSON([]byte(desktopSegmentProbe))
	if err != nil {
		t.Fatalf("parseProbeJSON: %v", err)
	}

	if !got.Video.Present || got.Video.Codec != "h264" {
		t.Errorf("video = %+v, want present h264", got.Video)
	}
	if got.Video.Width != 1280 || got.Video.Height != 720 {
		t.Errorf("resolution = %dx%d, want 1280x720", got.Video.Width, got.Video.Height)
	}
	if math.Abs(got.Video.AvgFrameRate-29.97) > 0.01 {
		t.Errorf("frame rate = %v, want ~29.97", got.Video.AvgFrameRate)
	}
	if got.Video.Frames != 300 {
		t.Errorf("frames = %d, want 300", got.Video.Frames)
	}
	if !got.Audio.Present || got.Audio.Codec != "aac" {
		t.Errorf("audio = %+v, want present aac", got.Audio)
	}
	if math.Abs(got.DurationSecs-10.016) > 0.001 {
		t.Errorf("duration = %v, want 10.016", got.DurationSecs)
	}
}

// The codec here is what decides whether concat can stream-copy the audio or has to transcode it,
// so reading it wrong silently either leaves an unplayable Opus-in-MP4 recording or re-encodes
// audio that was already fine.
func TestParseProbeJSONWebRTCSegmentReportsOpus(t *testing.T) {
	got, err := parseProbeJSON([]byte(webrtcSegmentProbe))
	if err != nil {
		t.Fatalf("parseProbeJSON: %v", err)
	}
	if got.Audio.Codec != "opus" {
		t.Errorf("audio codec = %q, want opus", got.Audio.Codec)
	}
	if got.Video.Frames != 0 {
		t.Errorf("frames = %d, want 0 when the container carries no count", got.Video.Frames)
	}
}

// A recording whose audio track never got muxed is the failure this exists to catch: it must not
// read as "present with an empty codec name".
func TestParseProbeJSONVideoOnly(t *testing.T) {
	got, err := parseProbeJSON([]byte(`{
        "streams": [{"codec_name": "h264", "codec_type": "video", "width": 800, "height": 600}],
        "format": {"duration": "5.0"}
    }`))
	if err != nil {
		t.Fatalf("parseProbeJSON: %v", err)
	}
	if got.Audio.Present {
		t.Errorf("audio = %+v, want absent", got.Audio)
	}
	if !got.Video.Present {
		t.Error("video should be present")
	}
}

func TestParseProbeJSONRejectsGarbage(t *testing.T) {
	if _, err := parseProbeJSON([]byte("not json")); err == nil {
		t.Fatal("expected an error for non-JSON ffprobe output")
	}
}

// ffprobe writes "0/0" when it cannot determine a rate. Treating that as a division and producing
// NaN would poison every downstream frame-rate comparison.
func TestParseRational(t *testing.T) {
	cases := []struct {
		in   string
		want float64
	}{
		{"30/1", 30},
		{"30000/1001", 29.97002997002997},
		{"0/0", 0},
		{"25", 25},
		{"", 0},
		{"garbage", 0},
	}
	for _, c := range cases {
		if got := parseRational(c.in); math.Abs(got-c.want) > 0.0001 {
			t.Errorf("parseRational(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestParsePacketCount(t *testing.T) {
	got, err := parsePacketCount([]byte(`{"streams":[{"nb_read_packets":"2340"}]}`))
	if err != nil {
		t.Fatalf("parsePacketCount: %v", err)
	}
	if got != 2340 {
		t.Errorf("got %d, want 2340", got)
	}
}

// Zero packets is a real, meaningful answer: a recording whose video never started. So the failure
// cases must be errors, never a zero that would be read as that answer.
func TestParsePacketCountFailsRatherThanReportingZero(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"no video stream selected", `{"streams":[]}`},
		{"count absent", `{"streams":[{}]}`},
		{"count not a number", `{"streams":[{"nb_read_packets":"N/A"}]}`},
		{"not json", `<?xml version="1.0"?>`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := parsePacketCount([]byte(c.in))
			if err == nil {
				t.Fatalf("got %d with no error, want an error", got)
			}
		})
	}
}

func TestParsePacketCountAcceptsGenuineZero(t *testing.T) {
	got, err := parsePacketCount([]byte(`{"streams":[{"nb_read_packets":"0"}]}`))
	if err != nil {
		t.Fatalf("an explicit zero is a valid count, not an error: %v", err)
	}
	if got != 0 {
		t.Errorf("got %d, want 0", got)
	}
}

const speechVolumeOutput = `[Parsed_volumedetect_0 @ 0x55d1] n_samples: 57722880
[Parsed_volumedetect_0 @ 0x55d1] mean_volume: -27.4 dB
[Parsed_volumedetect_0 @ 0x55d1] max_volume: -3.1 dB
[Parsed_volumedetect_0 @ 0x55d1] histogram_3db: 12
`

// 16-bit digital silence, i.e. the muted-microphone case this whole check exists for.
const silentVolumeOutput = `[Parsed_volumedetect_0 @ 0x55d1] n_samples: 57722880
[Parsed_volumedetect_0 @ 0x55d1] mean_volume: -91.0 dB
[Parsed_volumedetect_0 @ 0x55d1] max_volume: -91.0 dB
`

func TestParseVolumeDetectSpeech(t *testing.T) {
	got, err := parseVolumeDetect(speechVolumeOutput)
	if err != nil {
		t.Fatalf("parseVolumeDetect: %v", err)
	}
	if math.Abs(got.MeanDBFS-(-27.4)) > 0.001 {
		t.Errorf("mean = %v, want -27.4", got.MeanDBFS)
	}
	if math.Abs(got.PeakDBFS-(-3.1)) > 0.001 {
		t.Errorf("peak = %v, want -3.1", got.PeakDBFS)
	}
	if got.Silent() {
		t.Error("speech at -3.1 dB peak must not be reported as silent")
	}
}

func TestParseVolumeDetectSilence(t *testing.T) {
	got, err := parseVolumeDetect(silentVolumeOutput)
	if err != nil {
		t.Fatalf("parseVolumeDetect: %v", err)
	}
	if !got.Silent() {
		t.Errorf("peak %v dBFS must be reported as silent", got.PeakDBFS)
	}
}

// An oral exam is mostly gaps between answers, so a real recording can sit well below -40 dB mean
// while still containing perfectly audible speech. Thresholding on the mean would fail it.
func TestQuietButAudibleRecordingIsNotSilent(t *testing.T) {
	got, err := parseVolumeDetect(`[Parsed_volumedetect_0 @ 0x1] mean_volume: -46.2 dB
[Parsed_volumedetect_0 @ 0x1] max_volume: -18.7 dB
`)
	if err != nil {
		t.Fatalf("parseVolumeDetect: %v", err)
	}
	if got.Silent() {
		t.Errorf("mean %v / peak %v is a quiet room, not a dead mic", got.MeanDBFS, got.PeakDBFS)
	}
}

func TestParseVolumeDetectHandlesNegativeInfinity(t *testing.T) {
	got, err := parseVolumeDetect(`[Parsed_volumedetect_0 @ 0x1] mean_volume: -inf dB
[Parsed_volumedetect_0 @ 0x1] max_volume: -inf dB
`)
	if err != nil {
		t.Fatalf("parseVolumeDetect: %v", err)
	}
	if !math.IsInf(got.PeakDBFS, -1) {
		t.Errorf("peak = %v, want -Inf", got.PeakDBFS)
	}
	if !got.Silent() {
		t.Error("-inf peak must be reported as silent")
	}
}

// Without a max_volume line there is no verdict to give. Returning the zero value would read as
// 0 dBFS -- a recording at full scale -- and returning the -inf initialiser would flag every
// recording as silent. Both are worse than refusing to answer.
func TestParseVolumeDetectRequiresPeak(t *testing.T) {
	if _, err := parseVolumeDetect("ffmpeg version 6.0\nnothing useful here\n"); err == nil {
		t.Fatal("expected an error when volumedetect produced no max_volume")
	}
}
