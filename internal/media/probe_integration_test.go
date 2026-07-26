package media

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

// The unit tests in probe_test.go pin the parsing against hand-written fixtures, which cannot catch
// the failures that actually matter here: a wrong ffprobe flag, an output shape that differs from
// the documented one, or a silence threshold that does not survive a real encode/decode round trip.
// These tests run the real binaries against files ffmpeg itself generates, and skip when the
// binaries are unavailable so the suite stays runnable without them.

func requireFFmpeg(t *testing.T) {
	t.Helper()
	for _, bin := range []string{"ffmpeg", "ffprobe"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not on PATH; skipping integration test", bin)
		}
	}
}

// generate builds a short test file, skipping the test when this ffmpeg build lacks the encoder
// rather than reporting a failure that says nothing about the code under test.
func generate(t *testing.T, name string, args ...string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	full := append([]string{"-hide_banner", "-loglevel", "error", "-y"}, args...)
	full = append(full, path)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffmpeg", full...).CombinedOutput()
	if err != nil {
		t.Skipf("this ffmpeg build could not generate %s (%v): %s", name, err, out)
	}
	return path
}

func TestProbeRealAACFile(t *testing.T) {
	requireFFmpeg(t)
	path := generate(t, "aac.mp4",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2",
		"-c:v", "libx264", "-c:a", "aac", "-shortest",
	)

	report, err := Probe(context.Background(), path)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}

	if !report.Video.Present || report.Video.Codec != "h264" {
		t.Errorf("video = %+v, want present h264", report.Video)
	}
	if report.Video.Width != 320 || report.Video.Height != 240 {
		t.Errorf("resolution = %dx%d, want 320x240", report.Video.Width, report.Video.Height)
	}
	if report.Video.AvgFrameRate < 14 || report.Video.AvgFrameRate > 16 {
		t.Errorf("frame rate = %v, want ~15", report.Video.AvgFrameRate)
	}
	if !report.Audio.Present || report.Audio.Codec != "aac" {
		t.Errorf("audio = %+v, want present aac", report.Audio)
	}
	if report.DurationSecs < 1.5 || report.DurationSecs > 2.5 {
		t.Errorf("duration = %v, want ~2", report.DurationSecs)
	}
}

// This is the case that drives the transcode decision in the assembler: if Probe reports anything
// other than "opus" here, WebRTC recordings keep shipping as unplayable Opus-in-MP4.
func TestProbeRealOpusFileDrivesTranscodeDecision(t *testing.T) {
	requireFFmpeg(t)
	path := generate(t, "opus.mp4",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=2",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=2",
		"-c:v", "libx264", "-c:a", "libopus", "-shortest",
	)

	report, err := Probe(context.Background(), path)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if report.Audio.Codec != "opus" {
		t.Errorf("audio codec = %q, want opus", report.Audio.Codec)
	}
}

func TestProbeRealVideoOnlyFile(t *testing.T) {
	requireFFmpeg(t)
	path := generate(t, "silent.mp4",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=2",
		"-c:v", "libx264", "-an",
	)

	report, err := Probe(context.Background(), path)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if report.Audio.Present {
		t.Errorf("audio = %+v, want absent", report.Audio)
	}
	if !report.Video.Present {
		t.Error("video should be present")
	}
}

func TestProbeRejectsNonMedia(t *testing.T) {
	requireFFmpeg(t)
	path := filepath.Join(t.TempDir(), "not-media.mp4")
	if err := writeFile(path, "this is not an mp4"); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := Probe(context.Background(), path); err == nil {
		t.Fatal("expected Probe to fail on a file that is not media")
	}
}

// The whole point of 6.1: a recording nobody spoke into must be distinguishable from one that
// captured speech, through a real encode and a real decode -- not just in the parser.
func TestMeasureAudioLevelSeparatesSilenceFromSpeech(t *testing.T) {
	requireFFmpeg(t)

	silent := generate(t, "quiet.mp4",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=3",
		"-f", "lavfi", "-i", "anullsrc=r=48000:cl=mono",
		"-c:v", "libx264", "-c:a", "aac", "-shortest",
	)
	loud := generate(t, "loud.mp4",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=3",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-c:v", "libx264", "-c:a", "aac", "-shortest",
	)

	silentLevel, err := MeasureAudioLevel(context.Background(), silent)
	if err != nil {
		t.Fatalf("MeasureAudioLevel(silent): %v", err)
	}
	if !silentLevel.Silent() {
		t.Errorf("a null audio source measured peak=%v mean=%v and was not flagged silent",
			silentLevel.PeakDBFS, silentLevel.MeanDBFS)
	}

	loudLevel, err := MeasureAudioLevel(context.Background(), loud)
	if err != nil {
		t.Fatalf("MeasureAudioLevel(loud): %v", err)
	}
	if loudLevel.Silent() {
		t.Errorf("a 440Hz tone measured peak=%v mean=%v and was wrongly flagged silent",
			loudLevel.PeakDBFS, loudLevel.MeanDBFS)
	}

	t.Logf("silent: peak=%.1f mean=%.1f | tone: peak=%.1f mean=%.1f (threshold %.1f)",
		silentLevel.PeakDBFS, silentLevel.MeanDBFS,
		loudLevel.PeakDBFS, loudLevel.MeanDBFS, SilentPeakDBFS)
}

// A quiet-but-real recording is the false alarm that would get this check switched off. -30 dB is
// well below normal speech yet unmistakably not silence.
//
// This is the tightest case in the suite: it measures around -47.4 dB peak against a -50 threshold,
// roughly 3 dB of headroom. That is deliberate -- it is the guard rail on SilentPeakDBFS. If
// somebody raises the threshold, this test is what fails, and it should.
func TestQuietRealAudioIsNotFlaggedSilent(t *testing.T) {
	requireFFmpeg(t)
	path := generate(t, "faint.mp4",
		"-f", "lavfi", "-i", "testsrc=size=320x240:rate=15:duration=3",
		"-f", "lavfi", "-i", "sine=frequency=440:duration=3",
		"-af", "volume=-30dB",
		"-c:v", "libx264", "-c:a", "aac", "-shortest",
	)

	level, err := MeasureAudioLevel(context.Background(), path)
	if err != nil {
		t.Fatalf("MeasureAudioLevel: %v", err)
	}
	if level.Silent() {
		t.Errorf("a -30dB tone (peak=%v mean=%v) must not read as silent; the threshold is too high",
			level.PeakDBFS, level.MeanDBFS)
	}
	t.Logf("faint tone: peak=%.1f mean=%.1f", level.PeakDBFS, level.MeanDBFS)
}
