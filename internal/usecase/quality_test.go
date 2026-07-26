package usecase

import (
	"testing"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"go.uber.org/zap"
)

func TestNormalizeStopReason(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"known reason", "Submitted", "Submitted"},
		{"recovery marker", "RecoveredAfterCrash", "RecoveredAfterCrash"},
		{"surrounding whitespace", "  CaptureFailure  ", "CaptureFailure"},
		{"empty stays empty", "", ""},
		{"unknown value dropped", "SomethingElse", ""},
		{"casing is not guessed at", "submitted", ""},
		// This string reaches operational logs and, later, the audit trail behind a grading
		// decision. An allowlist is what keeps it from being a log-injection hole.
		{"log injection attempt dropped", "Submitted\"}\n{\"level\":\"error", ""},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := NormalizeStopReason(c.in); got != c.want {
				t.Errorf("NormalizeStopReason(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// Every enum value the desktop client can send must survive normalisation, or the reason silently
// vanishes for exactly the stop that mattered.
func TestNormalizeStopReasonAcceptsEveryClientValue(t *testing.T) {
	// Mirrors RecordingStopReason in VoxOralExam.Core/Models/Recording.cs.
	for _, reason := range []string{
		"Submitted", "Expired", "UserClosed", "ApplicationShutdown", "CaptureFailure",
		"RecoveredAfterCrash",
	} {
		if got := NormalizeStopReason(reason); got != reason {
			t.Errorf("client sends %q but the server drops it (got %q)", reason, got)
		}
	}
}

func TestCheckDurationWindow(t *testing.T) {
	created := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	// A 30-minute window, the shape of a real exam slot.
	session := &cache.UploadSession{CreatedAt: created, ExpiresAt: created.Add(30 * time.Minute)}
	log := zap.NewNop()

	t.Run("recording inside the window is not flagged", func(t *testing.T) {
		quality := RecordingQuality{DurationSecs: 20 * 60}
		checkDurationWindow(log, session, &quality)
		if !quality.WindowKnown {
			t.Error("window should be known")
		}
		if quality.OverrunsWindow {
			t.Error("20 minutes inside a 30 minute window must not be flagged")
		}
	})

	// Finishing early is the ordinary case, so it must never become a finding -- this is the whole
	// reason the duration check only judges the overrun.
	t.Run("finishing early is reported but never flagged", func(t *testing.T) {
		quality := RecordingQuality{DurationSecs: 5 * 60}
		checkDurationWindow(log, session, &quality)
		if quality.OverrunsWindow {
			t.Error("a candidate finishing early must not be flagged")
		}
	})

	// A recording longer than the credentials that covered it cannot happen legitimately.
	t.Run("overrunning the window is flagged", func(t *testing.T) {
		quality := RecordingQuality{DurationSecs: 45 * 60}
		checkDurationWindow(log, session, &quality)
		if !quality.OverrunsWindow {
			t.Error("45 minutes in a 30 minute window must be flagged")
		}
	})

	// Clock skew between client and server, plus the segment that was mid-write when the window
	// closed, must not read as an overrun.
	t.Run("slightly over the window is within grace", func(t *testing.T) {
		quality := RecordingQuality{DurationSecs: 30*60 + 30}
		checkDurationWindow(log, session, &quality)
		if quality.OverrunsWindow {
			t.Errorf("30s past a 30 minute window is inside the %v grace", durationOverrunGrace)
		}
	})

	t.Run("no session leaves the window unknown", func(t *testing.T) {
		quality := RecordingQuality{DurationSecs: 45 * 60}
		checkDurationWindow(log, nil, &quality)
		if quality.WindowKnown || quality.OverrunsWindow {
			t.Errorf("got %+v, want an unknown window and no verdict", quality)
		}
	})

	// A watchdog-driven assembly reads a session that may carry nothing useful. An unknown window
	// must stay unknown rather than becoming a zero-length one that every recording overruns.
	t.Run("zero-length window yields no verdict", func(t *testing.T) {
		quality := RecordingQuality{DurationSecs: 60}
		checkDurationWindow(log, &cache.UploadSession{CreatedAt: created, ExpiresAt: created}, &quality)
		if quality.WindowKnown || quality.OverrunsWindow {
			t.Errorf("got %+v, want an unknown window and no verdict", quality)
		}
	})
}
