package webrtc

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
)

func claimsWith(exp time.Time, scheduleEndAt int64) *auth.StreamClaims {
	claims := &auth.StreamClaims{ScheduleEndAt: scheduleEndAt}
	if !exp.IsZero() {
		claims.ExpiresAt = jwt.NewNumericDate(exp)
	}
	return claims
}

// The property that matters most here is one-directional: being late costs a delayed recording, being
// early destroys one, because assembly short-circuits on an existing recording.mp4 and a truncated
// file can never be replaced by the complete one.
func TestWebRTCAssemblyDueAt(t *testing.T) {
	now := time.Now().UTC()

	t.Run("schedule end wins when it runs past the token", func(t *testing.T) {
		scheduleEnd := now.Add(3 * time.Hour)
		got := webrtcAssemblyDueAt(claimsWith(now.Add(10*time.Minute), scheduleEnd.Unix()))

		want := scheduleEnd.Add(webrtcAssemblyGrace)
		if got.Before(want.Add(-2*time.Second)) || got.After(want.Add(2*time.Second)) {
			t.Fatalf("got %v, want ~%v", got, want)
		}
	})

	t.Run("token expiry is used when no schedule end is present", func(t *testing.T) {
		exp := now.Add(2 * time.Hour)
		got := webrtcAssemblyDueAt(claimsWith(exp, 0))

		want := exp.Add(webrtcAssemblyGrace)
		if got.Before(want.Add(-2*time.Second)) || got.After(want.Add(2*time.Second)) {
			t.Fatalf("got %v, want ~%v", got, want)
		}
	})

	// A stale token must never produce a due time in the past: that would let the very next watchdog
	// sweep assemble a stream that is still live and permanently truncate it. Tokens are short-lived
	// and re-minted mid-exam, so an expired one arriving here is ordinary, not exceptional.
	t.Run("an already-expired token still yields a future due time", func(t *testing.T) {
		got := webrtcAssemblyDueAt(claimsWith(now.Add(-time.Hour), 0))

		if !got.After(now.Add(webrtcAssemblyGrace - 2*time.Second)) {
			t.Fatalf("got %v, want at least the grace period from now (%v)", got, now.Add(webrtcAssemblyGrace))
		}
	})

	t.Run("no expiry and no schedule end still yields a future due time", func(t *testing.T) {
		got := webrtcAssemblyDueAt(claimsWith(time.Time{}, 0))

		if !got.After(now.Add(webrtcAssemblyGrace - 2*time.Second)) {
			t.Fatalf("got %v, want at least the grace period from now", got)
		}
	})
}
