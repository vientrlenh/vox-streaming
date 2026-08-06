package storage

import "testing"

func TestHLSInitKey(t *testing.T) {
	got := hlsInitKey("schedule-1", "session-1", "stream-1", 3)
	want := "schedules/schedule-1/sessions/session-1/streams/stream-1/hls/init-03.mp4"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestHLSFragmentKey(t *testing.T) {
	got := hlsFragmentKey("schedule-1", "session-1", "stream-1", 42)
	want := "schedules/schedule-1/sessions/session-1/streams/stream-1/hls/000042.m4s"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}
