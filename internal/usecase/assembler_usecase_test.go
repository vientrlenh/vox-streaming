package usecase

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

func TestRecordingSummary(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	t.Run("no gaps: duration is total span from first start to last end", func(t *testing.T) {
		metas := []cache.SegmentMeta{
			seg(0, base, 5*time.Second),
			seg(1, base.Add(5*time.Second), 5*time.Second),
		}
		durationSecs, hasGaps := recordingSummary(metas)
		if hasGaps {
			t.Error("expected hasGaps=false for contiguous segments")
		}
		if durationSecs != 10 {
			t.Errorf("got durationSecs=%d, want 10", durationSecs)
		}
	})

	t.Run("with gap: duration still spans first start to last end, hasGaps true", func(t *testing.T) {
		metas := []cache.SegmentMeta{
			seg(0, base, 5*time.Second),
			seg(1, base.Add(20*time.Second), 5*time.Second),
		}
		durationSecs, hasGaps := recordingSummary(metas)
		if !hasGaps {
			t.Error("expected hasGaps=true when a >2s gap exists")
		}
		if durationSecs != 25 {
			t.Errorf("got durationSecs=%d, want 25 (last.EndedAt - first.StartedAt)", durationSecs)
		}
	})
}

func TestRecordingStatus(t *testing.T) {
	if got := recordingStatus(true); got != "PARTIAL" {
		t.Errorf("got %q, want PARTIAL when hasGaps=true", got)
	}
	if got := recordingStatus(false); got != "READY" {
		t.Errorf("got %q, want READY when hasGaps=false", got)
	}
}

func TestRecordingRequestFromStreamEnded(t *testing.T) {
	endedAt := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	event := domain.StreamEndedEvent{
		EventID:       "event-1",
		ScheduleID:    "schedule-1",
		SessionID:     "session-1",
		ParticipantID: "participant-1",
		StreamID:      "stream-1",
		StreamType:    "camera",
		EndedAt:       endedAt,
	}

	got := recordingRequestFromStreamEnded(event)

	if got.EventID != event.EventID || got.StreamID != event.StreamID || got.ScheduleID != event.ScheduleID ||
		got.SessionID != event.SessionID || got.ParticipantID != event.ParticipantID || got.StreamType != event.StreamType {
		t.Errorf("got %+v, want fields copied from %+v", got, event)
	}
	if got.Source != "SERVER_WEBRTC" {
		t.Errorf("got Source=%q, want SERVER_WEBRTC (distinguishes this trigger from DESKTOP_SEGMENT_UPLOAD)", got.Source)
	}
	if !got.RequestedAt.Equal(endedAt) {
		t.Errorf("got RequestedAt=%v, want it to match event.EndedAt=%v", got.RequestedAt, endedAt)
	}
}

func TestWriteConcatList(t *testing.T) {
	dir := t.TempDir()

	t.Run("writes basenames only, one per line", func(t *testing.T) {
		files := []string{
			filepath.Join(dir, "0001.mp4"),
			filepath.Join(dir, "0002.mp4"),
		}
		listPath := filepath.Join(dir, "concat_list.txt")
		if err := writeConcatList(listPath, files); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		data, err := os.ReadFile(listPath)
		if err != nil {
			t.Fatalf("read concat list: %v", err)
		}
		want := "file '0001.mp4'\nfile '0002.mp4'\n"
		if string(data) != want {
			t.Errorf("got %q, want %q", string(data), want)
		}
	})

	t.Run("escapes single quotes in file names", func(t *testing.T) {
		files := []string{filepath.Join(dir, "it's-a-segment.mp4")}
		listPath := filepath.Join(dir, "concat_list2.txt")
		if err := writeConcatList(listPath, files); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		data, err := os.ReadFile(listPath)
		if err != nil {
			t.Fatalf("read concat list: %v", err)
		}
		if !strings.Contains(string(data), `it'\''s-a-segment.mp4`) {
			t.Errorf("got %q, want single quote escaped as '\\''", string(data))
		}
	})
}
