package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newScheduleStreamRegistry(t *testing.T) *ScheduleStreamRegistry {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewScheduleStreamRegistry(client)
}

func sampleStream(streamID string, startedAt time.Time) ScheduleStream {
	return ScheduleStream{
		StreamID:      streamID,
		ScheduleID:    "schedule-1",
		SessionID:     "session-1",
		ParticipantID: "candidate-1",
		StreamType:    "camera",
		StartedAt:     startedAt,
	}
}

// The reason this index exists: a finished stream has to stay findable, because the live session
// registry forgets it the moment the peer closes while its footage is still retained and playable.
func TestScheduleStreamRegistry_KeepsEndedStreams(t *testing.T) {
	registry := newScheduleStreamRegistry(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 17, 14, 0, 0, 0, time.UTC)

	if err := registry.Record(ctx, sampleStream("stream-1", base)); err != nil {
		t.Fatalf("Record: %v", err)
	}
	endedAt := base.Add(3 * time.Minute)
	if err := registry.MarkEnded(ctx, "schedule-1", "stream-1", endedAt); err != nil {
		t.Fatalf("MarkEnded: %v", err)
	}

	streams, err := registry.List(ctx, "schedule-1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(streams) != 1 {
		t.Fatalf("got %d streams, want 1", len(streams))
	}
	got := streams[0]
	if got.EndedAt == nil || !got.EndedAt.Equal(endedAt) {
		t.Fatalf("got EndedAt=%v, want %v", got.EndedAt, endedAt)
	}
	// The identifiers must survive the end stamp: they are the only way back to the footage, and the
	// close path no longer has them to supply.
	if got.SessionID != "session-1" || got.ParticipantID != "candidate-1" || got.StreamType != "camera" {
		t.Fatalf("MarkEnded lost identifiers: %+v", got)
	}
}

// A peer can be closed more than once — replaced on reconnect, then the websocket handler's own
// defer — and the second call must not push the recorded end time past when the stream really
// stopped.
func TestScheduleStreamRegistry_FirstEndWins(t *testing.T) {
	registry := newScheduleStreamRegistry(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 17, 14, 0, 0, 0, time.UTC)

	if err := registry.Record(ctx, sampleStream("stream-1", base)); err != nil {
		t.Fatalf("Record: %v", err)
	}
	first := base.Add(time.Minute)
	if err := registry.MarkEnded(ctx, "schedule-1", "stream-1", first); err != nil {
		t.Fatalf("MarkEnded: %v", err)
	}
	if err := registry.MarkEnded(ctx, "schedule-1", "stream-1", base.Add(10*time.Minute)); err != nil {
		t.Fatalf("second MarkEnded: %v", err)
	}

	streams, _ := registry.List(ctx, "schedule-1")
	if len(streams) != 1 || streams[0].EndedAt == nil || !streams[0].EndedAt.Equal(first) {
		t.Fatalf("got %+v, want EndedAt to stay %v", streams, first)
	}
}

// Ending a stream the index has already expired out from under is ordinary, not an error: the entry
// lives on a fixed retention while a peer can outlive it.
func TestScheduleStreamRegistry_MarkEndedOnMissingEntryIsNotAnError(t *testing.T) {
	registry := newScheduleStreamRegistry(t)

	if err := registry.MarkEnded(context.Background(), "schedule-1", "ghost", time.Now().UTC()); err != nil {
		t.Fatalf("MarkEnded on a missing entry should be a no-op, got: %v", err)
	}
}

func TestScheduleStreamRegistry_ListsOldestFirst(t *testing.T) {
	registry := newScheduleStreamRegistry(t)
	ctx := context.Background()
	base := time.Date(2026, 8, 17, 14, 0, 0, 0, time.UTC)

	if err := registry.Record(ctx, sampleStream("stream-late", base.Add(time.Minute))); err != nil {
		t.Fatalf("Record: %v", err)
	}
	if err := registry.Record(ctx, sampleStream("stream-early", base)); err != nil {
		t.Fatalf("Record: %v", err)
	}

	streams, err := registry.List(ctx, "schedule-1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(streams) != 2 || streams[0].StreamID != "stream-early" {
		t.Fatalf("got %+v, want oldest first", streams)
	}
}
