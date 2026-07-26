package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRegistry(t *testing.T) (*PendingAssemblyRegistry, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewPendingAssemblyRegistry(client), server
}

func samplePending(streamID string, dueAt time.Time) PendingAssembly {
	return PendingAssembly{
		StreamID:      streamID,
		ScheduleID:    "schedule-1",
		SessionID:     "session-1",
		ParticipantID: "candidate-1",
		StreamType:    "camera",
		DueAt:         dueAt,
	}
}

// The whole point of the watchdog is that it does not fire early: assembly is effectively one-shot,
// so a premature run permanently prevents the complete recording from ever being built.
func TestPendingAssembly_NotReturnedBeforeDue(t *testing.T) {
	registry, _ := newTestRegistry(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := registry.Schedule(ctx, samplePending("stream-1", now.Add(30*time.Minute))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	due, err := registry.Due(ctx, now, 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("got %d due entries, want 0 before the due time", len(due))
	}
}

func TestPendingAssembly_ReturnedOnceDueWithIdentifiers(t *testing.T) {
	registry, _ := newTestRegistry(t)
	ctx := context.Background()
	now := time.Now().UTC()

	want := samplePending("stream-1", now.Add(-time.Minute))
	if err := registry.Schedule(ctx, want); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	due, err := registry.Due(ctx, now, 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("got %d due entries, want 1", len(due))
	}

	// The identifiers must survive independently of the upload session, which Redis deletes exactly
	// when this becomes due -- carrying them here is the reason the detail key exists at all.
	got := due[0]
	if got.StreamID != want.StreamID || got.ScheduleID != want.ScheduleID ||
		got.SessionID != want.SessionID || got.ParticipantID != want.ParticipantID ||
		got.StreamType != want.StreamType {
		t.Fatalf("identifiers did not round-trip: got %+v, want %+v", got, want)
	}
}

// A resumed upload session pushes ExpiresAt out; the watchdog has to follow it rather than fire at
// the original time and assemble a stream that is still legitimately growing.
func TestPendingAssembly_RescheduleMovesDueTimeWithoutDuplicating(t *testing.T) {
	registry, _ := newTestRegistry(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := registry.Schedule(ctx, samplePending("stream-1", now.Add(-time.Minute))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := registry.Schedule(ctx, samplePending("stream-1", now.Add(30*time.Minute))); err != nil {
		t.Fatalf("Schedule (reschedule): %v", err)
	}

	due, err := registry.Due(ctx, now, 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("got %d due entries, want 0 after the due time was pushed out", len(due))
	}

	due, err = registry.Due(ctx, now.Add(time.Hour), 10)
	if err != nil {
		t.Fatalf("Due (later): %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("got %d due entries at the new due time, want exactly 1 (not a duplicate)", len(due))
	}
}

func TestPendingAssembly_CancelRemoves(t *testing.T) {
	registry, _ := newTestRegistry(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := registry.Schedule(ctx, samplePending("stream-1", now.Add(-time.Minute))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := registry.Cancel(ctx, "stream-1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	due, err := registry.Due(ctx, now, 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("got %d due entries after Cancel, want 0", len(due))
	}
}

// The set and the detail keys expire independently, so a member can outlive its own detail. That
// must not wedge the sweep or surface an entry with no identifiers.
func TestPendingAssembly_ExpiredDetailIsSkippedAndCleanedUp(t *testing.T) {
	registry, server := newTestRegistry(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := registry.Schedule(ctx, samplePending("stream-1", now.Add(-time.Minute))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	server.Del(pendingAssemblyKey("stream-1"))

	due, err := registry.Due(ctx, now, 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 0 {
		t.Fatalf("got %d due entries, want 0 when the detail is gone", len(due))
	}

	if server.Exists(pendingAssemblyDueKey()) {
		members, _ := server.ZMembers(pendingAssemblyDueKey())
		if len(members) != 0 {
			t.Fatalf("orphaned member left in the due set: %v", members)
		}
	}
}

func TestPendingAssembly_DueIsOrderedOldestFirst(t *testing.T) {
	registry, _ := newTestRegistry(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := registry.Schedule(ctx, samplePending("newer", now.Add(-time.Minute))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	if err := registry.Schedule(ctx, samplePending("older", now.Add(-time.Hour))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	due, err := registry.Due(ctx, now, 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 2 {
		t.Fatalf("got %d due entries, want 2", len(due))
	}
	if due[0].StreamID != "older" {
		t.Fatalf("got %q first, want the oldest due stream first", due[0].StreamID)
	}
}
