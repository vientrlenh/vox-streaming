package watcher

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"go.uber.org/zap"
)

type fakeAssembler struct {
	calls []domain.RecordingAssemblyRequestedEvent
	err   error
}

func (f *fakeAssembler) AssembleRequested(_ context.Context, event domain.RecordingAssemblyRequestedEvent) error {
	f.calls = append(f.calls, event)
	return f.err
}

type watchdogFixture struct {
	watchdog  *AssemblyWatchdog
	pending   *cache.PendingAssemblyRegistry
	segments  *cache.SegmentRegistry
	assembler *fakeAssembler
}

func newWatchdogFixture(t *testing.T) *watchdogFixture {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	pending := cache.NewPendingAssemblyRegistry(client)
	segments := cache.NewSegmentRegistry(client)
	assembler := &fakeAssembler{}

	return &watchdogFixture{
		watchdog:  NewAssemblyWatchdog(pending, segments, assembler, time.Minute, zap.NewNop()),
		pending:   pending,
		segments:  segments,
		assembler: assembler,
	}
}

func (f *watchdogFixture) schedule(t *testing.T, streamID string, dueAt time.Time) {
	t.Helper()
	if err := f.pending.Schedule(context.Background(), cache.PendingAssembly{
		StreamID:      streamID,
		ScheduleID:    "schedule-1",
		SessionID:     "session-1",
		ParticipantID: "candidate-1",
		StreamType:    "camera",
		DueAt:         dueAt,
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
}

func (f *watchdogFixture) addSegment(t *testing.T, streamID string, seq int64) {
	t.Helper()
	if err := f.segments.Add(context.Background(), streamID, cache.SegmentMeta{
		Seq:       seq,
		S3Key:     "schedules/schedule-1/sessions/session-1/streams/" + streamID + "/segments/0000.mp4",
		StartedAt: time.Now().UTC(),
		EndedAt:   time.Now().UTC().Add(10 * time.Second),
	}); err != nil {
		t.Fatalf("segments.Add: %v", err)
	}
}

func (f *watchdogFixture) stillPending(t *testing.T) []cache.PendingAssembly {
	t.Helper()
	due, err := f.pending.Due(context.Background(), time.Now().UTC().Add(24*time.Hour), 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	return due
}

// The reason the watchdog exists: a client that crashed, shut down or went offline for good never
// calls /complete, and its segments would otherwise sit in S3 forever with nothing to assemble them.
func TestWatchdog_AssemblesStreamTheClientNeverCompleted(t *testing.T) {
	f := newWatchdogFixture(t)
	f.addSegment(t, "stream-1", 0)
	f.addSegment(t, "stream-1", 1)
	f.schedule(t, "stream-1", time.Now().UTC().Add(-time.Minute))

	f.watchdog.SweepOnce(context.Background())

	if len(f.assembler.calls) != 1 {
		t.Fatalf("got %d assemble calls, want 1", len(f.assembler.calls))
	}
	got := f.assembler.calls[0]
	if got.StreamID != "stream-1" || got.ScheduleID != "schedule-1" ||
		got.SessionID != "session-1" || got.ParticipantID != "candidate-1" {
		t.Fatalf("assembler got the wrong identifiers: %+v", got)
	}
	// Downstream has to be able to tell a recording the client vouched for from one salvaged after
	// the client went silent.
	if got.Source != "SERVER_WATCHDOG" {
		t.Fatalf("got Source=%q, want SERVER_WATCHDOG", got.Source)
	}
	if len(f.stillPending(t)) != 0 {
		t.Fatal("a successfully assembled stream should no longer be pending")
	}
}

// Firing early is the one thing the watchdog must never do: assembly short-circuits on an existing
// recording.mp4, so an early run permanently prevents the complete one.
func TestWatchdog_LeavesStreamsThatAreNotDueYet(t *testing.T) {
	f := newWatchdogFixture(t)
	f.addSegment(t, "stream-1", 0)
	f.schedule(t, "stream-1", time.Now().UTC().Add(30*time.Minute))

	f.watchdog.SweepOnce(context.Background())

	if len(f.assembler.calls) != 0 {
		t.Fatalf("got %d assemble calls, want 0 before the stream is due", len(f.assembler.calls))
	}
	if len(f.stillPending(t)) != 1 {
		t.Fatal("a stream that is not due yet must stay pending")
	}
}

// A camera that never opened produces no segments. Running the assembler on it would only fail and
// publish a FAILED recording state for a recording that never started.
func TestWatchdog_DropsStreamWithNoSegments(t *testing.T) {
	f := newWatchdogFixture(t)
	f.schedule(t, "stream-1", time.Now().UTC().Add(-time.Minute))

	f.watchdog.SweepOnce(context.Background())

	if len(f.assembler.calls) != 0 {
		t.Fatalf("got %d assemble calls, want 0 for a stream with no segments", len(f.assembler.calls))
	}
	if len(f.stillPending(t)) != 0 {
		t.Fatal("a stream with nothing to assemble should be dropped, not retried forever")
	}
}

func TestWatchdog_RetriesFailureWithBackoffThenGivesUp(t *testing.T) {
	f := newWatchdogFixture(t)
	f.assembler.err = errors.New("s3 unavailable")
	f.addSegment(t, "stream-1", 0)
	f.schedule(t, "stream-1", time.Now().UTC().Add(-time.Minute))

	f.watchdog.SweepOnce(context.Background())

	if len(f.assembler.calls) != 1 {
		t.Fatalf("got %d assemble calls, want 1", len(f.assembler.calls))
	}
	remaining := f.stillPending(t)
	if len(remaining) != 1 {
		t.Fatal("a failed assembly must stay pending so it can be retried")
	}
	if remaining[0].Attempts != 1 {
		t.Fatalf("got Attempts=%d, want 1", remaining[0].Attempts)
	}
	// Backed off rather than retried on the very next sweep: the plausible failures here (S3, disk,
	// ffmpeg) do not recover in seconds.
	if !remaining[0].DueAt.After(time.Now().UTC()) {
		t.Fatalf("got DueAt=%v, want a future due time", remaining[0].DueAt)
	}

	// Drive it to the give-up threshold. Each sweep needs the entry due again, but rescheduling it
	// must carry the accumulated Attempts forward -- writing a fresh entry would reset the counter
	// and the loop would never terminate.
	for attempt := 2; attempt <= watchdogMaxAttempts; attempt++ {
		pendings := f.stillPending(t)
		if len(pendings) == 0 {
			t.Fatalf("entry vanished before attempt %d", attempt)
		}
		carried := pendings[0]
		carried.DueAt = time.Now().UTC().Add(-time.Minute)
		if err := f.pending.Schedule(context.Background(), carried); err != nil {
			t.Fatalf("reschedule for attempt %d: %v", attempt, err)
		}
		f.watchdog.SweepOnce(context.Background())
	}

	if len(f.assembler.calls) != watchdogMaxAttempts {
		t.Fatalf("got %d assemble calls, want %d", len(f.assembler.calls), watchdogMaxAttempts)
	}

	if len(f.stillPending(t)) != 0 {
		t.Fatal("the watchdog should give up after repeated failures instead of retrying forever")
	}
}
