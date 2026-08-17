package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"go.uber.org/zap"
)

// storage and publisher stay nil: the incomplete branch under test reaches neither, and standing up
// S3 and Kafka to observe a Redis write would only make the test harder to trust.
func newStreamEndedFixture(t *testing.T) (*AssemblerUseCase, *cache.PendingAssemblyRegistry, *cache.SegmentRegistry) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	segments := cache.NewSegmentRegistry(client)
	pending := cache.NewPendingAssemblyRegistry(client)
	uc := NewAssemblerUseCase(
		nil,
		segments,
		cache.NewInventoryRegistry(client),
		cache.NewSessionRegistry(client),
		pending,
		nil,
		90*time.Second,
		zap.NewNop(),
	)
	return uc, pending, segments
}

func streamEndedEvent() domain.StreamEndedEvent {
	return domain.StreamEndedEvent{
		StreamID:      "stream-1",
		ScheduleID:    "schedule-1",
		SessionID:     "session-1",
		ParticipantID: "candidate-1",
		StreamType:    "camera",
	}
}

// The grace period has to be served out in Redis rather than in a time.AfterFunc.
//
// A timer is discarded by every deploy, restart and crash, silently stranding whichever recordings
// were mid-grace at that moment, and it never gets created at all if the consumer that would have
// armed it is dead. On 2026-08-17 the vox-assembler consumer had exited hours earlier; four streams
// published stream.ended into a topic with no reader and no recording was ever assembled from them.
func TestOnStreamEnded_ParksIncompleteStreamInTheDurableDueSet(t *testing.T) {
	uc, pending, _ := newStreamEndedFixture(t)

	if err := uc.OnStreamEnded(context.Background(), streamEndedEvent()); err != nil {
		t.Fatalf("OnStreamEnded: %v", err)
	}

	due, err := pending.Due(context.Background(), time.Now().UTC().Add(24*time.Hour), 10)
	if err != nil {
		t.Fatalf("Due: %v", err)
	}
	if len(due) != 1 {
		t.Fatalf("got %d pending entries, want 1", len(due))
	}

	got := due[0]
	if got.StreamID != "stream-1" || got.ScheduleID != "schedule-1" ||
		got.SessionID != "session-1" || got.ParticipantID != "candidate-1" || got.StreamType != "camera" {
		t.Fatalf("pending entry carries the wrong identifiers: %+v", got)
	}
	// Labelled as the ordinary end of the WebRTC path, not as a salvage: reporting every watchdog
	// assembly as a rescue is how operators learn to ignore the ones that really are.
	if got.Source != cache.AssemblySourceWebRTC {
		t.Fatalf("got Source=%q, want %q", got.Source, cache.AssemblySourceWebRTC)
	}
	// Due after the grace period, never immediately: assembly short-circuits on an existing
	// recording.mp4, so assembling while segments are still arriving permanently truncates it.
	if !got.DueAt.After(time.Now().UTC().Add(30 * time.Second)) {
		t.Fatalf("got DueAt=%v, want at least most of the grace period out", got.DueAt)
	}
}

// Entries written before PendingAssembly gained a Source are all from the upload path, so they must
// keep reading as salvage rather than silently becoming the empty string downstream.
func TestPendingAssembly_SourceDefaultsToWatchdogForOlderEntries(t *testing.T) {
	if got := (cache.PendingAssembly{}).AssemblySource(); got != cache.AssemblySourceWatchdog {
		t.Fatalf("got %q, want %q", got, cache.AssemblySourceWatchdog)
	}
	explicit := cache.PendingAssembly{Source: cache.AssemblySourceWebRTC}
	if got := explicit.AssemblySource(); got != cache.AssemblySourceWebRTC {
		t.Fatalf("got %q, want %q", got, cache.AssemblySourceWebRTC)
	}
}
