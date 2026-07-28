package cache

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestSessionRegistry(t *testing.T) *SessionRegistry {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewSessionRegistry(client)
}

func registerTestUpload(t *testing.T, registry *SessionRegistry, streamID string) {
	t.Helper()
	now := time.Now().UTC()
	_, _, err := registry.RegisterOrGetUpload(context.Background(), UploadSession{
		StreamID:        streamID,
		CandidateID:     "candidate-1",
		SessionID:       "session-1",
		ScheduleID:      "schedule-1",
		StreamType:      "camera",
		CreatedAt:       now,
		ExpiresAt:       now.Add(time.Hour),
		UploadTokenHash: "hash",
	})
	if err != nil {
		t.Fatalf("register upload session: %v", err)
	}
}

func TestMarkUploadCompleteRecordsStopReason(t *testing.T) {
	registry := newTestSessionRegistry(t)
	ctx := context.Background()
	registerTestUpload(t, registry, "stream-1")

	newly, err := registry.MarkUploadComplete(ctx, "stream-1", "Submitted")
	if err != nil {
		t.Fatalf("mark complete: %v", err)
	}
	if !newly {
		t.Error("the first completion must report itself as new")
	}

	session, err := registry.LookupUpload(ctx, "stream-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if session.StopReason != "Submitted" || !session.Completed {
		t.Errorf("got %+v, want a completed session stopped by Submitted", session)
	}
}

// The reason a run observed beats the one a later run infers. Startup recovery re-completes a
// stream whose original run died before recording its own success locally, and it can only report
// RecoveredAfterCrash -- true of that run, but not of the recording. Overwriting would replace an
// observed reason with a salvage marker, in the one case where the real reason still explains the
// recording.
func TestMarkUploadCompleteKeepsTheFirstStopReason(t *testing.T) {
	registry := newTestSessionRegistry(t)
	ctx := context.Background()
	registerTestUpload(t, registry, "stream-1")

	if _, err := registry.MarkUploadComplete(ctx, "stream-1", "Submitted"); err != nil {
		t.Fatalf("first complete: %v", err)
	}

	newly, err := registry.MarkUploadComplete(ctx, "stream-1", "RecoveredAfterCrash")
	if err != nil {
		t.Fatalf("second complete: %v", err)
	}
	if newly {
		t.Error("a repeat completion must not report itself as new")
	}

	session, err := registry.LookupUpload(ctx, "stream-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if session.StopReason != "Submitted" {
		t.Errorf("StopReason = %q, want the first reason to survive", session.StopReason)
	}
}

// The opposite direction still has to work: a stream completed without a reason -- an older client,
// or a client that sent no body -- is exactly where a later caller's reason is worth taking.
func TestMarkUploadCompleteFillsAnAbsentStopReason(t *testing.T) {
	registry := newTestSessionRegistry(t)
	ctx := context.Background()
	registerTestUpload(t, registry, "stream-1")

	if _, err := registry.MarkUploadComplete(ctx, "stream-1", ""); err != nil {
		t.Fatalf("first complete: %v", err)
	}
	if _, err := registry.MarkUploadComplete(ctx, "stream-1", "RecoveredAfterCrash"); err != nil {
		t.Fatalf("second complete: %v", err)
	}

	session, err := registry.LookupUpload(ctx, "stream-1")
	if err != nil {
		t.Fatalf("lookup: %v", err)
	}
	if session.StopReason != "RecoveredAfterCrash" {
		t.Errorf("StopReason = %q, want the later reason to fill the gap", session.StopReason)
	}
}

// Completion must preserve the session's existing expiry: the assembly watchdog is armed off
// ExpiresAt, so resetting the TTL here would move when a recording gets finalized.
func TestMarkUploadCompletePreservesExpiry(t *testing.T) {
	registry := newTestSessionRegistry(t)
	ctx := context.Background()
	registerTestUpload(t, registry, "stream-1")

	before, err := registry.client.TTL(ctx, uploadSessionKey("stream-1")).Result()
	if err != nil {
		t.Fatalf("read ttl: %v", err)
	}

	if _, err := registry.MarkUploadComplete(ctx, "stream-1", "Submitted"); err != nil {
		t.Fatalf("mark complete: %v", err)
	}

	after, err := registry.client.TTL(ctx, uploadSessionKey("stream-1")).Result()
	if err != nil {
		t.Fatalf("read ttl: %v", err)
	}
	if after > before {
		t.Errorf("ttl grew from %v to %v; completion must keep the existing expiry", before, after)
	}
}

func TestMarkUploadCompleteReportsAMissingSession(t *testing.T) {
	registry := newTestSessionRegistry(t)

	_, err := registry.MarkUploadComplete(context.Background(), "never-registered", "Submitted")
	if err == nil {
		t.Fatal("completing an unknown stream must be an error, not a silent success")
	}
}
