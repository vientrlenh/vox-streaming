package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deliberately as long as segmentTTL: an entry has to outlive the upload session it describes (that
// key is deleted by Redis the moment the session expires, which is precisely when the watchdog
// wants to act on it), but there is no point keeping it once the segment metadata it would assemble
// from is gone too.
const pendingAssemblyTTL = segmentTTL

// PendingAssembly is a stream that still owes a recording: opened for segment upload and not yet
// finished by the client calling /complete, or a WebRTC stream whose stream.ended has not yet been
// turned into a recording. It carries its own copy of the identifiers the assembler needs, because
// by the time it comes due neither the upload session nor the peer exists to look them up from.
type PendingAssembly struct {
	StreamID      string    `json:"streamId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamType    string    `json:"streamType"`
	DueAt         time.Time `json:"dueAt"`
	Attempts      int       `json:"attempts"`
	// Which pipeline owes this recording, so the watchdog can label what it produces and log it
	// honestly. A WebRTC stream reaching its grace period is the NORMAL end of that path; an upload
	// session expiring with no /complete is a salvage after the exam machine went silent. Reporting
	// both as the latter would train operators to ignore the alarming one.
	//
	// Empty on entries written before this field existed, and those are all from the upload path --
	// see AssemblySource.
	Source string `json:"source,omitempty"`
}

// AssemblySource is the Source to act on, defaulting entries that predate the field.
func (p PendingAssembly) AssemblySource() string {
	if p.Source == "" {
		return AssemblySourceWatchdog
	}
	return p.Source
}

const (
	// Salvaged after the client went silent: nobody vouched for this recording being whole.
	AssemblySourceWatchdog = "SERVER_WATCHDOG"
	// The ordinary end of a WebRTC stream, assembled once its grace period elapsed. Matches the
	// Source that AssemblerUseCase.OnStreamEnded uses on the immediate path, so downstream cannot
	// tell whether the grace period was waited out in-process or in Redis -- which is the point.
	AssemblySourceWebRTC = "SERVER_WEBRTC"
)

// PendingAssemblyRegistry is the durable side of the assembly watchdog: a due-time-ordered set of
// streams that still owe a recording, so a recording gets assembled even when the client never
// says it is finished -- a crashed, shut down, force-closed or permanently offline exam machine.
//
// A sorted set rather than a timer: an in-process time.AfterFunc is lost on every deploy and
// restart, and losing it silently strands every recording that was mid-flight at that moment.
type PendingAssemblyRegistry struct {
	client *redis.Client
}

func NewPendingAssemblyRegistry(client *redis.Client) *PendingAssemblyRegistry {
	return &PendingAssemblyRegistry{client: client}
}

// Schedule registers (or reschedules) a stream to be assembled once it is due. Calling it again for
// the same stream moves the due time, which is what makes a resumed upload session -- one whose
// credential was refreshed and expiry pushed out -- extend the watchdog rather than race it.
func (r *PendingAssemblyRegistry) Schedule(ctx context.Context, pending PendingAssembly) error {
	data, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("pending assembly marshal: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.ZAdd(ctx, pendingAssemblyDueKey(), redis.Z{
		Score:  float64(pending.DueAt.UTC().Unix()),
		Member: pending.StreamID,
	})
	pipe.Expire(ctx, pendingAssemblyDueKey(), pendingAssemblyTTL)
	pipe.Set(ctx, pendingAssemblyKey(pending.StreamID), string(data), pendingAssemblyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule pending assembly: %w", err)
	}
	return nil
}

// Due returns the streams whose due time has passed, oldest first. Entries whose detail key has
// expired out from under the set are cleaned up here rather than returned: without the identifiers
// there is nothing to assemble, and their segments are gone on the same TTL anyway.
func (r *PendingAssemblyRegistry) Due(ctx context.Context, now time.Time, limit int64) ([]PendingAssembly, error) {
	members, err := r.client.ZRangeByScore(ctx, pendingAssemblyDueKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", now.UTC().Unix()),
		Count: limit,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list due assemblies: %w", err)
	}

	pendings := make([]PendingAssembly, 0, len(members))
	for _, streamID := range members {
		raw, err := r.client.Get(ctx, pendingAssemblyKey(streamID)).Bytes()
		if errors.Is(err, redis.Nil) {
			_ = r.client.ZRem(ctx, pendingAssemblyDueKey(), streamID).Err()
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("load pending assembly %s: %w", streamID, err)
		}

		var pending PendingAssembly
		if err := json.Unmarshal(raw, &pending); err != nil {
			// Undecodable is unrecoverable, not transient: drop it rather than wedge every
			// later sweep behind the same bad entry.
			_ = r.Cancel(ctx, streamID)
			continue
		}
		pendings = append(pendings, pending)
	}
	return pendings, nil
}

// Cancel removes a stream from the watchdog. Called when the client finishes normally, and when the
// watchdog has taken the stream as far as it can.
func (r *PendingAssemblyRegistry) Cancel(ctx context.Context, streamID string) error {
	pipe := r.client.Pipeline()
	pipe.ZRem(ctx, pendingAssemblyDueKey(), streamID)
	pipe.Del(ctx, pendingAssemblyKey(streamID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cancel pending assembly: %w", err)
	}
	return nil
}

func pendingAssemblyDueKey() string {
	return "assembly-pending:due"
}

func pendingAssemblyKey(streamID string) string {
	return fmt.Sprintf("assembly-pending:%s", streamID)
}
