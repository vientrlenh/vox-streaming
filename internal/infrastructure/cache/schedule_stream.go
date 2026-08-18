package cache

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deliberately the same as hlsFragmentTTL, and that is a constraint rather than a coincidence: this
// index exists only to point at fragments, so an entry that outlives them would advertise a stream
// whose playlist then 404s. Whatever changes one must change the other.
const scheduleStreamTTL = hlsFragmentTTL

// ScheduleStream is one stream that existed in a schedule, alive or finished.
//
// The live session registry only knows about streams that are still connected, which is the right
// answer for "who is on air" and the wrong one for "what happened in this room". A proctor who
// reloads the page mid-exam would otherwise lose every student who had already dropped -- including
// the ones worth looking at -- even though their footage is still retained and playable.
type ScheduleStream struct {
	StreamID      string     `json:"streamId"`
	ScheduleID    string     `json:"scheduleId"`
	SessionID     string     `json:"sessionId"`
	ParticipantID string     `json:"participantId"`
	StreamType    string     `json:"streamType"`
	StartedAt     time.Time  `json:"startedAt"`
	EndedAt       *time.Time `json:"endedAt,omitempty"`
}

type ScheduleStreamRegistry struct {
	client *redis.Client
}

func NewScheduleStreamRegistry(client *redis.Client) *ScheduleStreamRegistry {
	return &ScheduleStreamRegistry{client: client}
}

// Record registers a stream under its schedule. Called when a peer is accepted, so the entry exists
// before the first byte of media does -- a stream that dies early is exactly the one a proctor most
// wants to find afterwards.
func (r *ScheduleStreamRegistry) Record(ctx context.Context, stream ScheduleStream) error {
	data, err := json.Marshal(stream)
	if err != nil {
		return fmt.Errorf("marshal schedule stream: %w", err)
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, scheduleStreamKey(stream.ScheduleID), stream.StreamID, string(data))
	pipe.Expire(ctx, scheduleStreamKey(stream.ScheduleID), scheduleStreamTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("record schedule stream: %w", err)
	}
	return nil
}

// MarkEnded stamps the moment a stream stopped.
//
// Reads-then-writes rather than overwriting: the caller at close time no longer has the identifiers
// the entry was created with, and rebuilding them from scratch would lose whichever the close path
// happens not to carry. A missing entry is not an error -- the index may simply have expired under a
// very long-running peer -- so this returns nil rather than resurrecting a half-populated record.
func (r *ScheduleStreamRegistry) MarkEnded(ctx context.Context, scheduleID, streamID string, at time.Time) error {
	raw, err := r.client.HGet(ctx, scheduleStreamKey(scheduleID), streamID).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load schedule stream: %w", err)
	}

	var stream ScheduleStream
	if err := json.Unmarshal(raw, &stream); err != nil {
		return fmt.Errorf("unmarshal schedule stream: %w", err)
	}
	// First end wins. A peer can be closed more than once (explicit replace on reconnect, then the
	// websocket handler's own defer), and the later call would otherwise push the recorded end time
	// forward past when the stream actually stopped.
	if stream.EndedAt != nil {
		return nil
	}
	endedAt := at.UTC()
	stream.EndedAt = &endedAt

	return r.Record(ctx, stream)
}

// List returns every stream recorded for a schedule, oldest first. Entries that cannot be decoded
// are skipped rather than failing the call: one bad record must not hide every other student.
func (r *ScheduleStreamRegistry) List(ctx context.Context, scheduleID string) ([]ScheduleStream, error) {
	result, err := r.client.HGetAll(ctx, scheduleStreamKey(scheduleID)).Result()
	if err != nil {
		return nil, fmt.Errorf("list schedule streams: %w", err)
	}

	streams := make([]ScheduleStream, 0, len(result))
	for _, value := range result {
		var stream ScheduleStream
		if err := json.Unmarshal([]byte(value), &stream); err != nil {
			continue
		}
		streams = append(streams, stream)
	}
	slices.SortFunc(streams, func(a, b ScheduleStream) int {
		return cmp.Or(a.StartedAt.Compare(b.StartedAt), cmp.Compare(a.StreamID, b.StreamID))
	})
	return streams, nil
}

func scheduleStreamKey(scheduleID string) string {
	return fmt.Sprintf("schedule:%s:streams", scheduleID)
}
