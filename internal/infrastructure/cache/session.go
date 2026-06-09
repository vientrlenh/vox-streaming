package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const sessionTTL = 5 * time.Minute

type SessionInfo struct {
	InstanceID 	string `json:"instanceId"`
	RoomID 		string 	`json:"roomId"`
	StreamID 	string `json:"streamId"`
	ParticipantID string `json:"participantId"`
	StreamType 	string 	`json:"streamType"`
	StartedAt time.Time `json:"startedAt"`
}

type SessionRegistry struct {
	client *redis.Client
	instanceID string
}

func NewSessionRegistry(client *redis.Client) *SessionRegistry {
	instanceID, _ := os.Hostname()
	return &SessionRegistry{
		client: client, 
		instanceID: instanceID,
	}
}

func (r *SessionRegistry) Register(ctx context.Context, roomID, participantID, streamType, streamID string, startedAt time.Time) error {
	val, err := json.Marshal(SessionInfo{
		InstanceID: r.instanceID, 
		RoomID: roomID,
		StreamID: streamID,
		ParticipantID: participantID,
		StreamType: streamType,
		StartedAt: startedAt,
	})

	if err != nil {
		return fmt.Errorf("streaming session registry marshal: %w", err)
	}
	return r.client.Set(ctx, sessionKey(roomID, participantID, streamType), val, sessionTTL).Err()
}

func (r *SessionRegistry) Unregister(ctx context.Context, roomID, participantID, streamType string) error {
	return r.client.Del(ctx, sessionKey(roomID, participantID, streamType)).Err()
}

func (r *SessionRegistry) Lookup(ctx context.Context, roomID, participantID, streamType string) (*SessionInfo, error) {
	val, err := r.client.Get(ctx, sessionKey(roomID, participantID, streamType)).Bytes()
	if err != nil {
		return nil, fmt.Errorf("streaming session registry lookup: %w", err)
	}
	var info SessionInfo 
	if err := json.Unmarshal(val, &info); err != nil {
		return nil, fmt.Errorf("streaming session registry unmarshal: %w", err)
	}
	return &info, nil
}

func (r *SessionRegistry) ScanRoom(ctx context.Context, roomID string) ([]SessionInfo, error) {
	pattern := fmt.Sprintf("streaming-session:%s:*", roomID)
	return r.scanByPattern(ctx, pattern)
}

func (r *SessionRegistry) ScanAll(ctx context.Context) ([]SessionInfo, error) {
	return r.scanByPattern(ctx, "streaming-session:*")
}

func sessionKey(roomID, participantID, streamType string) string {
	return fmt.Sprintf("streaming-session:%s:%s:%s", roomID, participantID, streamType)
}

func (r *SessionRegistry) scanByPattern(ctx context.Context, pattern string) ([]SessionInfo, error) {
	var keys []string
	iter := r.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	} 
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("streaming session registry scan: %w", err)
	}
	if len(keys) == 0 {
		return nil, nil
	}

	vals, err := r.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("streaming session registry mget: %w", err)
	}

	infos := make([]SessionInfo, 0, len(vals))
	for _, v := range vals {
		if v == nil {
			continue // key expired giữa chừng
		}
		var info SessionInfo
		if err := json.Unmarshal([]byte(v.(string)), &info); err != nil {
			continue
		}
		infos = append(infos, info)
	}
	return infos, nil
}


func (r *SessionRegistry) Refresh(ctx context.Context, roomID, participantID, streamType string) error {
	return r.client.Expire(ctx, sessionKey(roomID, participantID, streamType), sessionTTL).Err()
}