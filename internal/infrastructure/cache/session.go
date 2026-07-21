package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const sessionTTL = 5 * time.Minute

type SessionInfo struct {
	InstanceID    string    `json:"instanceId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	StreamID      string    `json:"streamId"`
	ParticipantID string    `json:"participantId"`
	StreamType    string    `json:"streamType"`
	StartedAt     time.Time `json:"startedAt"`
}

type SessionRegistry struct {
	client     *redis.Client
	instanceID string
}

func NewSessionRegistry(client *redis.Client) *SessionRegistry {
	instanceID, _ := os.Hostname()
	return &SessionRegistry{
		client:     client,
		instanceID: instanceID,
	}
}

func (r *SessionRegistry) Register(ctx context.Context, scheduleID, sessionID, participantID, streamType, streamID string, startedAt time.Time) error {
	session := SessionInfo{
		InstanceID:    r.instanceID,
		ScheduleID:    scheduleID,
		SessionID:     sessionID,
		StreamID:      streamID,
		ParticipantID: participantID,
		StreamType:    streamType,
		StartedAt:     startedAt,
	}
	val, err := json.Marshal(session)

	if err != nil {
		return fmt.Errorf("streaming session registry marshal: %w", err)
	}
	return r.client.Set(ctx, sessionKey(scheduleID, sessionID, participantID, streamType), val, sessionTTL).Err()
}

func (r *SessionRegistry) Unregister(ctx context.Context, scheduleID, sessionID, participantID, streamType string) error {
	return r.client.Del(ctx, sessionKey(scheduleID, sessionID, participantID, streamType)).Err()
}

func (r *SessionRegistry) Lookup(ctx context.Context, scheduleID, sessionID, participantID, streamType string) (*SessionInfo, error) {
	val, err := r.client.Get(ctx, sessionKey(scheduleID, sessionID, participantID, streamType)).Bytes()
	if err != nil {
		return nil, fmt.Errorf("streaming session registry lookup: %w", err)
	}
	var info SessionInfo
	if err := json.Unmarshal(val, &info); err != nil {
		return nil, fmt.Errorf("streaming session registry unmarshal: %w", err)
	}
	return &info, nil
}

func (r *SessionRegistry) ScanSchedule(ctx context.Context, scheduleID string) ([]SessionInfo, error) {
	pattern := fmt.Sprintf("streaming-session:%s:*", scheduleID)
	return r.scanByPattern(ctx, pattern)
}

func (r *SessionRegistry) ScanAll(ctx context.Context) ([]SessionInfo, error) {
	return r.scanByPattern(ctx, "streaming-session:*")
}

func sessionKey(scheduleID, sessionID, participantID, streamType string) string {
	return fmt.Sprintf("streaming-session:%s:%s:%s:%s", scheduleID, sessionID, participantID, streamType)
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

func (r *SessionRegistry) Refresh(ctx context.Context, scheduleID, sessionID, participantID, streamType string) error {
	return r.client.Expire(ctx, sessionKey(scheduleID, sessionID, participantID, streamType), sessionTTL).Err()
}

var (
	ErrUploadSessionNotFound = errors.New("upload session not found")
	ErrUploadSessionExpired  = errors.New("upload session expired")
)

type UploadSession struct {
	StreamID    string    `json:"streamId"`
	CandidateID string    `json:"candidateId"`
	SessionID   string    `json:"sessionId"`
	ScheduleID  string    `json:"scheduleId"`
	StreamType  string    `json:"streamType"`
	CreatedAt   time.Time `json:"createdAt"`
	ExpiresAt   time.Time `json:"expiresAt"`
	Completed   bool      `json:"completed"`
}

func uploadSessionKey(streamID string) string {
	return fmt.Sprintf("upload-session:%s", streamID)
}

func uploadSessionIndexKey(session UploadSession) string {
	return fmt.Sprintf(
		"upload-session-index:%s:%s:%s:%s",
		session.ScheduleID,
		session.SessionID,
		session.CandidateID,
		session.StreamType,
	)
}

var registerUploadScript = redis.NewScript(`
local existingKey = redis.call("GET", KEYS[1])
if existingKey then
  local existing = redis.call("GET", existingKey)
  if existing then
    local existingSession = cjson.decode(existing)
    if not existingSession.completed then
      return existing
    end
  end
  redis.call("DEL", KEYS[1])
end

redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[2])
redis.call("SET", KEYS[1], KEYS[2], "PX", ARGV[2])
return ARGV[1]
`)

// RegisterOrGetUpload guarantees one active upload stream per candidate, exam
// session and stream type. Repeating POST /stream/sessions resumes an unfinished
// stream; after completion it creates a new stream so a resumed exam can record
// another contiguous part without reopening the finalized stream.
func (r *SessionRegistry) RegisterOrGetUpload(ctx context.Context, session UploadSession) (*UploadSession, bool, error) {
	ttl := time.Until(session.ExpiresAt)
	if ttl <= 0 {
		return nil, false, ErrUploadSessionExpired
	}

	val, err := json.Marshal(session)
	if err != nil {
		return nil, false, fmt.Errorf("upload session registry marshal: %w", err)
	}

	stored, err := registerUploadScript.Run(
		ctx,
		r.client,
		[]string{uploadSessionIndexKey(session), uploadSessionKey(session.StreamID)},
		string(val),
		ttl.Milliseconds(),
	).Text()
	if err != nil {
		return nil, false, fmt.Errorf("register upload session: %w", err)
	}

	var registered UploadSession
	if err := json.Unmarshal([]byte(stored), &registered); err != nil {
		return nil, false, fmt.Errorf("upload session registry unmarshal: %w", err)
	}

	return &registered, registered.StreamID == session.StreamID, nil
}

func (r *SessionRegistry) LookupUpload(ctx context.Context, streamID string) (*UploadSession, error) {
	val, err := r.client.Get(ctx, uploadSessionKey(streamID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrUploadSessionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("upload session registry lookup: %w", err)
	}

	var session UploadSession
	if err := json.Unmarshal(val, &session); err != nil {
		return nil, fmt.Errorf("upload session registry unmarshal: %w", err)
	}
	if !time.Now().UTC().Before(session.ExpiresAt) {
		_ = r.client.Del(ctx, uploadSessionKey(streamID)).Err()
		return nil, ErrUploadSessionExpired
	}

	return &session, nil
}

var markUploadCompleteScript = redis.NewScript(`
local raw = redis.call("GET", KEYS[1])
if not raw then
  return -1
end

local session = cjson.decode(raw)
if session.completed then
  return 0
end

session.completed = true
redis.call("SET", KEYS[1], cjson.encode(session), "KEEPTTL")
return 1
`)

// MarkUploadComplete atomically closes the upload session while preserving its
// existing expiry. The bool is false when the session was already complete.
func (r *SessionRegistry) MarkUploadComplete(ctx context.Context, streamID string) (bool, error) {
	result, err := markUploadCompleteScript.Run(
		ctx,
		r.client,
		[]string{uploadSessionKey(streamID)},
	).Int64()
	if err != nil {
		return false, fmt.Errorf("mark upload session complete: %w", err)
	}

	switch result {
	case -1:
		return false, ErrUploadSessionNotFound
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("mark upload session complete: unexpected result %d", result)
	}
}
