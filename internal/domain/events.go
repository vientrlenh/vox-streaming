package domain

import (
	"context"
	"time"
)

const (
	TopicFrameReady    = "exam.frame.ready"
	TopicStreamStarted = "exam.stream.started"
	TopicStreamEnded   = "exam.stream.ended"
	TopicRoomClosed    = "exam.room.closed"
)

type FrameReadyEvent struct {
	EventID       string    `json:"eventId"`
	RoomID        string    `json:"roomId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	FrameURL      string    `json:"frameUrl"`
	CapturedAt    time.Time `json:"capturedAt"`
	SequenceNo    int64     `json:"sequenceNo"`
}

type StreamStartedEvent struct {
	EventID       string    `json:"eventId"`
	RoomID        string    `json:"roomId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	StartedAt     time.Time `json:"startedAt"`
}

type StreamEndedEvent struct {
	EventID       string    `json:"eventId"`
	RoomID        string    `json:"roomId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	RecordingURL  string    `json:"recordingUrl"`
	Duration      int64     `json:"durationSecs"`
	EndedAt       time.Time `json:"endedAt"`
}

type RoomClosedEvent struct {
	EventID  string    `json:"eventId"`
	RoomID   string    `json:"roomId"`
	ExamID   string    `json:"examId"`
	ClosedAt time.Time `json:"closedAt"`
	Reason   string    `json:"reason"`
}

type ParticipantEvent struct {
	Type          string    `json:"type"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	At            time.Time `json:"at"`
}

const (
	ParticipantJoined = "joined"
	ParticipantLeft   = "left"
)

type AlertEvent struct {
	RoomID        string    `json:"roomId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	AlertType     string    `json:"alertType"`
	Confidence    float64   `json:"confidence"`
	CapturedAt    time.Time `json:"capturedAt"`
}

const (
	// AI detect alerts
	AlertPhoneDetected   = "PHONE_DETECTED"
	AlertMultiplePersons = "MULTIPLE_PERSONS"
	AlertFaceNotVisible  = "FACE_NOT_VISIBLE"
	AlertSuspiciousGaze  = "SUSPICIOUS_GAZE"

	// Streaming service detect alerts
	AlertStreamDropped = "STREAM_DROPPED"
	AlertTrackEnded    = "TRACK_ENDED"
	AlertReconnectLoop = "RECONNECT_LOOP"
)

type EventPublisher interface {
	PublishFrameReady(ctx context.Context, event FrameReadyEvent) error
	PublishStreamStarted(ctx context.Context, event StreamStartedEvent) error
	PublishStreamEnded(ctx context.Context, event StreamEndedEvent) error
	PublishRoomClosed(ctx context.Context, event RoomClosedEvent) error
}
