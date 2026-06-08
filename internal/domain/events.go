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
	EventID       string    `json:"event_id"`
	RoomID        string    `json:"room_id"`
	ParticipantID string    `json:"participant_id"`
	StreamID      string    `json:"stream_id"`
	StreamType    string    `json:"stream_type"`
	FrameURL      string    `json:"frame_url"`
	CapturedAt    time.Time `json:"captured_at"`
	SequenceNo    int64     `json:"sequence_no"`
}


type StreamStartedEvent struct {
	EventID 		string			`json:"event_id"`
	RoomID			string 			`json:"room_id"`
	ParticipantID	string			`json:"participant_id"`
	StreamID 		string			`json:"stream_id"`
	StreamType 		string			`json:"stream_type"`
	StartedAt 		time.Time 		`json:"started_at"`
}

type StreamEndedEvent struct {
	EventID 		string 			`json:"event_id"`
	RoomID			string			`json:"room_id"`
	ParticipantID 	string 			`json:"participant_id"`
	StreamID		string			`json:"stream_id"`
	RecordingURL 	string 			`json:"recording_url"`
	Duration 		int64			`json:"duration_secs"`
	EndedAt 		time.Time 		`json:"ended_at"`
}


type RoomClosedEvent struct {
	EventID 		string 			`json:"event_id"`
	RoomID 			string 			`json:"room_id"`
	ExamID 			string 			`json:"exam_id"`
	ClosedAt 		time.Time 		`json:"closed_at"`
	Reason 			string 			`json:"reason"`
}


type EventPublisher interface {
	PublishFrameReady(ctx context.Context, event FrameReadyEvent) error
	PublishStreamStarted(ctx context.Context, event StreamStartedEvent) error 
	PublishStreamEnded(ctx context.Context, event StreamEndedEvent) error
	PublishRoomClosed(ctx context.Context, event RoomClosedEvent) error
}