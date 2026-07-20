package domain

import (
	"context"
	"time"
)

const (
	TopicFrameReady    = "exam.frame.ready"
	TopicStreamStarted = "exam.stream.started"
	TopicStreamEnded   = "exam.stream.ended"
	TopicScheduleClosed    = "exam.schedule.closed"
	TopicAlertRaised = "exam.alert.raised"
)

type FrameReadyEvent struct {
	EventID       string    `json:"eventId"`
	ScheduleID        string    `json:"scheduleId"`
	SessionID 	  string 	`json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	FrameURL      string    `json:"frameUrl"`
	CapturedAt    time.Time `json:"capturedAt"`
	SequenceNo    int64     `json:"sequenceNo"`
}

type StreamStartedEvent struct {
	EventID       string    `json:"eventId"`
	ScheduleID        string    `json:"scheduleId"`
	SessionID 	  string 	`json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	StartedAt     time.Time `json:"startedAt"`
}

type StreamEndedEvent struct {
	EventID       string    `json:"eventId"`
	ScheduleID        string    `json:"scheduleId"`
	SessionID  	  string	`json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	SegmentKeys   []string  `json:"segmentKeys"`
	Duration      int64     `json:"durationSecs"`
	EndedAt       time.Time `json:"endedAt"`
}

type ScheduleClosedEvent struct {
	EventID  string    `json:"eventId"`
	ScheduleID   string    `json:"scheduleId"`
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

type AlertLevel string

const (
	AlertLevelCritical AlertLevel = "CRITICAL"
	AlertLevelWarning  AlertLevel = "WARNING"
	AlertLevelInfo     AlertLevel = "INFO"
)

const (
	AlertSourceAI = "ai"
	AlertSourceStreaming = "streaming"
)

func DefaultAlertLevel(alertType string) AlertLevel {
	switch alertType {
	case AlertPhoneDetected, AlertMultiplePersons, AlertProhibitedObject: 
		return AlertLevelCritical
	case AlertFaceNotVisible, AlertSuspiciousGaze, AlertStreamDropped, AlertTrackEnded, AlertReconnectLoop, AlertRecordingIncomplete:
		return AlertLevelWarning
	default: 
		return AlertLevelInfo
	}
}

type AlertEvent struct {
	Source        string     `json:"source"`
	ScheduleID        string     `json:"scheduleId"`
	ParticipantID string     `json:"participantId"`
	StreamID      string     `json:"streamId"`
	StreamType    string     `json:"streamType"`
	AlertType     string     `json:"alertType"`
	Detail        string     `json:"detail"`
	Confidence    float64    `json:"confidence"`
	SequenceNo    int64      `json:"sequenceNo"`
	Level         AlertLevel `json:"level"`
	CapturedAt    time.Time  `json:"capturedAt"`
}

const (
	// AI detect alerts
	AlertPhoneDetected   = "PHONE_DETECTED"
	AlertMultiplePersons = "MULTIPLE_PERSONS"
	AlertFaceNotVisible  = "FACE_NOT_VISIBLE"
	AlertSuspiciousGaze  = "SUSPICIOUS_GAZE"
	AlertProhibitedObject = "PROHIBITED_OBJECT"

	// Streaming service detect alerts
	AlertStreamDropped       = "STREAM_DROPPED"
	AlertTrackEnded          = "TRACK_ENDED"
	AlertReconnectLoop       = "RECONNECT_LOOP"
	AlertRecordingIncomplete = "RECORDING_INCOMPLETE"
)

type AlertRaisedEvent struct {
	EventID  string    `json:"eventId"`
	RaisedAt time.Time `json:"raisedAt"`
	AlertEvent
}

type EventPublisher interface {
	PublishFrameReady(ctx context.Context, event FrameReadyEvent) error
	PublishStreamStarted(ctx context.Context, event StreamStartedEvent) error
	PublishStreamEnded(ctx context.Context, event StreamEndedEvent) error
	PublishScheduleClosed(ctx context.Context, event ScheduleClosedEvent) error
}
