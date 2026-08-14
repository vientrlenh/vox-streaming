package domain

import (
	"context"
	"time"
)

const (
	TopicFrameReady                 = "exam.frame.ready"
	TopicStreamStarted              = "exam.stream.started"
	TopicStreamEnded                = "exam.stream.ended"
	TopicRecordingAssemblyRequested = "exam.recording.assembly.requested"
	TopicRecordingPartChanged       = "exam.recording.part.changed"
	TopicScheduleClosed             = "exam.schedule.closed"
	TopicAlertRaised                = "exam.alert.raised"
)

type RecordingAssemblyRequestedEvent struct {
	EventID       string    `json:"eventId"`
	StreamID      string    `json:"streamId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamType    string    `json:"streamType"`
	Source        string    `json:"source"`
	RequestedAt   time.Time `json:"requestedAt"`
}

type RecordingPartChangedEvent struct {
	EventID       string    `json:"eventId"`
	StreamID      string    `json:"streamId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamType    string    `json:"streamType"`
	Source        string    `json:"source"`
	Status        string    `json:"status"`
	ObjectKey     string    `json:"objectKey,omitempty"`
	DurationSecs  int64     `json:"durationSecs,omitempty"`
	HasGaps       bool      `json:"hasGaps"`
	ErrorMessage  string    `json:"errorMessage,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
}

type FrameReadyEvent struct {
	EventID       string    `json:"eventId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	FrameURL      string    `json:"frameUrl"`
	CapturedAt    time.Time `json:"capturedAt"`
	SequenceNo    int64     `json:"sequenceNo"`
}

type StreamStartedEvent struct {
	EventID       string    `json:"eventId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	StartedAt     time.Time `json:"startedAt"`
}

type StreamEndedEvent struct {
	EventID       string    `json:"eventId"`
	ScheduleID    string    `json:"scheduleId"`
	SessionID     string    `json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	SegmentKeys   []string  `json:"segmentKeys"`
	Duration      int64     `json:"durationSecs"`
	EndedAt       time.Time `json:"endedAt"`
}

type ScheduleClosedEvent struct {
	EventID    string    `json:"eventId"`
	ScheduleID string    `json:"scheduleId"`
	ExamID     string    `json:"examId"`
	ClosedAt   time.Time `json:"closedAt"`
	Reason     string    `json:"reason"`
}

type ParticipantEvent struct {
	Type          string    `json:"type"`
	SessionID     string    `json:"sessionId"`
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
	AlertSourceAI        = "ai"
	AlertSourceStreaming = "streaming"
)

func DefaultAlertLevel(alertType string) AlertLevel {
	switch alertType {
	case AlertPhoneDetected, AlertMultiplePersons, AlertProhibitedObject:
		return AlertLevelCritical
	case AlertFaceNotVisible, AlertSuspiciousGaze, AlertStreamDropped, AlertTrackEnded, AlertReconnectLoop, AlertRecordingIncomplete, AlertWindowFocusLost:
		return AlertLevelWarning
	default:
		return AlertLevelInfo
	}
}

type AlertEvent struct {
	Source        string     `json:"source"`
	SessionID    string     `json:"sessionId"`
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
	AlertPhoneDetected    = "PHONE_DETECTED"
	AlertMultiplePersons  = "MULTIPLE_PERSONS"
	AlertFaceNotVisible   = "FACE_NOT_VISIBLE"
	AlertSuspiciousGaze   = "SUSPICIOUS_GAZE"
	AlertProhibitedObject = "PROHIBITED_OBJECT"

	// Client detect alerts -- do chinh app thi bao len, khong phai suy ra tu video.
	//
	// Thi sinh roi khoi cua so thi (WPF Window.Deactivated -> WS focus_lost -> Python
	// push_alert). Xep WARNING ngang FACE_NOT_VISIBLE, khong phai CRITICAL: mat focus co rat
	// nhieu nguyen nhan vo tinh -- popup Windows Update, thong bao he thong, tro chuot ra man
	// hinh phu -- nen de CRITICAL se lam giam thi quen dan voi muc canh bao cao nhat, va do
	// moi la thu lam hong ca he thong canh bao. Client da gop cac lan cach nhau duoi 3 giay
	// nen moi canh bao toi day la mot lan roi di that; nhieu lan don dap moi dang nghi.
	AlertWindowFocusLost = "WINDOW_FOCUS_LOST"

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
	PublishRecordingAssemblyRequested(ctx context.Context, event RecordingAssemblyRequestedEvent) error
	PublishRecordingPartChanged(ctx context.Context, event RecordingPartChangedEvent) error
}
