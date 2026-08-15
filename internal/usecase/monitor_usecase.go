package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"go.uber.org/zap"
)

type StreamInfo struct {
	SessionID 	  string 	`json:"sessionId"`
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	StartedAt     time.Time `json:"startedAt"`
}

type ScheduleSummary struct {
	ScheduleID      string       `json:"scheduleId"`
	ActiveCount int          `json:"activeCount"`
	Streams     []StreamInfo `json:"streams"`
}

type SessionScanner interface {
	ScanSchedule(ctx context.Context, scheduleID string) ([]cache.SessionInfo, error)
	ScanAll(ctx context.Context) ([]cache.SessionInfo, error)
	ScanSession(ctx context.Context, sessionID string) ([]cache.SessionInfo, error)
}

type ParticipantEventer interface {
	PublishParticipantEvent(ctx context.Context, scheduleID string, event domain.ParticipantEvent)
	SubscribeEvents(ctx context.Context, scheduleID string) <-chan domain.ParticipantEvent
}

type AlertEventer interface {
	PublishAlertEvent(ctx context.Context, sessionID string, alert domain.AlertEvent) error
	SubscribeAlerts(ctx context.Context, sessionID string) <-chan domain.AlertEvent
}

type AlertRaisedPublisher interface {
	PublishAlertRaised(ctx context.Context, event domain.AlertRaisedEvent) error
}

type MonitorUseCase struct {
	scanner SessionScanner
	participantEventer ParticipantEventer
	alertEventer AlertEventer
	alertPublisher AlertRaisedPublisher
	logger *zap.Logger
}


func NewMonitorUseCase(scanner SessionScanner, participantEventer ParticipantEventer, alertEventer AlertEventer, alertPublisher AlertRaisedPublisher, logger *zap.Logger) *MonitorUseCase {
	return &MonitorUseCase{
		scanner: scanner, 
		participantEventer: participantEventer,
		alertEventer: alertEventer, 
		alertPublisher: alertPublisher,
		logger: logger,
	}
}

// trả về danh sách stream đang active trong phòng, được gọi ngay khi monitor kết nối để render trạng thái ban đầu
func (u *MonitorUseCase) GetScheduleSnapshot(ctx context.Context, scheduleID string) ([]StreamInfo, error) {
	sessions, err := u.scanner.ScanSchedule(ctx, scheduleID)
	if err != nil {
		return nil, fmt.Errorf("get schedule snapshot: %w", err)
	}
	infos := make([]StreamInfo, 0, len(sessions))
	for _, s := range sessions {
		infos = append(infos, StreamInfo{ 
			SessionID: s.SessionID, 
			ParticipantID: s.ParticipantID,
			StreamID: s.StreamID, 
			StreamType: s.StreamType, 
			StartedAt: s.StartedAt,
		})
	}
	u.logger.Debug("schedule snapshot detected", 
		zap.String("scheduleId", scheduleID), 
		zap.Int("activeStreams", 
		len(infos)),
	)
	return infos, nil
}


// trả về tất cả phòng đang có stream - dành cho school admin
func (u *MonitorUseCase) GetActiveSchedules(ctx context.Context, allowedScheduleIDs []string) ([]ScheduleSummary, error) {
	all, err := u.scanner.ScanAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("get active schedules:%w", err)
	}
	allowed := make(map[string]bool, len(allowedScheduleIDs))
	for _, id := range allowedScheduleIDs {
		allowed[id] = true
	}
	scheduleSession := make(map[string][]StreamInfo)
	for _, s := range all {
		if !allowed[s.ScheduleID] {
			continue
		}
		scheduleSession[s.ScheduleID] = append(scheduleSession[s.ScheduleID], StreamInfo{
			SessionID: s.SessionID, 
			StreamID: s.StreamID, 
			ParticipantID: s.ParticipantID, 
			StreamType: s.StreamType, 
			StartedAt: s.StartedAt,
		})
	}
	result := make([]ScheduleSummary, 0, len(scheduleSession))
	for k, v := range scheduleSession {
		result = append(result, ScheduleSummary{
			ScheduleID: k,
			ActiveCount: len(v),
			Streams: v,
		})
	}
	u.logger.Debug("active schedule fetched", 
		zap.Int("allowedSchedules", len(allowedScheduleIDs)), 
		zap.Int("activeSchedules", len(result)),
	)
	return result, nil
}

// FindLiveStream reports whether streamID is currently live under
// scheduleID, returning its session info if so. Reuses the same
// SessionScanner (Redis, instance-agnostic) as GetScheduleSnapshot — this is
// deliberately NOT backed by the in-process SessionManager in the webrtc
// package, which only reflects peers held by the instance handling this
// particular request and would false-negative on a multi-instance deployment.
// Returns (nil, nil) if the stream isn't live (ended, or never existed).
func (u *MonitorUseCase) FindLiveStream(ctx context.Context, scheduleID, streamID string) (*cache.SessionInfo, error) {
	sessions, err := u.scanner.ScanSchedule(ctx, scheduleID)
	if err != nil {
		return nil, fmt.Errorf("find live stream: %w", err)
	}
	for _, s := range sessions {
		if s.StreamID == streamID {
			return &s, nil
		}
	}
	return nil, nil
}

func (u *MonitorUseCase) NotifyJoined(ctx context.Context, scheduleID, sessionID, participantID, streamID, streamType string) {
	u.notifyParticipant(ctx, domain.ParticipantJoined, scheduleID, sessionID, participantID, streamID, streamType)
}

func (u *MonitorUseCase) NotifyLeft(ctx context.Context, scheduleID, sessionID, participantID, streamID, streamType string) {
	u.notifyParticipant(ctx, domain.ParticipantLeft, scheduleID, sessionID, participantID, streamID, streamType)
}

// NotifyDisconnected reports that a stream's transport dropped while the stream is still open --
// the peer is inside its reconnect grace period and may yet recover. Pair with NotifyReconnected.
func (u *MonitorUseCase) NotifyDisconnected(ctx context.Context, scheduleID, sessionID, participantID, streamID, streamType string) {
	u.notifyParticipant(ctx, domain.ParticipantDisconnected, scheduleID, sessionID, participantID, streamID, streamType)
}

// NotifyReconnected reports that a stream previously announced as disconnected is back.
func (u *MonitorUseCase) NotifyReconnected(ctx context.Context, scheduleID, sessionID, participantID, streamID, streamType string) {
	u.notifyParticipant(ctx, domain.ParticipantReconnected, scheduleID, sessionID, participantID, streamID, streamType)
}

func (u *MonitorUseCase) notifyParticipant(ctx context.Context, eventType, scheduleID, sessionID, participantID, streamID, streamType string) {
	u.participantEventer.PublishParticipantEvent(ctx, scheduleID, domain.ParticipantEvent{
		Type:          eventType,
		SessionID:     sessionID,
		ParticipantID: participantID,
		StreamID:      streamID,
		StreamType:    streamType,
		At:            time.Now().UTC(),
	})
}

func (u *MonitorUseCase) SubscribeEvents(ctx context.Context, scheduleID string) <-chan domain.ParticipantEvent {
	return u.participantEventer.SubscribeEvents(ctx, scheduleID)
}


// enrichAlertIdentity fills in a participant/stream id the caller could not supply, looking it up
// from this service's own session registry by exam session id.
//
// The alert producers are not all equally informed, and the least informed one is not fixable at
// the source: the AI proctoring service on its direct client path receives only an exam attempt id,
// so it has no way to know which candidate or which stream that is. Rather than let it guess -- it
// used to copy the session id into all three fields, which is how monitors ended up printing a raw
// UUID where a student name belongs, and how AI alerts stopped matching any tile on the grid --
// this service answers the question itself. It is the right place to: it minted the peer from the
// student's stream token, so it is the only participant in the chain that ever knew the mapping.
//
// A blank result is left blank. A wrong id is worse than a missing one, because a missing one shows
// up as a gap and a wrong one shows up as somebody else.
func (u *MonitorUseCase) enrichAlertIdentity(ctx context.Context, alert domain.AlertEvent) domain.AlertEvent {
	needsParticipant := alert.ParticipantID == "" || alert.ParticipantID == alert.SessionID
	needsStream := alert.StreamID == "" || alert.StreamID == alert.SessionID
	if alert.SessionID == "" || (!needsParticipant && !needsStream) {
		return alert
	}

	sessions, err := u.scanner.ScanSession(ctx, alert.SessionID)
	if err != nil {
		u.logger.Warn("resolve alert identity failed, forwarding alert as received",
			zap.String("sessionId", alert.SessionID),
			zap.Error(err),
		)
		return alert
	}
	if len(sessions) == 0 {
		return alert
	}

	// Prefer the registration matching the alert's own stream type; a session normally has one per
	// type and picking blindly would credit a camera alert to the screen stream.
	match := sessions[0]
	for _, session := range sessions {
		if alert.StreamType != "" && session.StreamType == alert.StreamType {
			match = session
			break
		}
	}

	if needsParticipant {
		alert.ParticipantID = match.ParticipantID
	}
	if needsStream {
		alert.StreamID = match.StreamID
	}
	if alert.StreamType == "" {
		alert.StreamType = match.StreamType
	}
	return alert
}

func (u *MonitorUseCase) PublishAlert(ctx context.Context, alert domain.AlertEvent, eventID string) error {
	if eventID == "" {
		eventID = uuid.NewString()
	}
	alert = u.enrichAlertIdentity(ctx, alert)
	if alert.Level == "" {
		alert.Level = domain.DefaultAlertLevel(alert.AlertType)
	}
	if alert.CapturedAt.IsZero() {
		alert.CapturedAt = time.Now().UTC()
	}

	// redis pub/sub live, do not block because of kafka
	liveErr := u.alertEventer.PublishAlertEvent(ctx, alert.SessionID, alert)
	if liveErr != nil {
		u.logger.Warn("live alert publish failed", 
			zap.String("sessionId", alert.SessionID), 
			zap.String("alertType", alert.AlertType), 
			zap.Error(liveErr),
		)
	}

	// durable - kafka exam.alert.raised (persist/flag/audit)
	var durErr error
	if u.alertPublisher != nil {
		durErr = u.alertPublisher.PublishAlertRaised(ctx, domain.AlertRaisedEvent{
			EventID: eventID, 
			RaisedAt: time.Now().UTC(),
			AlertEvent: alert,
		}, )
		if durErr != nil {
			u.logger.Error("durable alert publish failed", 
				zap.String("sessionId", alert.SessionID), 
				zap.String("alertType", alert.AlertType), 
				zap.Error(durErr),
			)
		}
	}

	if liveErr != nil && durErr != nil {
		return fmt.Errorf("alert delivery failed: live=%v durable=%v", liveErr, durErr)
	}
	return nil
}


func (u *MonitorUseCase) SubscribeAlerts(ctx context.Context, sessionID string) <-chan domain.AlertEvent {
	return u.alertEventer.SubscribeAlerts(ctx, sessionID)
}