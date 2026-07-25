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
}

type ParticipantEventer interface {
	PublishParticipantEvent(ctx context.Context, scheduleID string, event domain.ParticipantEvent)
	SubscribeEvents(ctx context.Context, scheduleID string) <-chan domain.ParticipantEvent
}

type AlertEventer interface {
	PublishAlertEvent(ctx context.Context, scheduleID string, alert domain.AlertEvent) error
	SubscribeAlerts(ctx context.Context, scheduleID string) <-chan domain.AlertEvent
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

func (u *MonitorUseCase) NotifyJoined(ctx context.Context, scheduleID, participantID, streamID, streamType string) {
	u.participantEventer.PublishParticipantEvent(ctx, scheduleID, domain.ParticipantEvent{
		Type: domain.ParticipantJoined, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		StreamType: streamType, 
		At: time.Now().UTC(),
	})
}

func (u *MonitorUseCase) NotifyLeft(ctx context.Context, scheduleID, participantID, streamID, streamType string) {
	u.participantEventer.PublishParticipantEvent(ctx, scheduleID, domain.ParticipantEvent{
		Type: domain.ParticipantLeft, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		StreamType: streamType,
		At: time.Now().UTC(),
	})
}

func (u *MonitorUseCase) SubscribeEvents(ctx context.Context, scheduleID string) <-chan domain.ParticipantEvent {
	return u.participantEventer.SubscribeEvents(ctx, scheduleID)
}


func (u *MonitorUseCase) PublishAlert(ctx context.Context, alert domain.AlertEvent, eventID string) error {
	if eventID == "" {
		eventID = uuid.NewString()
	}
	if alert.Level == "" {
		alert.Level = domain.DefaultAlertLevel(alert.AlertType)
	}
	if alert.CapturedAt.IsZero() {
		alert.CapturedAt = time.Now().UTC()
	}

	// redis pub/sub live, do not block because of kafka
	liveErr := u.alertEventer.PublishAlertEvent(ctx, alert.ScheduleID, alert)
	if liveErr != nil {
		u.logger.Warn("live alert publish failed", 
			zap.String("scheduleId", alert.ScheduleID), 
			zap.String("alertType", alert.AlertType), 
			zap.Error(liveErr),
		)
	}

	// durable - kafka exam.alert.raised (persist/flag/audit)
	var durErr error
	if u.alertPublisher != nil {
		durErr = u.alertPublisher.PublishAlertRaised(ctx, domain.AlertRaisedEvent{
			EventID: uuid.NewString(), 
			RaisedAt: time.Now().UTC(),
			AlertEvent: alert,
		}, )
		if durErr != nil {
			u.logger.Error("durable alert publish failed", 
				zap.String("scheduleId", alert.ScheduleID), 
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


func (u *MonitorUseCase) SubscribeAlerts(ctx context.Context, scheduleID string) <-chan domain.AlertEvent {
	return u.alertEventer.SubscribeAlerts(ctx, scheduleID)
}