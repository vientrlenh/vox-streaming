package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"go.uber.org/zap"
)

type StreamInfo struct {
	ParticipantID string    `json:"participantId"`
	StreamID      string    `json:"streamId"`
	StreamType    string    `json:"streamType"`
	StartedAt     time.Time `json:"startedAt"`
}

type RoomSummary struct {
	RoomID      string       `json:"roomId"`
	ActiveCount int          `json:"activeCount"`
	Streams     []StreamInfo `json:"streams"`
}

type SessionScanner interface {
	ScanRoom(ctx context.Context, roomID string) ([]cache.SessionInfo, error)
	ScanAll(ctx context.Context) ([]cache.SessionInfo, error)
}

type ParticipantEventer interface {
	PublishParticipantEvent(ctx context.Context, roomID string, event domain.ParticipantEvent)
	SubscribeEvents(ctx context.Context, roomID string) <-chan domain.ParticipantEvent
}

type AlertEventer interface {
	PublishAlertEvent(ctx context.Context, roomID string, alert domain.AlertEvent) error
	SubscribeAlerts(ctx context.Context, roomID string) <-chan domain.AlertEvent
}

type MonitorUseCase struct {
	scanner SessionScanner
	participantEventer ParticipantEventer
	alertEventer AlertEventer
	logger *zap.Logger
}


func NewMonitorUseCase(scanner SessionScanner, participantEventer ParticipantEventer, alertEventer AlertEventer, logger *zap.Logger) *MonitorUseCase {
	return &MonitorUseCase{
		scanner: scanner, 
		participantEventer: participantEventer,
		alertEventer: alertEventer,
		logger: logger,
	}
}

// trả về danh sách stream đang active trong phòng, được gọi ngay khi monitor kết nối để render trạng thái ban đầu
func (u *MonitorUseCase) GetRoomSnapshot(ctx context.Context, roomID string) ([]StreamInfo, error) {
	sessions, err := u.scanner.ScanRoom(ctx, roomID)
	if err != nil {
		return nil, fmt.Errorf("get room snapshot: %w", err)
	}
	infos := make([]StreamInfo, 0, len(sessions))
	for _, s := range sessions {
		infos = append(infos, StreamInfo{
			ParticipantID: s.ParticipantID,
			StreamID: s.StreamID, 
			StreamType: s.StreamType, 
			StartedAt: s.StartedAt,
		})
	}
	u.logger.Debug("room snapshot detected", 
		zap.String("roomId", roomID), 
		zap.Int("activeStreams", 
		len(infos)),
	)
	return infos, nil
}


// trả về tất cả phòng đang có stream - dành cho school admin
func (u *MonitorUseCase) GetActiveRooms(ctx context.Context, allowedRoomIDs []string) ([]RoomSummary, error) {
	all, err := u.scanner.ScanAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("get active rooms:%w", err)
	}
	allowed := make(map[string]bool, len(allowedRoomIDs))
	for _, id := range allowedRoomIDs {
		allowed[id] = true
	}
	roomSession := make(map[string][]StreamInfo)
	for _, s := range all {
		if !allowed[s.RoomID] {
			continue
		}
		roomSession[s.RoomID] = append(roomSession[s.RoomID], StreamInfo{
			StreamID: s.StreamID, 
			ParticipantID: s.ParticipantID, 
			StreamType: s.StreamType, 
			StartedAt: s.StartedAt,
		})
	}
	result := make([]RoomSummary, 0, len(roomSession))
	for k, v := range roomSession {
		result = append(result, RoomSummary{
			RoomID: k,
			ActiveCount: len(v),
			Streams: v,
		})
	}
	u.logger.Debug("active room fetched", 
		zap.Int("allowedRooms", len(allowedRoomIDs)), 
		zap.Int("activeRooms", len(result)),
	)
	return result, nil
}

func (u *MonitorUseCase) NotifyJoined(ctx context.Context, roomID, participantID, streamID, streamType string) {
	u.participantEventer.PublishParticipantEvent(ctx, roomID, domain.ParticipantEvent{
		Type: domain.ParticipantJoined, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		StreamType: streamType, 
		At: time.Now().UTC(),
	})
}

func (u *MonitorUseCase) NotifyLeft(ctx context.Context, roomID, participantID, streamID, streamType string) {
	u.participantEventer.PublishParticipantEvent(ctx, roomID, domain.ParticipantEvent{
		Type: domain.ParticipantLeft, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		StreamType: streamType,
		At: time.Now().UTC(),
	})
}

func (u *MonitorUseCase) SubscribeEvents(ctx context.Context, roomID string) <-chan domain.ParticipantEvent {
	return u.participantEventer.SubscribeEvents(ctx, roomID)
}


func (u *MonitorUseCase) PublishAlert(ctx context.Context, roomID, participantID, streamID, alertType string, confidence float64, capturedAt time.Time) error {
	return u.alertEventer.PublishAlertEvent(ctx, roomID, domain.AlertEvent{
		RoomID: roomID, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		AlertType: alertType, 
		Confidence: confidence, 
		CapturedAt: capturedAt,
	})
}


func (u *MonitorUseCase) SubscribeAlerts(ctx context.Context, roomID string) <-chan domain.AlertEvent {
	return u.alertEventer.SubscribeAlerts(ctx, roomID)
}