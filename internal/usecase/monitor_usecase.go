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
	ParticipantID string
	StreamID      string
	StreamType    string
	StartedAt     time.Time
}

type RoomSummary struct {
	RoomID string
	ActiveCount int
	Streams []StreamInfo
}

type SessionScanner interface {
	ScanRoom(ctx context.Context, roomID string) ([]cache.SessionInfo, error)
	ScanAll(ctx context.Context) ([]cache.SessionInfo, error)
}

type ParticipantEventer interface {
	PublishParticipantEvent(ctx context.Context, roomID string, event domain.ParticipantEvent)
	SubscribeEvents(ctx context.Context, roomID string) <-chan domain.ParticipantEvent
}

type MonitorUseCase struct {
	scanner SessionScanner
	eventer ParticipantEventer
	logger *zap.Logger
}


func NewMonitorUseCase(scanner SessionScanner, eventer ParticipantEventer, logger *zap.Logger) *MonitorUseCase {
	return &MonitorUseCase{
		scanner: scanner, 
		eventer: eventer,
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
	u.logger.Debug("room snapshot detected", zap.String("room_id", roomID), zap.Int("active_streams", len(infos)))
	return infos, nil
}


// trả về tất cả phòng đang có stream - dành cho school admin
func (u *MonitorUseCase) GetActiveRooms(ctx context.Context) ([]RoomSummary, error) {
	all, err := u.scanner.ScanAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("get active rooms:%w", err)
	}
	byRoom := make(map[string]*RoomSummary)
	for _, s := range all {
		rs, ok := byRoom[s.ParticipantID]
		_ = rs
		_ = ok
	}
	return nil, nil
}

func (u *MonitorUseCase) NotifyJoined(ctx context.Context, roomID, participantID, streamID, streamType string) {
	u.eventer.PublishParticipantEvent(ctx, roomID, domain.ParticipantEvent{
		Type: domain.ParticipantJoined, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		StreamType: streamType, 
		At: time.Now().UTC(),
	})
}

func (u *MonitorUseCase) NotifyLeft(ctx context.Context, roomID, participantID, streamID, streamType string) {
	u.eventer.PublishParticipantEvent(ctx, roomID, domain.ParticipantEvent{
		Type: domain.ParticipantLeft, 
		ParticipantID: participantID, 
		StreamID: streamID, 
		StreamType: streamType,
		At: time.Now().UTC(),
	})
}