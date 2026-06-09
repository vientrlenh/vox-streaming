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

type StreamUseCase struct {
	publisher domain.EventPublisher
	sessions  *cache.SessionRegistry
	logger    *zap.Logger
}

func NewStreamUseCase(publisher domain.EventPublisher, sessions *cache.SessionRegistry, logger *zap.Logger) *StreamUseCase {
	return &StreamUseCase{
		publisher: publisher,
		sessions: sessions,
		logger:    logger,
	}
}

func (u *StreamUseCase) NotifyStreamStarted(ctx context.Context, roomID, participantID, streamID, streamType string) error {
	startedAt := time.Now().UTC()
	if err := u.sessions.Register(ctx, roomID, participantID, streamType, streamID, startedAt); err != nil {
		u.logger.Warn("session register failed - monitor may miss this peer",
			zap.String("stream_id", streamID), 
			zap.Error(err), 
		)
	}

	event := domain.StreamStartedEvent{
		EventID:       uuid.NewString(),
		RoomID:        roomID,
		ParticipantID: participantID,
		StreamID:      streamID,
		StreamType:    streamType,
		StartedAt:     startedAt,
	}

	if err := u.publisher.PublishStreamStarted(ctx, event); err != nil {
		return fmt.Errorf("notify stream started: %w", err)
	}

	u.logger.Info("stream started event published",
		zap.String("stream_id", streamID),
		zap.String("room_id", roomID),
		zap.String("type", streamType),
	)

	return nil
}

func (u *StreamUseCase) PublishFrame(ctx context.Context, roomID, participantID, streamID, streamType, frameURL string, seq int64) error {
	// refresh Redis trước để tách độc lập với Kafka
	// frame tick = peer đang sống -> refresh session dù Kafka có fail hay không
	if err := u.sessions.Refresh(ctx, roomID, participantID, streamType); err != nil {
		u.logger.Warn("session refresh failed", 
			zap.String("stream_id", streamID), 
			zap.Error(err),
		)
	}

	event := domain.FrameReadyEvent{
		EventID:       uuid.NewString(),
		RoomID:        roomID,
		ParticipantID: participantID,
		StreamID:      streamID,
		StreamType:    streamType,
		CapturedAt:    time.Now().UTC(),
		FrameURL:      frameURL,
		SequenceNo:    seq,
	}

	// publish event - failure trả error ngay
	if err := u.publisher.PublishFrameReady(ctx, event); err != nil {
		// warning, không fail stream
		u.logger.Error("publish frame event failed", 
			zap.String("stream_id", streamID), 
			zap.Int64("seq", seq), 
			zap.Error(err),
		)
		return err
	}
	u.logger.Debug("frame event published",
		zap.String("stream_id", streamID),
		zap.Int64("seq", seq),
		zap.String("frame_url", frameURL),
	)


	return nil
}

func (u *StreamUseCase) NotifyStreamEnded(ctx context.Context, roomID, participantID, streamID, streamType, recordingURL string, durationSecs int64) error {
	// peer đóng -> unregister
	if err := u.sessions.Unregister(ctx, roomID, participantID, streamType); err != nil {
		u.logger.Warn("session unregister failed", 
			zap.String("stream_id", streamID), 
			zap.Error(err),
		)
	}
	event := domain.StreamEndedEvent{
		EventID:       uuid.NewString(),
		RoomID:        roomID,
		ParticipantID: participantID,
		StreamID:      streamID,
		StreamType:    streamType,
		RecordingURL:  recordingURL,
		Duration:      durationSecs,
		EndedAt:       time.Now().UTC(),
	}

	if err := u.publisher.PublishStreamEnded(ctx, event); err != nil {
		return fmt.Errorf("notify stream ended: %w", err)
	}

	u.logger.Info("stream ended event published",
		zap.String("stream_id", streamID),
		zap.String("recording_url", recordingURL),
		zap.Int64("duration_secs", durationSecs),
	)

	return nil
}
