package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"go.uber.org/zap"
)

type StreamUseCase struct {
	publisher domain.EventPublisher
	logger    *zap.Logger
}

func NewStreamUseCase(publisher domain.EventPublisher, logger *zap.Logger) *StreamUseCase {
	return &StreamUseCase{
		publisher: publisher,
		logger:    logger,
	}
}

func (u *StreamUseCase) NotifyStreamStarted(ctx context.Context, roomID, participantID, streamID, streamType string) error {
	event := domain.StreamStartedEvent{
		EventID:       uuid.NewString(),
		RoomID:        roomID,
		ParticipantID: participantID,
		StreamID:      streamID,
		StreamType:    streamType,
		StartedAt:     time.Now().UTC(),
	}

	if err := u.publisher.PublishStreamStarted(ctx, event); err != nil {
		return fmt.Errorf("notify stream started: %w", err)
	}

	u.logger.Info("stream started event published", zap.String("stream_id", streamID), zap.String("room_id", roomID), zap.String("type", streamType))
	return nil
}

func (u *StreamUseCase) PublishFrame(ctx context.Context, roomID, participantID, streamID, streamType, frameURL string, seq int64) error {
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

	if err := u.publisher.PublishFrameReady(ctx, event); err != nil {
		// warning, không fail stream
		u.logger.Error("publish frame event failed", zap.String("stream_id", streamID), zap.Int64("seq", seq), zap.Error(err))
		return err
	}
	u.logger.Debug("frame event published", zap.String("stream_id", streamID), zap.Int64("seq", seq), zap.String("frame_url", frameURL))
	return nil
}

func (u *StreamUseCase) NotifyStreamEnded(ctx context.Context, roomID, participantID, streamID, recordingURL string, durationSecs int64) error {
	event := domain.StreamEndedEvent{
		EventID:       uuid.NewString(),
		RoomID:        roomID,
		ParticipantID: participantID,
		StreamID:      streamID,
		RecordingURL:  recordingURL,
		Duration:      durationSecs,
		EndedAt:       time.Now().UTC(),
	}

	if err := u.publisher.PublishStreamEnded(ctx, event); err != nil {
		return fmt.Errorf("notify stream ended: %w", err)
	}

	u.logger.Info("stream ended event published", zap.String("stream_id", streamID), zap.String("recording_url", recordingURL), zap.Int64("duration_secs", durationSecs))
	return nil
}
