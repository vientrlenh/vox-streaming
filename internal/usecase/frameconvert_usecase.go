package usecase

import (
	"context"
	"fmt"

	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/recorder"
	"go.uber.org/zap"
)

type FrameBroadcaster interface {
	HasMonitor(ctx context.Context, scheduleID string) (bool, error)
	PublishFrameURL(ctx context.Context, scheduleID, streamID, streamType, frameURL string, seq int64)
}

type FrameConvertUseCase struct {
	storage *storage.Client
	broadcaster FrameBroadcaster
	sem chan struct{}
	logger *zap.Logger
}

func NewFrameConvertUseCase(storage *storage.Client, broadcaster FrameBroadcaster, maxConcurrent int, logger *zap.Logger) *FrameConvertUseCase {
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}
	return &FrameConvertUseCase{
		storage: storage, 
		broadcaster: broadcaster, 
		sem: make(chan struct{}, maxConcurrent), 
		logger: logger,
	}
}

func (u *FrameConvertUseCase) Convert(ctx context.Context, event domain.FrameReadyEvent) error {
	watching, err := u.broadcaster.HasMonitor(ctx, event.ScheduleID)
	if err != nil {
		u.logger.Warn("monitor check failed, converting anyway", zap.Error(err))
	} else if !watching {
		return nil
	}

	select {
	case u.sem<-struct{}{}:
		defer func() {
			<-u.sem
		}()
	case<-ctx.Done():
		return ctx.Err()
	}

	annexB, err := u.storage.DownloadFrame(ctx, event.ScheduleID, event.StreamID, event.SequenceNo)
	if err != nil {
		return fmt.Errorf("download frame: %w", err)
	}

	jpeg, err := recorder.H264ToJPEG(ctx, annexB)
	if err != nil {
		return fmt.Errorf("jpeg decode: %w", err)
	}

	key, err := u.storage.UploadFrameJPEG(ctx, event.ScheduleID, event.StreamID, event.SequenceNo, jpeg)
	if err != nil {
		return fmt.Errorf("upload jpeg: %w", err)
	}

	url, err := u.storage.PresignFrame(ctx, key, u.storage.PresignExpiry())
	if err != nil {
		url = key
	}
	u.broadcaster.PublishFrameURL(ctx, event.ScheduleID, event.StreamID, event.StreamType, url, event.SequenceNo)
	return nil
}