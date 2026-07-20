package server

import (
	"context"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	alertv1 "github.com/vientrlenh/vox-streaming/pkg/pb/alert/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AlertServer struct {
	alertv1.UnimplementedAlertServiceServer
	monitorUseCase *usecase.MonitorUseCase
	logger         *zap.Logger
}

func NewAlertServer(mu *usecase.MonitorUseCase, logger *zap.Logger) *AlertServer {
	return &AlertServer{
		monitorUseCase: mu,
		logger:         logger,
	}
}

func (s *AlertServer) PushAlert(ctx context.Context, req *alertv1.PushAlertRequest) (*alertv1.PushAlertResponse, error) {
	if req.ScheduleId == "" || req.ParticipantId == "" || req.AlertType == "" {
		return nil, status.Error(codes.InvalidArgument, "scheduleId, participantId, alertType are required")
	}

	capturedAt := time.Now().UTC()
	if req.CapturedAtMs > 0 {
		capturedAt = time.UnixMilli(req.CapturedAtMs).UTC()
	}
	alert := domain.AlertEvent{
		Source: domain.AlertSourceAI, 
		ScheduleID: req.ScheduleId, 
		ParticipantID: req.ParticipantId, 
		StreamID: req.StreamId, 
		StreamType: req.StreamType,
		AlertType: req.AlertType, 
		Detail: req.Detail, 
		Confidence: float64(req.Confidence), 
		SequenceNo: req.SequenceNo,
		CapturedAt: capturedAt,
	}
	if err := s.monitorUseCase.PublishAlert(ctx, alert, req.EventId); err != nil {
		s.logger.Error("publish alert failed", 
			zap.String("scheduleId", req.ScheduleId), 
			zap.String("alertType", req.AlertType), 
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "alert service temporary unavailable")
	}

	s.logger.Info("ai alert published", 
		zap.String("scheduleId", req.ScheduleId),  
		zap.String("alertType", req.AlertType), 
		zap.String("detail", req.Detail),
		zap.Float32("confidence", req.Confidence),
	)


	return &alertv1.PushAlertResponse{Received: true}, nil
}