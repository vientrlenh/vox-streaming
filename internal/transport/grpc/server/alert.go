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

// PushAlert nhận cảnh báo từ AI service.
//
// participantId CỐ Ý không bắt buộc. Bên phát không phải lúc nào cũng biết thí sinh nào: đường WS
// của bài thi và đường WebRTC nối thẳng từ app thi chỉ cầm mỗi exam attempt id, nên chúng gửi rỗng
// đúng như hợp đồng đã thoả thuận -- "không biết thì để rỗng, đừng lấy id khác điền vào", vì một id
// bịa thì service này không phân biệt nổi với id thật và sẽ gắn vi phạm sang hồ sơ người khác.
// enrichAlertIdentity ngay phía sau tra bù từ session registry, nơi duy nhất biết ánh xạ đó.
//
// Bắt buộc trường này từng làm mọi cảnh báo WINDOW_FOCUS_LOST bị chặn ngay ở cửa, và chặn im lặng:
// hàm return trước mọi lệnh log nên phía streaming không để lại dấu vết nào, còn phía gọi chỉ thấy
// một InvalidArgument chung chung. Đó là lý do nhánh từ chối bên dưới có log riêng.
func (s *AlertServer) PushAlert(ctx context.Context, req *alertv1.PushAlertRequest) (*alertv1.PushAlertResponse, error) {
	if req.SessionId == "" || req.AlertType == "" {
		s.logger.Warn("alert rejected: thiếu trường bắt buộc",
			zap.String("sessionId", req.SessionId),
			zap.String("alertType", req.AlertType),
			zap.String("participantId", req.ParticipantId),
			zap.String("streamId", req.StreamId),
		)
		return nil, status.Error(codes.InvalidArgument, "sessionId, alertType are required")
	}

	capturedAt := time.Now().UTC()
	if req.CapturedAtMs > 0 {
		capturedAt = time.UnixMilli(req.CapturedAtMs).UTC()
	}
	alert := domain.AlertEvent{
		Source: domain.AlertSourceAI, 
		SessionID: req.SessionId, 
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
			zap.String("sessionId", req.SessionId), 
			zap.String("alertType", req.AlertType), 
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "alert service temporary unavailable")
	}

	s.logger.Info("ai alert published", 
		zap.String("sessionId", req.SessionId),  
		zap.String("alertType", req.AlertType), 
		zap.String("detail", req.Detail),
		zap.Float32("confidence", req.Confidence),
	)


	return &alertv1.PushAlertResponse{Received: true}, nil
}