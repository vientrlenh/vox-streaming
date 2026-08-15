package server

import (
	"context"
	"testing"

	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	alertv1 "github.com/vientrlenh/vox-streaming/pkg/pb/alert/v1"
	"go.uber.org/zap"
)

type fakeScanner struct{ sessions []cache.SessionInfo }

func (f *fakeScanner) ScanSession(ctx context.Context, sessionID string) ([]cache.SessionInfo, error) {
	return f.sessions, nil
}
func (f *fakeScanner) ScanSchedule(ctx context.Context, scheduleID string) ([]cache.SessionInfo, error) {
	return nil, nil
}
func (f *fakeScanner) ScanAll(ctx context.Context) ([]cache.SessionInfo, error) { return nil, nil }

type fakeEventer struct{ live []domain.AlertEvent }

func (f *fakeEventer) PublishAlertEvent(ctx context.Context, sessionID string, a domain.AlertEvent) error {
	f.live = append(f.live, a)
	return nil
}
func (f *fakeEventer) SubscribeAlerts(ctx context.Context, sessionID string) <-chan domain.AlertEvent {
	return nil
}
func (f *fakeEventer) PublishParticipantEvent(ctx context.Context, scheduleID string, e domain.ParticipantEvent) {
}
func (f *fakeEventer) SubscribeEvents(ctx context.Context, scheduleID string) <-chan domain.ParticipantEvent {
	return nil
}

type fakePublisher struct{ durable []domain.AlertRaisedEvent }

func (f *fakePublisher) PublishAlertRaised(ctx context.Context, e domain.AlertRaisedEvent) error {
	f.durable = append(f.durable, e)
	return nil
}

// participantId rỗng phải được NHẬN rồi tra bù, không được từ chối.
//
// Hợp đồng này đã hỏng hai lần theo hai cách khác nhau, nên nó đáng có test: bên phát cố ý gửi rỗng
// khi không biết thí sinh nào (đường WS bài thi, đường WebRTC nối thẳng từ app thi), còn lớp
// transport thì từng bắt buộc trường này và chặn im lặng toàn bộ WINDOW_FOCUS_LOST.
func TestPushAlertAcceptsEmptyParticipant(t *testing.T) {
	const session = "11111111-1111-4111-8111-111111111111"

	scanner := &fakeScanner{sessions: []cache.SessionInfo{{
		SessionID: session, ParticipantID: "cand-1", StreamID: "stream-cam", StreamType: "camera",
	}}}
	eventer := &fakeEventer{}
	publisher := &fakePublisher{}

	mu := usecase.NewMonitorUseCase(scanner, eventer, eventer, publisher, zap.NewNop())
	srv := NewAlertServer(mu, zap.NewNop())

	// Đúng thứ Python gửi cho WINDOW_FOCUS_LOST: chỉ có session id, hai id kia để rỗng.
	resp, err := srv.PushAlert(context.Background(), &alertv1.PushAlertRequest{
		SessionId:  session,
		AlertType:  domain.AlertWindowFocusLost,
		Confidence: 1.0,
	})
	if err != nil {
		t.Fatalf("participantId rỗng vẫn bị từ chối: %v", err)
	}
	if !resp.Received {
		t.Fatal("không được ack")
	}

	if len(publisher.durable) != 1 {
		t.Fatalf("nhánh durable phải có đúng 1 cảnh báo, có %d", len(publisher.durable))
	}
	got := publisher.durable[0]
	t.Logf("durable: sessionId=%s participantId=%s streamId=%s streamType=%s level=%s",
		got.SessionID, got.ParticipantID, got.StreamID, got.StreamType, got.Level)

	if got.ParticipantID != "cand-1" {
		t.Errorf("participantId phải được tra bù = cand-1, got %q", got.ParticipantID)
	}
	if got.StreamID != "stream-cam" {
		t.Errorf("streamId phải được tra bù = stream-cam, got %q", got.StreamID)
	}
	if got.Level != domain.AlertLevelWarning {
		t.Errorf("level phải là WARNING, got %q", got.Level)
	}
	if len(eventer.live) != 1 {
		t.Errorf("nhánh live phải có 1 cảnh báo, có %d", len(eventer.live))
	}

	// sessionId rỗng thì vẫn phải bị từ chối.
	if _, err := srv.PushAlert(context.Background(), &alertv1.PushAlertRequest{
		AlertType: domain.AlertWindowFocusLost,
	}); err == nil {
		t.Error("thiếu sessionId mà vẫn được nhận")
	}
}
