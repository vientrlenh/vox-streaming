package webrtc

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	grpcclient "github.com/vientrlenh/vox-streaming/internal/transport/grpc/client"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
)

type SignalMessage struct {
	Type      string                   `json:"type"`
	SDP       string                   `json:"sdp,omitempty"`
	Candidate *webrtc.ICECandidateInit `json:"candidate,omitempty"`
}

type Handler struct {
	peerCfg   PeerConfig
	sessions  *SessionManager
	streamUseCase   *usecase.StreamUseCase
	monitorUseCase 	*usecase.MonitorUseCase
	upgrader  websocket.Upgrader
	storage *storage.Client
	segments *cache.SegmentRegistry
	logger    *zap.Logger
	validator *auth.Validator
	broadcaster *RedisBroadcaster
	examClient *grpcclient.ExamClient
}

type MonitorMessage struct {
	Type 	string 			`json:"type"`
	Streams []usecase.StreamInfo `json:"streams,omitempty"`
	Frame *FrameNotification `json:"frame,omitempty"`
	Event *domain.ParticipantEvent `json:"event,omitempty"`
	Alert *domain.AlertEvent `json:"alert,omitempty"`
}

const (
	writeDeadline = 10 * time.Second
	pongWait      = 60 * time.Second
	pingPeriod    = 45 * time.Second
	maxMsgSize    = 64 * 1024
)

func NewHandler(
	peerCfg PeerConfig,
	streamUseCase *usecase.StreamUseCase,
	monitorUseCase *usecase.MonitorUseCase,
	allowedOrigins []string,
	logger *zap.Logger,
	validator *auth.Validator,
	broadcaster *RedisBroadcaster, 
	examClient *grpcclient.ExamClient, 
	storage *storage.Client, 
	segments *cache.SegmentRegistry, 
) *Handler {
	return &Handler{
		peerCfg:  peerCfg,
		sessions: NewSessionManager(),
		streamUseCase:  streamUseCase,
		monitorUseCase: monitorUseCase,
		storage: storage, 
		segments: segments, 
		upgrader: websocket.Upgrader{
			ReadBufferSize:  8192,
			WriteBufferSize: 8192,
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				return slices.Contains(allowedOrigins, origin)
			},
		},
		logger:    logger,
		validator: validator,
		broadcaster: broadcaster,
		examClient: examClient,
	}
}

func (h *Handler) ServeStream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	scheduleID := q.Get("scheduleId")
	streamType := q.Get("streamType")
	token := q.Get("token")

	claims, err := h.validator.Validate(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	if !claims.IsStudent() {
		http.Error(w, "forbbiden: use /ws/monitor for monitoring", http.StatusForbidden)
		return
	}
	if len(claims.ScheduleIDs) != 1 || claims.ScheduleIDs[0] != scheduleID || !claims.CanStream(streamType) {
		http.Error(w, "forbidden: wrong schedule", http.StatusForbidden)
		return
	}

	if h.examClient != nil {
		allowed, reason, err := h.examClient.ValidateAccess(r.Context(), scheduleID, claims.UserID, streamType)
		if err != nil {
			h.logger.Warn("exam validation unavaiable, denying", zap.Error(err))
			http.Error(w, "exam service unavaiable", http.StatusServiceUnavailable)
			return
		}
		if !allowed {
			h.logger.Warn("exam access denined", 
				zap.String("reason", reason), 
				zap.String("participantId", claims.UserID),
			)
			http.Error(w, reason, http.StatusForbidden)
			return
		}
	}

	rawConn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}
	conn := &safeConn{
		conn: rawConn,
	}
	defer rawConn.Close()

	peer, err := NewPeer(h.peerCfg, scheduleID, claims.SessionID, claims.UserID, streamType, h.streamUseCase, h.monitorUseCase, h.storage, h.segments, h.logger)
	if err != nil {
		h.logger.Error("peer creation failed", zap.Error(err))
		_ = rawConn.WriteJSON(map[string]string{
			"type":    "error",
			"message": "server error",
		})
		return
	}

	if old := h.sessions.Replace(scheduleID, claims.UserID, streamType, peer); old != nil {
		old.Close() // explicit close, clear ownership
		h.logger.Info("replaced existing peer on reconnect", 
			zap.String("schedule_id", scheduleID), 
			zap.String("participant_id", claims.UserID), 
			zap.String("stream_type", streamType),
		)
	}

	defer func() {
		h.sessions.RemoveIfSame(scheduleID, claims.UserID, streamType, peer)
		peer.Close()
	}()

	peer.pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		_ = conn.WriteJSON(SignalMessage{
			Type:      "ice-candidate",
			Candidate: &init,
		})
	})
	h.runSignaling(conn, peer)

}

func (h *Handler) ServeMonitor(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	scheduleID := q.Get("scheduleId")
	token := q.Get("token")

	if scheduleID == "" {
		http.Error(w, "missing scheduleId", http.StatusBadRequest)
		return
	}

	claims, err := h.validator.Validate(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !claims.CanMonitorSchedule(scheduleID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	rawConn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("monitor websocket upgrade failed", zap.Error(err))
		return
	}
	defer rawConn.Close()

	// context riêng để cancel cả 2 subscription khi monitor disconnect
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	conn := &safeConn{conn: rawConn}

	h.logger.Info("monitor connected", zap.String("scheduleId", scheduleID), zap.String("userId", claims.UserID))

	// gửi snapshot ngay khi kết nối - monitor thấy ngay khi ai đó đang online
	snapshot, err := h.monitorUseCase.GetScheduleSnapshot(ctx, scheduleID)
	if err != nil {
		h.logger.Error("get schedule snapshot failed", zap.String("scheduleId", scheduleID), zap.Error(err))
	}
	_ = conn.WriteJSON(MonitorMessage{
		Type: "snapshot", 
		Streams: snapshot,
	})

	frameCh := h.broadcaster.Subscribe(ctx, scheduleID)
	eventCh := h.monitorUseCase.SubscribeEvents(ctx, scheduleID)
	alertCh := h.monitorUseCase.SubscribeAlerts(ctx, scheduleID)

	// Read goroutine dùng để detect disconnect
	go func() {
		defer cancel()
		rawConn.SetReadLimit(maxMsgSize)
		rawConn.SetReadDeadline(time.Now().Add(pongWait))
		rawConn.SetPongHandler(func(string) error {
			rawConn.SetReadDeadline(time.Now().Add(pongWait))
			return nil
		})
		for {
			if _, _, err := rawConn.ReadMessage(); err != nil {
				return
			}
		}
	}()
	
	// ping goroutine
	go func() {
		defer cancel()
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		for {
			select {
			case<-ticker.C:
				conn.mu.Lock()
				conn.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
				err := conn.conn.WriteMessage(websocket.PingMessage, nil)
				conn.mu.Unlock()
				if err != nil {
					return
				}
			case<-ctx.Done():
				return
			}
		}
	}()

	// write loop, dùng cho merge frame và participant events
	for {
		select {
		case<-ctx.Done():
			return
		case notif, ok := <-frameCh:
			if !ok {
				return
			}
			if err := conn.WriteJSON(MonitorMessage{Type: "frame", Frame: &notif}); err != nil {
				return
			}
		case event, ok := <-eventCh:
			if !ok {
				return
			}
			if err := conn.WriteJSON(MonitorMessage{Type: "participant", Event: &event}); err != nil {
				return
			}
		case alert, ok := <-alertCh:
			if !ok {
				return
			}
			if err := conn.WriteJSON(MonitorMessage{Type: "alert", Alert: &alert}); err != nil {
				return
			}
		}
	}
}

func (h *Handler) GetActiveSchedules(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	claims, err := h.validator.Validate(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	schedules, err := h.monitorUseCase.GetActiveSchedules(r.Context(), claims.ScheduleIDs)
	if err != nil {
		h.logger.Error("get active schedules failed", zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(schedules)
}

func (h *Handler) runSignaling(conn *safeConn, peer *Peer) {
	conn.conn.SetReadLimit(maxMsgSize)
	conn.conn.SetReadDeadline(time.Now().Add(pongWait))
	conn.conn.SetPongHandler(func(string) error {
		conn.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Gửi ping định kỳ
	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				conn.mu.Lock()
				conn.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
				err := conn.conn.WriteMessage(websocket.PingMessage, nil)
				conn.mu.Unlock()
				if err != nil {
					return
				}
			case <-peer.done:
				return
			}
		}
	}()

	// Đọc signaling message
	for {
		var msg SignalMessage
		if err := conn.conn.ReadJSON(&msg); err != nil {
			return
		}

		switch msg.Type {
		case "offer":
			answer, err := peer.HandleOffer(msg.SDP)
			if err != nil {
				h.logger.Error("handler offer failed", zap.Error(err))
				return
			}
			if err := conn.WriteJSON(SignalMessage{
				Type: "answer",
				SDP:  answer,
			}); err != nil {
				return
			}
		case "ice-candidate":
			if msg.Candidate != nil {
				peer.AddICECandidate(*msg.Candidate)
			}
		}
	}
}

type safeConn struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (s *safeConn) WriteJSON(v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn.SetWriteDeadline(time.Now().Add(writeDeadline))
	return s.conn.WriteJSON(v)
}
