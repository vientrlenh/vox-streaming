package webrtc

import (
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
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
	useCase   *usecase.StreamUseCase
	upgrader  websocket.Upgrader
	logger    *zap.Logger
	validator *auth.Validator
	broadcaster *RedisBroadcaster
}

const (
	writeDeadline = 10 * time.Second
	pongWait      = 60 * time.Second
	pingPeriod    = 45 * time.Second
	maxMsgSize    = 64 * 1024
)

func NewHandler(
	peerCfg PeerConfig,
	useCase *usecase.StreamUseCase,
	allowedOrigins []string,
	logger *zap.Logger,
	validator *auth.Validator,
	broadcaster *RedisBroadcaster,
) *Handler {
	return &Handler{
		peerCfg:  peerCfg,
		sessions: NewSessionManager(),
		useCase:  useCase,
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
	}
}

func (h *Handler) ServeStream(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	roomID := q.Get("room_id")
	streamType := q.Get("stream_type")
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
	if len(claims.RoomIDs) != 1 || claims.RoomIDs[0] != roomID || !claims.CanStream(streamType) {
		http.Error(w, "forbidden: wrong room", http.StatusForbidden)
		return
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

	peer, err := NewPeer(h.peerCfg, roomID, claims.UserID, streamType, h.useCase, h.logger)
	if err != nil {
		h.logger.Error("peer creation failed", zap.Error(err))
		_ = rawConn.WriteJSON(map[string]string{
			"type":    "error",
			"message": "server error",
		})
		return
	}

	h.sessions.Add(roomID, claims.UserID, streamType, peer)
	defer func() {
		h.sessions.Remove(roomID, claims.UserID, streamType)
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
	roomID := q.Get("room_id")
	token := q.Get("token")

	if roomID == "" {
		http.Error(w, "missing room_id", http.StatusBadRequest)
		return
	}

	claims, err := h.validator.Validate(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !claims.CanMonitorRoom(roomID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	rawConn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("monitor websocket upgrade failed", zap.Error(err))
		return
	}
	defer rawConn.Close()

	ch := h.broadcaster.Subscribe(r.Context(), roomID)

	h.logger.Info("monitor connected", zap.String("room_id", roomID), zap.String("user_id", claims.UserID))

	for notif := range ch {
		rawConn.SetWriteDeadline(time.Now().Add(writeDeadline))
		if err := rawConn.WriteJSON(notif); err != nil {
			return
		}
	}
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
