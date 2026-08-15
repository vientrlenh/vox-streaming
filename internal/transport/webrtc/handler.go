package webrtc

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
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
	hlsFragments *cache.HLSFragmentRegistry
	liveRewindWindow time.Duration
	logger    *zap.Logger
	validator *auth.Validator
	broadcaster *RedisBroadcaster
	examClient *grpcclient.ExamClient
}

type MonitorMessage struct {
	Type 	string 			`json:"type"`
	Streams []usecase.StreamInfo `json:"streams"`
	Frame *FrameNotification `json:"frame,omitempty"`
	Event *domain.ParticipantEvent `json:"event,omitempty"`
	Alert *domain.AlertEvent `json:"alert,omitempty"`
}

// newSnapshotMessage builds a snapshot message whose Streams is never nil, so it
// can never serialize to `"streams":null`.
//
// The invariant lives here rather than at the call site because the nil comes
// from a path that is easy to overlook: GetScheduleSnapshot returns nil together
// with an error, and the caller deliberately keeps going (a monitor that gets no
// snapshot at all is worse than one that starts empty). Every future caller gets
// the guarantee for free.
func newSnapshotMessage(streams []usecase.StreamInfo) MonitorMessage {
	if streams == nil {
		streams = []usecase.StreamInfo{}
	}
	return MonitorMessage{Type: "snapshot", Streams: streams}
}

const (
	writeDeadline = 10 * time.Second
	pongWait      = 60 * time.Second
	pingPeriod    = 45 * time.Second
	maxMsgSize    = 64 * 1024

	// How often a connected monitor is re-sent the full picture of who is live.
	//
	// Everything else a monitor learns after connecting arrives as a delta over Redis pub/sub, which
	// is fire-and-forget: a subscriber that is momentarily behind, a Redis blip, a Kafka consumer
	// rebalance, and that delta is gone with nothing to replay it. Deltas alone therefore let a
	// monitor drift from reality and never come back -- a tile frozen for the rest of the exam
	// because the one "left" event that would have retired it was dropped.
	//
	// A periodic snapshot makes that drift self-healing: whatever was missed is corrected within one
	// interval, without the client having to detect anything. The interval is a Redis SCAN per
	// monitor, so it is deliberately slower than the frame cadence -- this is a reconciliation
	// backstop, not the primary path.
	monitorSnapshotPeriod = 15 * time.Second
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
	hlsFragments *cache.HLSFragmentRegistry,
	liveRewindWindow time.Duration,
) *Handler {
	return &Handler{
		peerCfg:  peerCfg,
		sessions: NewSessionManager(),
		streamUseCase:  streamUseCase,
		monitorUseCase: monitorUseCase,
		storage: storage,
		segments: segments,
		hlsFragments: hlsFragments,
		liveRewindWindow: liveRewindWindow,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  8192,
			WriteBufferSize: 8192,
			// An absent Origin is allowed; a present one must be on the list.
			//
			// Origin is a browser mechanism, and the WPF capture client is not a
			// browser -- it sends no Origin at all, so an exact-match check alone
			// rejects every native client with a bare "request origin not allowed".
			//
			// Allowing the empty case does not weaken the browser protection this
			// check exists for. CSWSH needs a page to reach an endpoint carrying
			// ambient credentials; here the credential is the JWT in the query
			// string, never a cookie, and a page cannot suppress its own Origin
			// header -- so an empty Origin can never have come from one.
			CheckOrigin: func(r *http.Request) bool {
				origin := r.Header.Get("Origin")
				if origin == "" {
					return true
				}
				// "*" here means the same thing it means to the rs/cors middleware
				// this same allowedOrigins slice is also passed to (cmd/server/main.go) --
				// allow any origin. Without this special case, exact-match membership
				// against a literal "*" element never matches a real browser Origin
				// (rejected every real request in prod, confirmed live 2026-08-11:
				// "websocket origin rejected" for https://voxenta.net against
				// allowed=["*"]).
				if slices.Contains(allowedOrigins, "*") || slices.Contains(allowedOrigins, origin) {
					return true
				}
				// gorilla's own rejection names neither the origin it saw nor the list
				// it compared against, so the failure reads identically whether the
				// deployment config is wrong or the client is. Nearly every real case
				// is a near-miss -- trailing slash, http vs https, a port that is
				// present on one side only -- and none of those are visible without
				// printing both sides.
				logger.Warn("websocket origin rejected",
					zap.String("origin", origin),
					zap.Strings("allowed", allowedOrigins),
				)
				return false
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

	claims, err := h.validator.ValidateStream(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	
	if !claims.CanAccess(scheduleID, streamType) {
		http.Error(w, "forbidden: wrong schedule or stream type", http.StatusForbidden)
		return
	}

	participantID := claims.CandidateID

	if h.examClient != nil {
		allowed, reason, err := h.examClient.ValidateAccess(r.Context(), scheduleID, participantID, claims.SessionID, streamType)
		if err != nil {
			h.logger.Warn("exam validation unavaiable, denying", zap.Error(err))
			http.Error(w, "exam service unavaiable", http.StatusServiceUnavailable)
			return
		}
		if !allowed {
			h.logger.Warn("exam access denined", 
				zap.String("reason", reason), 
				zap.String("participantId", participantID),
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

	peer, err := NewPeer(h.peerCfg, scheduleID, claims.SessionID, participantID, streamType, h.streamUseCase, h.monitorUseCase, h.storage, h.segments, h.hlsFragments, h.logger)
	if err != nil {
		h.logger.Error("peer creation failed", zap.Error(err))
		_ = rawConn.WriteJSON(map[string]string{
			"type":    "error",
			"message": "server error",
		})
		return
	}

	if old := h.sessions.Replace(scheduleID, participantID, streamType, peer); old != nil {
		old.Close() // explicit close, clear ownership
		h.logger.Info("replaced existing peer on reconnect", 
			zap.String("scheduleId", scheduleID), 
			zap.String("participantId", participantID), 
			zap.String("streamType", streamType),
		)
	}

	defer func() {
		h.sessions.RemoveIfSame(scheduleID, participantID, streamType, peer)
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

	claims, err := h.validator.ValidateMonitor(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !claims.CanMonitorSchedule(scheduleID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if claims.ExpiresAt == nil || !claims.ExpiresAt.After(time.Now()) {
		http.Error(w, "token expired", http.StatusUnauthorized)
		return
	}

	rawConn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("monitor websocket upgrade failed", zap.Error(err))
		return
	}
	defer rawConn.Close()

	// context riêng để cancel cả 2 subscription khi monitor disconnect
	
	if claims.ExpiresAt == nil {
		http.Error(w, "invalid token expiration", http.StatusUnauthorized)
		return
	}

	ctx, cancel := context.WithDeadline(r.Context(), claims.ExpiresAt.Time)
	defer cancel()

	conn := &safeConn{conn: rawConn}

	h.logger.Info("monitor connected", zap.String("scheduleId", scheduleID), zap.String("userId", claims.UserID))

	// gửi snapshot ngay khi kết nối - monitor thấy ngay khi ai đó đang online
	snapshot, err := h.monitorUseCase.GetScheduleSnapshot(ctx, scheduleID)
	if err != nil {
		h.logger.Error("get schedule snapshot failed", zap.String("scheduleId", scheduleID), zap.Error(err))
	}
	_ = conn.WriteJSON(newSnapshotMessage(snapshot))

	frameCh := h.broadcaster.Subscribe(ctx, scheduleID)
	eventCh := h.monitorUseCase.SubscribeEvents(ctx, scheduleID)

	// Alert được publish theo sessionID (xem MonitorUseCase.PublishAlert), nhưng một màn hình
	// giám sát theo dõi cả scheduleID - có thể nhiều thí sinh cùng lúc. alertCh gộp alert của
	// tất cả session đang biết vào một channel duy nhất; addAlertSource cho phép thêm session mới
	// (thí sinh vào phòng sau khi giám thị đã kết nối) mà không cần dựng lại fan-in.
	alertCh, addAlertSource := newAlertFanIn(ctx)
	seenSessions := make(map[string]struct{})
	subscribeSession := func(sessionID string) {
		if sessionID == "" {
			return
		}
		if _, exists := seenSessions[sessionID]; exists {
			return
		}
		seenSessions[sessionID] = struct{}{}
		addAlertSource(h.monitorUseCase.SubscribeAlerts(ctx, sessionID))
	}
	for _, stream := range snapshot {
		subscribeSession(stream.SessionID)
	}

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

	snapshotTicker := time.NewTicker(monitorSnapshotPeriod)
	defer snapshotTicker.Stop()

	// write loop, dùng cho merge frame và participant events
	for {
		select {
		case<-ctx.Done():
			return
		case <-snapshotTicker.C:
			fresh, err := h.monitorUseCase.GetScheduleSnapshot(ctx, scheduleID)
			if err != nil {
				// Skip this round rather than closing: a transient Redis error should cost the
				// monitor one reconciliation, not its live connection.
				h.logger.Warn("periodic schedule snapshot failed",
					zap.String("scheduleId", scheduleID),
					zap.Error(err),
				)
				continue
			}
			// Also repairs alert routing, not just the tile grid. Alert subscriptions are opened
			// per exam session off the snapshot and the 'joined' events; a 'joined' that never
			// arrives means that student's alerts had nowhere to go for the rest of the exam.
			for _, stream := range fresh {
				subscribeSession(stream.SessionID)
			}
			if err := conn.WriteJSON(newSnapshotMessage(fresh)); err != nil {
				return
			}
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
			if event.Type == domain.ParticipantJoined {
				subscribeSession(event.SessionID)
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
	claims, err := h.validator.ValidateMonitor(token)
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

// authorizeMonitor validates a monitor token off the query string and checks it
// covers scheduleID, writing the error response itself and reporting whether the
// caller may proceed. Shared by both live-rewind endpoints so they can never
// drift apart on who is allowed to read a stream.
func (h *Handler) authorizeMonitor(w http.ResponseWriter, r *http.Request, scheduleID string) bool {
	claims, err := h.validator.ValidateMonitor(r.URL.Query().Get("token"))
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if !claims.CanMonitorSchedule(scheduleID) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	return true
}

// GetLiveManifest serves a freshly-built HLS media playlist for the live-rewind
// window of an in-progress stream. Only currently-live streams get a manifest
// (checked via MonitorUseCase.FindLiveStream, Redis-backed and instance-
// agnostic) — once a stream ends, monitors should fall back to the final
// assembled recording's own playback endpoint instead. Note that GetLiveAsset
// deliberately does NOT repeat that liveness check; see its own comment.
func (h *Handler) GetLiveManifest(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("scheduleId")
	streamID := r.PathValue("streamId")

	if !h.authorizeMonitor(w, r, scheduleID) {
		return
	}

	if h.hlsFragments == nil {
		http.Error(w, "live rewind not available", http.StatusNotFound)
		return
	}

	info, err := h.monitorUseCase.FindLiveStream(r.Context(), scheduleID, streamID)
	if err != nil {
		h.logger.Error("find live stream failed", zap.String("scheduleId", scheduleID), zap.String("streamId", streamID), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	if info == nil {
		http.Error(w, "stream is not live", http.StatusNotFound)
		return
	}

	inits, err := h.hlsFragments.ListInits(r.Context(), streamID)
	if err != nil {
		h.logger.Error("list hls init segments failed", zap.String("streamId", streamID), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	frags, err := h.hlsFragments.ListFragments(r.Context(), streamID)
	if err != nil {
		h.logger.Error("list hls fragments failed", zap.String("streamId", streamID), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Playlist-relative asset names, resolved by the browser against this
	// playlist's own URL — which keeps the manifest identical in proxy mode
	// (behind nginx) and direct mode. Each carries a compact per-stream
	// signature rather than the monitor JWT: a playlist URI does not inherit the
	// playlist's query string and native players can't add one, so whatever goes
	// here is repeated on every one of a multi-hour DVR's thousands of lines.
	// See auth.Validator.SignAsset.
	sig := url.QueryEscape(h.validator.SignAsset(streamID))
	assetURI := func(name string) string {
		return name + "?sig=" + sig
	}

	manifest, err := buildLiveManifest(inits, frags, h.liveRewindWindow, assetURI)
	if err != nil {
		h.logger.Warn("build live manifest failed, no fragments ready yet", zap.String("streamId", streamID), zap.Error(err))
		http.Error(w, "live rewind not ready yet", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Cache-Control", "no-store")
	// A full-length DVR playlist is thousands of lines sharing one byte-identical
	// signature suffix, so gzip takes it down by roughly an order of magnitude —
	// worth it on a response re-fetched every few seconds for as long as anyone
	// is watching.
	writeMaybeGzipped(w, r, manifest)
}

// GetLiveAsset resolves one stable, playlist-relative HLS asset name back to
// its stored object and redirects to a freshly presigned URL for it.
//
// Authorization is the per-stream signature the manifest embedded (minted only
// after a full monitor-token check there), or a monitor token directly — the
// latter so the endpoint stays usable by hand and by any client that would
// rather pass its own credential.
//
// Deliberately does NOT check stream liveness the way GetLiveManifest does:
// FindLiveStream is a Redis SCAN over the keyspace, which is far too expensive
// to repeat for every fragment fetch, and a fragment already listed in a
// manifest the client holds must stay fetchable for a few seconds after the
// stream ends rather than turning into a mid-playback 404.
func (h *Handler) GetLiveAsset(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("scheduleId")
	streamID := r.PathValue("streamId")
	assetName := r.PathValue("asset")

	// Additive, not either/or: a signature that has aged out falls through to the
	// token check rather than hard-failing, which is what lets a client holding a
	// stale playlist (rewinding through fragments it listed a while ago) keep
	// fetching as long as it still has a valid monitor token.
	authorized := false
	if sig := r.URL.Query().Get("sig"); sig != "" {
		authorized = h.validator.VerifyAsset(streamID, sig) == nil
	}
	if !authorized && !h.authorizeMonitor(w, r, scheduleID) {
		return
	}
	if h.hlsFragments == nil || h.storage == nil {
		http.Error(w, "live rewind not available", http.StatusNotFound)
		return
	}

	ref, err := parseHLSAssetName(assetName)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	var s3Key string
	if ref.IsInit {
		meta, err := h.hlsFragments.GetInit(r.Context(), streamID, ref.Epoch)
		if err != nil {
			h.logger.Error("get hls init segment failed", zap.String("streamId", streamID), zap.Int("epoch", ref.Epoch), zap.Error(err))
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if meta == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s3Key = meta.S3Key
	} else {
		meta, err := h.hlsFragments.GetFragment(r.Context(), streamID, ref.Seq)
		if err != nil {
			h.logger.Error("get hls fragment failed", zap.String("streamId", streamID), zap.Int64("seq", ref.Seq), zap.Error(err))
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if meta == nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		s3Key = meta.S3Key
	}

	// The stored key embeds the schedule the asset was uploaded under (see
	// storage.hlsFragmentKey), so this pins the asset to the schedule the token
	// actually authorizes: guessing another schedule's streamId gets a 404
	// rather than that schedule's media.
	if !strings.HasPrefix(s3Key, "schedules/"+scheduleID+"/") {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	signed, err := h.storage.PresignRecording(r.Context(), s3Key, h.storage.PresignExpiry())
	if err != nil {
		h.logger.Error("presign hls asset failed", zap.String("streamId", streamID), zap.String("s3Key", s3Key), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// Fragments are immutable once written, so a rewinding client may re-fetch
	// the same one; cache well inside the presign expiry so the redirect target
	// can never be replayed after it stops working.
	w.Header().Set("Cache-Control", "private, max-age=60")
	http.Redirect(w, r, signed, http.StatusFound)
}

// writeMaybeGzipped writes body, compressing it when the client advertised
// gzip. Headers are all set before the first write, as net/http requires.
func writeMaybeGzipped(w http.ResponseWriter, r *http.Request, body string) {
	if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		_, _ = io.WriteString(w, body)
		return
	}
	w.Header().Set("Content-Encoding", "gzip")
	w.Header().Add("Vary", "Accept-Encoding")
	gz := gzip.NewWriter(w)
	defer gz.Close()
	_, _ = io.WriteString(gz, body)
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

// newAlertFanIn gộp nhiều nguồn domain.AlertEvent vào một channel duy nhất. Trả về channel gộp
// và một hàm add để nạp thêm nguồn về sau (dùng khi có thí sinh mới vào phòng sau khi monitor đã
// kết nối) - vì số lượng nguồn không cố định ngay từ đầu, out không bao giờ được close tường minh;
// mọi goroutine (kể cả những goroutine được add() sau) tự thoát qua ctx.Done(), và select phía
// ServeMonitor đã có nhánh ctx.Done() riêng nên không cần out đóng lại mới dừng được.
func newAlertFanIn(ctx context.Context) (<-chan domain.AlertEvent, func(<-chan domain.AlertEvent)) {
	out := make(chan domain.AlertEvent, 32)

	add := func(input <-chan domain.AlertEvent) {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case alert, ok := <-input:
					if !ok {
						return
					}

					select {
					case out <- alert:
					case <-ctx.Done():
						return
					}
				}
			}
		}()
	}

	return out, add
}
