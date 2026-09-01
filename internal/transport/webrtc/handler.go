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
	// StreamID is sent on "stream-ready"/"stream-resumed" so the client knows which stream it is
	// currently on, and can hand it back as ?resumeStreamId= if its signaling socket drops. Empty
	// on every other message type.
	StreamID string `json:"streamId,omitempty"`
}

// Signaling message types sent by the server to tell a client which stream it is attached to.
//
//	stream-ready   -- this connection owns a NEW stream with this id.
//	stream-resumed -- this connection re-attached to the stream it named, which kept running.
//
// A client that ignores both keeps working exactly as before; it just never resumes and gets a new
// stream id on every reconnect, which is what every client did before continuity existed.
const (
	msgStreamReady   = "stream-ready"
	msgStreamResumed = "stream-resumed"
)

// msgBye is sent by the CLIENT, the only direction that knows the difference between a socket that
// broke and a stream that is over. See its handler in runSignaling.
const msgBye = "bye"

type Handler struct {
	peerCfg   PeerConfig
	sessions  *SessionManager
	streamUseCase   *usecase.StreamUseCase
	monitorUseCase 	*usecase.MonitorUseCase
	upgrader  websocket.Upgrader
	storage *storage.Client
	segments *cache.SegmentRegistry
	// Armed the moment a peer is accepted, so a WebRTC recording gets assembled even if nothing ever
	// processes its stream.ended -- see the arming site in Signal for what that protects against.
	pendingAssembly *cache.PendingAssemblyRegistry
	// Every stream this schedule has had, alive or finished -- the live session registry only knows
	// the former. See GetScheduleStreams.
	scheduleStreams *cache.ScheduleStreamRegistry
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
	pendingAssembly *cache.PendingAssemblyRegistry,
	scheduleStreams *cache.ScheduleStreamRegistry,
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
		pendingAssembly: pendingAssembly,
		scheduleStreams: scheduleStreams,
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
				if slices.Contains(allowedOrigins, origin) {
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

// How long after media can last arrive before the watchdog may assemble a WebRTC stream. Matches the
// upload path's 30m credential grace + 2m watchdog grace (see handler/segment) so the two recording
// sources are not salvaged on different schedules for no reason.
const webrtcAssemblyGrace = 32 * time.Minute

// webrtcAssemblyDueAt is the earliest moment this stream provably cannot grow any further, and
// therefore the earliest it is safe to let the watchdog assemble it. Assembly is effectively
// one-shot -- AssemblerUseCase.assemble short-circuits on an existing recording.mp4 -- so a due time
// that lands mid-exam does not merely produce a short recording, it permanently prevents the complete
// one from ever being built. Erring late costs a delayed recording; erring early destroys one.
//
// The token's own exp is not the answer by itself: it is how often permission gets re-checked, not
// how long the exam runs. Anchoring on time.Now() first means an already-stale token cannot produce a
// due time in the past and fire the watchdog on a stream that is still live.
func webrtcAssemblyDueAt(claims *auth.StreamClaims) time.Time {
	windowEnd := time.Now().UTC()
	if claims.ExpiresAt != nil && claims.ExpiresAt.Time.UTC().After(windowEnd) {
		windowEnd = claims.ExpiresAt.Time.UTC()
	}
	if scheduleEnd, ok := claims.ScheduleEnd(); ok && scheduleEnd.After(windowEnd) {
		windowEnd = scheduleEnd
	}
	return windowEnd.Add(webrtcAssemblyGrace)
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

	// A reconnecting client names the stream it was on. If that stream is still alive, take over its
	// signaling instead of minting a new one -- see adoptPeer for why this is the whole point.
	if resumeStreamID := q.Get("resumeStreamId"); resumeStreamID != "" {
		if adopted := h.adoptPeer(conn, scheduleID, participantID, streamType, resumeStreamID); adopted {
			return
		}
		// Fall through and start a fresh stream. Nothing is wrong here: the peer aged out of its
		// grace, or ICE failed underneath it, or this is a genuine re-entry. The client finds out
		// which because the stream-ready below carries a different id than the one it asked for.
		h.logger.Info("resume requested but no adoptable peer; starting a new stream",
			zap.String("scheduleId", scheduleID),
			zap.String("participantId", participantID),
			zap.String("streamType", streamType),
			zap.String("requestedStreamId", resumeStreamID),
		)
	}

	peer, err := NewPeer(h.peerCfg, scheduleID, claims.SessionID, participantID, streamType, h.streamUseCase, h.monitorUseCase, h.storage, h.segments, h.hlsFragments, h.logger)
	if err != nil {
		h.logger.Error("peer creation failed", zap.Error(err))
		_ = rawConn.WriteJSON(map[string]string{
			"type":    "error",
			"message": "server error",
		})
		return
	}

	// Arm the assembly watchdog before a single byte of media arrives.
	//
	// The normal trigger is stream.ended -> AssemblerUseCase.OnStreamEnded, but that trigger only
	// exists while the vox-assembler consumer is alive to receive it. On 2026-08-17 that consumer had
	// silently exited hours earlier; four streams published stream.ended into a topic with no reader
	// and nothing ever turned their segments into a recording. This entry depends on neither Kafka nor
	// this process surviving, and OnStreamEnded disarms it once a recording actually exists.
	//
	// Logged rather than fatal: refusing the connection would trade a rare unassembled recording for a
	// certain missing one.
	// Record the stream under its schedule before any media arrives, so a stream that dies in its
	// first seconds is still findable afterwards -- that is precisely the one a proctor goes looking
	// for. Logged rather than fatal for the same reason as the watchdog arm below.
	if err := h.scheduleStreams.Record(r.Context(), cache.ScheduleStream{
		StreamID:      peer.streamID,
		ScheduleID:    scheduleID,
		SessionID:     claims.SessionID,
		ParticipantID: participantID,
		StreamType:    streamType,
		StartedAt:     peer.startedAt,
	}); err != nil {
		h.logger.Error("record schedule stream failed; this stream will vanish from the room after a reload",
			zap.String("streamId", peer.streamID),
			zap.String("scheduleId", scheduleID),
			zap.Error(err),
		)
	}

	if err := h.pendingAssembly.Schedule(r.Context(), cache.PendingAssembly{
		StreamID:      peer.streamID,
		ScheduleID:    scheduleID,
		SessionID:     claims.SessionID,
		ParticipantID: participantID,
		StreamType:    streamType,
		DueAt:         webrtcAssemblyDueAt(claims),
		Source:        cache.AssemblySourceWebRTC,
	}); err != nil {
		h.logger.Error("arm assembly watchdog failed; this webrtc stream will only be assembled if stream.ended is processed",
			zap.String("streamId", peer.streamID),
			zap.String("scheduleId", scheduleID),
			zap.Error(err),
		)
	}

	if old := h.sessions.Replace(scheduleID, participantID, streamType, peer); old != nil {
		h.logger.Info("replaced existing peer on reconnect",
			zap.String("scheduleId", scheduleID),
			zap.String("participantId", participantID),
			zap.String("streamType", streamType),
			zap.String("oldStreamId", old.streamID),
		)
		// Closed in the background, NOT inline: Peer.close drains the previous stream's segment
		// uploads to S3 under a 60s cap (ffmpegUploadDrainTimeout) plus ffmpeg's own stop timeout.
		// Inline, every second of that lands between this student's WebSocket upgrade and the offer
		// exchange in runSignaling below -- the reconnect handshake stalls behind the archival work
		// of the connection it is replacing. That is exactly backwards: the drain is bookkeeping for
		// a stream that has already ended, while the student on the other end is sitting in front of
		// an exam waiting for their camera to come back.
		//
		// Safe to detach because ownership was already transferred by Replace above: the map now
		// points at the new peer, the old peer's own defer uses RemoveIfSame so it cannot evict it,
		// and Peer.close is guarded by sync.Once so this racing with that defer collapses to one
		// teardown.
		//
		// Costs a brief overlap where both peers hold a RecordSem slot. That is the right trade --
		// the slot is released as soon as ffmpeg is confirmed dead (see StopProcess in Peer.close),
		// well before the uploads finish, and the new peer does not ask for a slot until both its
		// tracks have arrived.
		go old.Close()
	}

	// End-of-life bookkeeping now hangs off the PEER's death rather than this request's return.
	//
	// It used to be a defer here, which quietly asserted that the stream ends when the WebSocket
	// ends. That is the assumption this whole change exists to remove: a signaling socket can drop
	// and come back while ICE, the tracks and the recording never faltered, and stamping MarkEnded
	// at the socket's death wrote an end time into the index for a stream that was still running.
	//
	// Firing on peer.done also stamps a TRUER time than the old placement did. done closes at the
	// top of Peer.close, ahead of the segment/HLS drain, so the recorded end time is when the stream
	// actually stopped rather than when its uploads finished minutes later.
	go func() {
		<-peer.done
		h.sessions.RemoveIfSame(scheduleID, participantID, streamType, peer)
		// A fresh context on purpose: r.Context() belongs to the request that opened the stream and
		// is long cancelled by the time a peer that outlived its socket finally closes.
		if err := h.scheduleStreams.MarkEnded(context.Background(), scheduleID, peer.streamID, time.Now().UTC()); err != nil {
			h.logger.Warn("mark schedule stream ended failed",
				zap.String("streamId", peer.streamID),
				zap.Error(err),
			)
		}
	}()

	// Reads the peer's CURRENT signaling connection rather than closing over this one, so candidates
	// gathered after an ICE restart go to whichever socket is attached by then.
	peer.pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c == nil {
			return
		}
		init := c.ToJSON()
		_ = peer.writeSignal(SignalMessage{
			Type:      "ice-candidate",
			Candidate: &init,
		})
	})

	gen, ok := peer.BindSignaling(conn)
	if !ok {
		// Only reachable if the peer died between NewPeer and here.
		h.logger.Warn("peer closed before signaling could bind", zap.String("streamId", peer.streamID))
		peer.Close()
		return
	}
	// Told before any negotiation so the client is holding the id even if the socket dies during the
	// offer exchange -- that early window is exactly when a fragile link tends to fail.
	if err := conn.WriteJSON(SignalMessage{Type: msgStreamReady, StreamID: peer.streamID}); err != nil {
		h.logger.Warn("send stream-ready failed", zap.String("streamId", peer.streamID), zap.Error(err))
	}

	defer h.releaseSignaling(peer, gen)
	h.runSignaling(conn, peer)
}

// adoptPeer re-attaches a reconnecting client to the stream it names, keeping the stream id, the
// ffmpeg recording, the HLS playlist and the segment registry entry it already owns. Reports
// whether the connection was served; false means the caller should start a fresh stream.
//
// Why this exists: ServeStream used to call NewPeer on every upgrade, so a dropped signaling socket
// necessarily produced a new stream id, a new playlist and a split recording -- on the wire,
// disconnected -> left -> joined(new id), which is indistinguishable from the student closing the
// app and re-entering. An ordinary network blip therefore spent a signal that should mean something
// much rarer, and left the proctor's evidence trail cut in two for no reason.
//
// What makes it safe is that the peer's WebRTC connection is untouched: this is the same
// PeerConnection on both ends, so there is no DTLS re-handshake to negotiate. The client keeps its
// own peer across the socket reconnect and follows up with an ICE restart, which runSignaling
// already accepts as an ordinary offer.
//
// Authorization is inherited, not re-derived: scheduleID and streamType were checked against the
// token by the caller and participantID IS the token's candidate id, so the session key can only
// ever address this candidate's own stream. The id match on top of that means a client cannot
// resume a stream it was never on, even one of its own from an earlier attempt.
func (h *Handler) adoptPeer(conn *safeConn, scheduleID, participantID, streamType, resumeStreamID string) bool {
	peer := h.sessions.Get(scheduleID, participantID, streamType)
	if peer == nil || peer.streamID != resumeStreamID || !peer.IsAlive() {
		return false
	}

	gen, ok := peer.BindSignaling(conn)
	if !ok {
		// Lost a race with the peer's own teardown between IsAlive and here.
		return false
	}

	h.logger.Info("adopted existing peer on signaling reconnect",
		zap.String("scheduleId", scheduleID),
		zap.String("participantId", participantID),
		zap.String("streamType", streamType),
		zap.String("streamId", peer.streamID),
	)
	if err := conn.WriteJSON(SignalMessage{Type: msgStreamResumed, StreamID: peer.streamID}); err != nil {
		h.logger.Warn("send stream-resumed failed", zap.String("streamId", peer.streamID), zap.Error(err))
	}

	// No Record, no pendingAssembly.Schedule, no sessions.Replace and no MarkEnded goroutine: this
	// stream is already indexed, already armed, already the registered session, and already has a
	// reaper from the connection that created it. Adoption adds a socket, nothing else.
	defer h.releaseSignaling(peer, gen)
	h.runSignaling(conn, peer)
	return true
}

// releaseSignaling detaches this connection from the peer, leaving the peer alive on its grace
// timer unless a newer connection has already taken it over.
func (h *Handler) releaseSignaling(peer *Peer, gen uint64) {
	if !peer.ReleaseSignaling(gen) {
		// Superseded: another connection adopted this peer while this handler was still unwinding.
		return
	}
	h.logger.Info("signaling detached; peer held for reconnect",
		zap.String("streamId", peer.streamID),
		zap.Duration("grace", signalingGrace),
	)
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

// scheduleStreamResponse mirrors the monitor snapshot's stream shape, plus the one field the
// snapshot cannot carry: when the stream stopped. Field names match so the client can feed both into
// the same reducer.
type scheduleStreamResponse struct {
	StreamID      string  `json:"streamId"`
	StreamType    string  `json:"streamType"`
	ParticipantID string  `json:"participantId"`
	SessionID     string  `json:"sessionId"`
	StartedAt     string  `json:"startedAt"`
	EndedAt       *string `json:"endedAt,omitempty"`
}

// GetScheduleStreams lists every stream this schedule has had, finished ones included.
//
// The websocket snapshot answers "who is on air right now", which is what the live grid needs and
// the wrong answer for a proctor who just reloaded the page: every student who had already dropped
// disappears from the room, and with them the way to reach footage that is still retained. This is
// the durable counterpart, read once on mount to seed the grid before the socket takes over.
//
// Bounded by the index's own retention, which mirrors the fragments' -- so a stream listed here can
// always be played, and one whose footage has expired is not offered in the first place.
func (h *Handler) GetScheduleStreams(w http.ResponseWriter, r *http.Request) {
	scheduleID := r.PathValue("scheduleId")
	if !h.authorizeMonitor(w, r, scheduleID) {
		return
	}

	streams, err := h.scheduleStreams.List(r.Context(), scheduleID)
	if err != nil {
		h.logger.Error("list schedule streams failed", zap.String("scheduleId", scheduleID), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	response := make([]scheduleStreamResponse, 0, len(streams))
	for _, stream := range streams {
		item := scheduleStreamResponse{
			StreamID:      stream.StreamID,
			StreamType:    stream.StreamType,
			ParticipantID: stream.ParticipantID,
			SessionID:     stream.SessionID,
			StartedAt:     stream.StartedAt.UTC().Format(time.RFC3339Nano),
		}
		if stream.EndedAt != nil {
			endedAt := stream.EndedAt.UTC().Format(time.RFC3339Nano)
			item.EndedAt = &endedAt
		}
		response = append(response, item)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
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

	// Not being live is a MODE, not a refusal.
	//
	// This used to 404 the moment the peer closed, which threw away the whole point of retaining
	// fragments: a student disconnects, the proctor goes to look at what just happened, and the
	// playlist reports "stream is not live" while every fragment of the footage is still sitting in
	// Redis for another day. Note that GetLiveAsset below never had this gate, so the fragments were
	// already being served -- only the one file that indexes them refused. Removing it widens
	// nothing: authorizeMonitor above is the actual access check.
	info, err := h.monitorUseCase.FindLiveStream(r.Context(), scheduleID, streamID)
	if err != nil {
		h.logger.Error("find live stream failed", zap.String("scheduleId", scheduleID), zap.String("streamId", streamID), zap.Error(err))
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	ended := info == nil

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

	manifest, err := buildLiveManifest(inits, frags, h.liveRewindWindow, ended, assetURI)
	if err != nil {
		// The only way here is "no fragments", which now covers two different situations that deserve
		// the same 404 but not the same reading: a live stream whose first fragment has not landed
		// yet, and a finished stream whose retention window has passed.
		h.logger.Warn("build live manifest failed",
			zap.String("streamId", streamID),
			zap.Bool("ended", ended),
			zap.Error(err),
		)
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
		case msgBye:
			// The one signal that means "this stream is FINISHED", as opposed to "this socket is".
			//
			// Everything else that ends a peer now routes through a grace window, because a socket
			// dying is no longer evidence the student left -- that is the whole point of the resume
			// path. But it left the deliberate case with no way to say so: a clean exam end looked
			// exactly like a network drop, so the peer sat out its grace and was then closed by the
			// ICE-failed branch, which sets closedByFailure. That flag does two things, and both are
			// wrong here: it publishes a confidence-1.0 STREAM_DROPPED alert into the proctoring
			// record of a student who simply finished, and it suppresses MarkComplete, leaving every
			// recording to wait out the full assembly grace before anything would build it.
			//
			// Closing inline and WITHOUT closedByFailure restores what the old unconditional
			// teardown gave for free, while keeping the grace for every exit that is not announced.
			// A client that never sends this is not broken, only slower: it falls back to the grace
			// path, which is exactly where it was before.
			h.logger.Info("client signalled a deliberate stop",
				zap.String("streamId", peer.streamID),
				zap.String("scheduleId", peer.scheduleID),
				zap.String("participantId", peer.participantID),
			)
			peer.Close()
			return
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
