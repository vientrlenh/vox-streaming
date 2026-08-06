package segment

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/stream"
	"github.com/vientrlenh/vox-streaming/internal/transport/api"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
)

const (
	maxSegmentSize     = 50 << 20 // 50 MB
	maxSegmentDuration = 2 * time.Minute
	maxCreateBodySize  = 4 << 10 // 4 KB

	// A whole exam's inventory in one document: a few hundred entries of a couple of hundred bytes.
	// The generous ceiling is a guard against abuse, not an expected size.
	maxInventoryBodySize = 2 << 20 // 2 MB
	maxInventorySegments = 5000

	// How long after an upload session expires the watchdog should assemble it. The credential is
	// already dead at ExpiresAt, so nothing can be added past that point; this only absorbs clock
	// skew between the session's own TTL and the sweep that acts on it.
	assemblyWatchdogGrace = 2 * time.Minute

	// How long an upload credential outlives the window it belong to. This is a data recovery window, not an authorization window: the client may have hours of buffered segments from a machine that was offline, and 410 Gone here means that evidence is gone for good
	uploadCredentialGrade = 30 * time.Minute
)

type SegmentHandler struct {
	useCase         *usecase.SegmentUseCase
	publisher       domain.EventPublisher
	validator       *auth.Validator
	sessionRegistry *cache.SessionRegistry
	pendingAssembly *cache.PendingAssemblyRegistry
	logger          *zap.Logger
}

func NewSegmentHandler(uc *usecase.SegmentUseCase, publisher domain.EventPublisher, v *auth.Validator, sr *cache.SessionRegistry, pa *cache.PendingAssemblyRegistry, logger *zap.Logger) *SegmentHandler {
	return &SegmentHandler{
		useCase:         uc,
		publisher:       publisher,
		validator:       v,
		sessionRegistry: sr,
		pendingAssembly: pa,
		logger:          logger,
	}
}

// Upload handles PUT /stream/sessions/{streamId}/segments/{seq}.
// The body must contain one MP4 segment.
func (h *SegmentHandler) Upload(w http.ResponseWriter, r *http.Request) {
	uploadToken, err := bearerToken(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	streamID := r.PathValue("streamId")
	seqStr := r.PathValue("seq")
	if _, err := uuid.Parse(streamID); err != nil {
		http.Error(w, "invalid streamId", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	startedAtStr := q.Get("startedAt")
	endedAtStr := q.Get("endedAt")

	if seqStr == "" || startedAtStr == "" || endedAtStr == "" {
		http.Error(w, "missing required params", http.StatusBadRequest)
		return
	}

	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil || seq < 0 {
		http.Error(w, "invalid seq", http.StatusBadRequest)
		return
	}
	startedAt, err := time.Parse(time.RFC3339, startedAtStr)
	if err != nil {
		http.Error(w, "invalid startedAt", http.StatusBadRequest)
		return
	}
	endedAt, err := time.Parse(time.RFC3339, endedAtStr)
	if err != nil {
		http.Error(w, "invalid endedAt", http.StatusBadRequest)
		return
	}
	if !endedAt.After(startedAt) || endedAt.Sub(startedAt) > maxSegmentDuration {
		http.Error(w, "invalid segment time range", http.StatusBadRequest)
		return
	}

	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "video/mp4" {
		http.Error(w, "content type must be video/mp4", http.StatusUnsupportedMediaType)
		return
	}
	if r.ContentLength > maxSegmentSize {
		http.Error(w, "segment too large", http.StatusRequestEntityTooLarge)
		return
	}

	data, err := io.ReadAll(io.LimitReader(r.Body, maxSegmentSize+1))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(data) > maxSegmentSize {
		http.Error(w, "segment too large", http.StatusRequestEntityTooLarge)
		return
	}
	if len(data) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	checksum := strings.ToLower(strings.TrimSpace(r.Header.Get("X-Segment-SHA256")))
	expectedHash, err := hex.DecodeString(checksum)
	if err != nil || len(expectedHash) != sha256.Size {
		http.Error(w, "invalid X-Segment-SHA256", http.StatusBadRequest)
		return
	}
	actualHash := sha256.Sum256(data)
	if !strings.EqualFold(checksum, hex.EncodeToString(actualHash[:])) {
		http.Error(w, "segment checksum mismatch", http.StatusUnprocessableEntity)
		return
	}

	req := usecase.SegmentUploadRequest{
		StreamID:    streamID,
		UploadToken: uploadToken,
		Seq:         seq,
		StartedAt:   startedAt,
		EndedAt:     endedAt,
		SHA256:      checksum,
		Data:        data,
	}

	if err := h.useCase.Upload(r.Context(), req); err != nil {
		h.logger.Warn("segment upload failed",
			zap.String("streamId", streamID),
			zap.Int64("seq", seq),
			zap.Error(err),
		)
		writeUseCaseError(w, err, "upload failed")
		return
	}

	h.logger.Info("segment uploaded",
		zap.String("streamId", streamID),
		zap.Int64("seq", seq),
		zap.Int("sizeBytes", len(data)),
	)

	api.WriteNoContent(w)
}

// Complete handles POST /stream/sessions/{streamId}/complete.
func (h *SegmentHandler) Complete(w http.ResponseWriter, r *http.Request) {
	uploadToken, err := bearerToken(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	streamID := r.PathValue("streamId")
	if _, err := uuid.Parse(streamID); err != nil {
		http.Error(w, "invalid streamId", http.StatusBadRequest)
		return
	}

	req := usecase.SegmentUploadRequest{
		StreamID:    streamID,
		UploadToken: uploadToken,
		StopReason:  readStopReason(r, h.logger, streamID),
	}
	session, newlyCompleted, err := h.useCase.MarkComplete(r.Context(), req)
	if err != nil {
		h.logger.Warn("mark segment complete failed",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
		writeUseCaseError(w, err, "mark complete failed")
		return
	}

	h.logger.Info("segment upload marked complete",
		zap.String("streamId", streamID),
		zap.Bool("newlyCompleted", newlyCompleted),
		zap.String("stopReason", session.StopReason),
	)

	eventID, idErr := uuid.NewV7()
	if idErr != nil {
		http.Error(w, "cannot create assembly event id", http.StatusInternalServerError)
		return
	}
	// Publish on every idempotent completion call. If Kafka was unavailable after
	// the first state transition, a client retry can still enqueue the durable job.
	if err := h.publisher.PublishRecordingAssemblyRequested(r.Context(), domain.RecordingAssemblyRequestedEvent{
		EventID: eventID.String(), StreamID: session.StreamID, ScheduleID: session.ScheduleID,
		SessionID: session.SessionID, ParticipantID: session.CandidateID,
		StreamType: session.StreamType, Source: "DESKTOP_SEGMENT_UPLOAD", RequestedAt: time.Now().UTC(),
	}); err != nil {
		h.logger.Error("publish recording assembly request failed", zap.String("streamId", streamID), zap.Error(err))
		http.Error(w, "cannot queue recording assembly", http.StatusServiceUnavailable)
		return
	}

	// Only after the assembly job is durably queued: disarming any earlier would leave a window
	// where neither the client nor the watchdog owns this stream. Disarming late is harmless --
	// assembly is idempotent -- so the ordering is safe in exactly one direction.
	if err := h.pendingAssembly.Cancel(r.Context(), streamID); err != nil {
		h.logger.Warn("disarm assembly watchdog failed; it may assemble this stream again later, which is idempotent",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
	}

	api.WriteNoContent(w)
}

const maxCompleteBodySize = 1 << 10

type completeRequest struct {
	StopReason string `json:"stopReason"`
}

// readStopReason parses the optional /complete body.
//
// Optional is the whole design constraint: every client in the field today POSTs to /complete with
// no body at all, and a stream whose completion is refused never gets assembled and never gets its
// watchdog disarmed. So every failure path here -- absent body, malformed JSON, unknown reason --
// yields the empty string and lets completion proceed. A diagnostic must not be able to cost a
// candidate their recording.
func readStopReason(r *http.Request, logger *zap.Logger, streamID string) string {
	if r.Body == nil {
		return ""
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxCompleteBodySize))
	if err != nil || len(bytes.TrimSpace(body)) == 0 {
		return ""
	}

	var parsed completeRequest
	if err := json.Unmarshal(body, &parsed); err != nil {
		logger.Warn("ignoring unparseable /complete body",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
		return ""
	}

	reason := usecase.NormalizeStopReason(parsed.StopReason)
	if reason == "" && strings.TrimSpace(parsed.StopReason) != "" {
		logger.Warn("ignoring unrecognised stop reason",
			zap.String("streamId", streamID),
			zap.Int("reasonLength", len(parsed.StopReason)),
		)
	}
	return reason
}

type inventorySegmentRequest struct {
	Seq           int64     `json:"seq"`
	StartedAt     time.Time `json:"startedAt"`
	EndedAt       time.Time `json:"endedAt"`
	SHA256        string    `json:"sha256"`
	SizeBytes     int64     `json:"sizeBytes"`
	FramesWritten int64     `json:"framesWritten"`
}

type inventoryRequest struct {
	Complete   bool                      `json:"complete"`
	DeclaredAt time.Time                 `json:"declaredAt"`
	Segments   []inventorySegmentRequest `json:"segments"`
}

// DeclareInventory handles PUT /stream/sessions/{streamId}/inventory: the client's own account of
// what it has captured, whether or not it managed to upload it.
//
// Sent repeatedly during a recording rather than only at the end, and that is the entire point: an
// inventory that only arrives with /complete tells the server nothing in the one case worth
// protecting against, which is the client never getting to /complete at all.
func (h *SegmentHandler) DeclareInventory(w http.ResponseWriter, r *http.Request) {
	uploadToken, err := bearerToken(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	streamID := r.PathValue("streamId")
	if _, err := uuid.Parse(streamID); err != nil {
		http.Error(w, "invalid streamId", http.StatusBadRequest)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxInventoryBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var body inventoryRequest
	if err := decoder.Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if len(body.Segments) > maxInventorySegments {
		http.Error(w, "too many segments", http.StatusRequestEntityTooLarge)
		return
	}

	segments := make([]cache.DeclaredSegment, len(body.Segments))
	for i, segment := range body.Segments {
		segments[i] = cache.DeclaredSegment{
			Seq:           segment.Seq,
			StartedAt:     segment.StartedAt,
			EndedAt:       segment.EndedAt,
			SHA256:        segment.SHA256,
			SizeBytes:     segment.SizeBytes,
			FramesWritten: segment.FramesWritten,
		}
	}

	if err := h.useCase.DeclareInventory(r.Context(), usecase.InventoryDeclaration{
		StreamID:    streamID,
		UploadToken: uploadToken,
		Complete:    body.Complete,
		DeclaredAt:  body.DeclaredAt,
		Segments:    segments,
	}); err != nil {
		h.logger.Warn("declare segment inventory failed",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
		writeUseCaseError(w, err, "declare inventory failed")
		return
	}

	api.WriteNoContent(w)
}

// Audit handles GET /stream/sessions/{streamId}/audit. Read-only, so unlike Upload it is not
// gated on session.Completed -- inspecting segment coverage should work both before and after
// the client has called /complete.
func (h *SegmentHandler) Audit(w http.ResponseWriter, r *http.Request) {
	uploadToken, err := bearerToken(r)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	streamID := r.PathValue("streamId")
	if _, err := uuid.Parse(streamID); err != nil {
		http.Error(w, "invalid streamId", http.StatusBadRequest)
		return
	}

	req := usecase.SegmentUploadRequest{
		StreamID:    streamID,
		UploadToken: uploadToken,
	}
	audit, err := h.useCase.Audit(r.Context(), req)
	if err != nil {
		h.logger.Warn("segment audit failed",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
		writeUseCaseError(w, err, "audit failed")
		return
	}

	if err := api.WriteJSON(w, http.StatusOK, toAuditResponse(audit)); err != nil {
		h.logger.Warn("write segment audit response failed", zap.String("streamId", streamID), zap.Error(err))
	}
}

type AuditGapResponse struct {
	FromSeq     int64 `json:"fromSeq"`
	ToSeq       int64 `json:"toSeq"`
	MissingSecs int64 `json:"missingSecs"`
}

type AuditResponse struct {
	StreamID             string             `json:"streamId"`
	TotalSegments        int                `json:"totalSegments"`
	RecordedDurationSecs int64              `json:"recordedDurationSecs"`
	HasGaps              bool               `json:"hasGaps"`
	Gaps                 []AuditGapResponse `json:"gaps"`
	// What the client declared measured against what arrived. Empty when the client has not sent an
	// inventory, in which case HasGaps falls back to the weaker timestamp heuristic.
	Coverage stream.StreamCoverage `json:"coverage"`
}

func toAuditResponse(audit *stream.StreamAudit) AuditResponse {
	gaps := make([]AuditGapResponse, len(audit.Gaps))
	for i, g := range audit.Gaps {
		gaps[i] = AuditGapResponse{
			FromSeq:     g.FromSeq,
			ToSeq:       g.ToSeq,
			MissingSecs: int64(g.Missing.Seconds()),
		}
	}
	return AuditResponse{
		StreamID:             audit.StreamID,
		TotalSegments:        audit.TotalSegments,
		RecordedDurationSecs: int64(audit.RecordedDuration.Seconds()),
		HasGaps:              audit.HasGaps,
		Gaps:                 gaps,
		Coverage:             audit.Coverage,
	}
}

type CreateSessionRequest struct {
	StreamType string `json:"streamType"`
}

type CreateSessionResponse struct {
	StreamID    string    `json:"streamId"`
	StreamType  string    `json:"streamType"`
	ExpiresAt   time.Time `json:"expiresAt"`
	UploadToken string    `json:"uploadToken"`
}

func (h *SegmentHandler) CreateSession(w http.ResponseWriter, r *http.Request) {
	claims, err := h.validateStreamToken(r)
	if err != nil {
		// Was silent before -- a bad/expired/wrong-secret JWT produced a bare 401 with nothing in
		// the server's own log, which is exactly the failure mode that's hardest to diagnose from
		// the client side alone (client just sees "401 Unauthorized", no reason why).
		h.logger.Warn("create upload session: stream token rejected", zap.Error(err))
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxCreateBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	var body CreateSessionRequest
	if err := decoder.Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(body.StreamType) == "" {
		http.Error(w, "streamType is required", http.StatusBadRequest)
		return
	}
	if !claims.CanStream(body.StreamType) {
		http.Error(w, "forbidden stream type", http.StatusForbidden)
		return
	}

	streamID, err := uuid.NewV7()
	if err != nil {
		http.Error(w, "cannot create stream id", http.StatusInternalServerError)
		return
	}

	// Deliberately NOT derived from the token's exp. The JWT's lifetime is how often permission
	// gets re-checked; this is how long already-recorded evidence can still be pushed. Tying them
	// together means a short-lived token both strands buffered segments and makes the assembly
	// watchdog below (armed at ExpiresAt + grace) finalize a recording mid-exam.
	//
	// Never shorter than the old behaviour: only extends when the issuer says the schedule runs
	// past the token
	uploadWindowEnd := claims.ExpiresAt.Time.UTC()
	if scheduleEnd, ok := claims.ScheduleEnd(); ok && scheduleEnd.After(uploadWindowEnd) {
		uploadWindowEnd = scheduleEnd
	}
	expiresAt := uploadWindowEnd.Add(uploadCredentialGrade)
	uploadToken, uploadTokenHash, err := newUploadToken()
	if err != nil {
		http.Error(w, "cannot create upload credential", http.StatusInternalServerError)
		return
	}

	session := cache.UploadSession{
		StreamID:        streamID.String(),
		CandidateID:     claims.CandidateID,
		SessionID:       claims.SessionID,
		ScheduleID:      claims.ScheduleID,
		StreamType:      body.StreamType,
		CreatedAt:       time.Now().UTC(),
		ExpiresAt:       expiresAt,
		UploadTokenHash: uploadTokenHash,
	}

	registered, created, err := h.sessionRegistry.RegisterOrGetUpload(r.Context(), session)
	if err != nil {
		h.logger.Warn("register upload session failed",
			zap.String("candidateId", claims.CandidateID),
			zap.String("sessionId", claims.SessionID),
			zap.String("streamType", body.StreamType),
			zap.Error(err),
		)
		http.Error(w, "cannot register session", http.StatusInternalServerError)
		return
	}

	// Arm the watchdog for this stream. Reached on a resumed session too (created == false), where
	// RegisterOrGetUpload has just pushed ExpiresAt out and the due time must follow it.
	//
	// A failure here is logged rather than returned: the client's own /complete remains the primary
	// path, and refusing to start recording because the safety net could not be armed would trade a
	// rare unassembled recording for a certain missing one.
	if err := h.pendingAssembly.Schedule(r.Context(), cache.PendingAssembly{
		StreamID:      registered.StreamID,
		ScheduleID:    registered.ScheduleID,
		SessionID:     registered.SessionID,
		ParticipantID: registered.CandidateID,
		StreamType:    registered.StreamType,
		DueAt:         registered.ExpiresAt.Add(assemblyWatchdogGrace),
	}); err != nil {
		h.logger.Error("arm assembly watchdog failed; this stream will only be assembled if the client calls /complete",
			zap.String("streamId", registered.StreamID),
			zap.Error(err),
		)
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	stateEventID, idErr := uuid.NewV7()
	if idErr != nil {
		http.Error(w, "cannot create recording state event id", http.StatusInternalServerError)
		return
	}
	if err := h.publisher.PublishRecordingPartChanged(r.Context(), domain.RecordingPartChangedEvent{
		EventID: stateEventID.String(), StreamID: registered.StreamID,
		ScheduleID: registered.ScheduleID, SessionID: registered.SessionID,
		ParticipantID: registered.CandidateID, StreamType: registered.StreamType,
		Source: "DESKTOP_SEGMENT_UPLOAD", Status: "UPLOADING", OccurredAt: time.Now().UTC(),
	}); err != nil {
		h.logger.Error("publish initial recording state failed", zap.String("streamId", registered.StreamID), zap.Error(err))
		http.Error(w, "cannot publish recording state", http.StatusServiceUnavailable)
		return
	}
	if err := api.WriteJSON(w, status, CreateSessionResponse{
		StreamID:    registered.StreamID,
		StreamType:  registered.StreamType,
		ExpiresAt:   registered.ExpiresAt,
		UploadToken: uploadToken,
	}); err != nil {
		h.logger.Warn("write create upload session response failed", zap.Error(err))
	}
}

func bearerToken(r *http.Request) (string, error) {
	authorization := r.Header.Get("Authorization")
	token, found := strings.CutPrefix(authorization, "Bearer ")
	if !found || strings.TrimSpace(token) == "" {
		return "", errors.New("missing bearer token")
	}
	return strings.TrimSpace(token), nil
}

func newUploadToken() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	hash := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(hash[:]), nil
}

func (h *SegmentHandler) validateStreamToken(r *http.Request) (*auth.StreamClaims, error) {
	authorization := r.Header.Get("Authorization")
	token, found := strings.CutPrefix(authorization, "Bearer ")
	if !found || strings.TrimSpace(token) == "" {
		return nil, errors.New("missing bearer token")
	}
	return h.validator.ValidateStream(token)
}

func writeUseCaseError(w http.ResponseWriter, err error, fallback string) {
	switch {
	case errors.Is(err, usecase.ErrUploadSessionNotFound):
		http.Error(w, "upload session not found", http.StatusNotFound)
	case errors.Is(err, usecase.ErrUploadSessionExpired):
		http.Error(w, "upload session expired", http.StatusGone)
	case errors.Is(err, usecase.ErrUploadSessionOwnership):
		http.Error(w, "forbidden", http.StatusForbidden)
	case errors.Is(err, usecase.ErrUploadSessionCompleted):
		http.Error(w, "upload session already completed", http.StatusConflict)
	case errors.Is(err, usecase.ErrSegmentConflict):
		http.Error(w, "segment sequence conflict", http.StatusConflict)
	default:
		http.Error(w, fallback, http.StatusInternalServerError)
	}
}
