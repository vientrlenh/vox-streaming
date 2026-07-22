package segment

import (
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
	"github.com/vientrlenh/vox-streaming/internal/transport/api"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
)

const (
	maxSegmentSize     = 50 << 20 // 50 MB
	maxSegmentDuration = 2 * time.Minute
	maxCreateBodySize  = 4 << 10 // 4 KB
)

type SegmentHandler struct {
	useCase         *usecase.SegmentUseCase
	publisher       domain.EventPublisher
	validator       *auth.Validator
	sessionRegistry *cache.SessionRegistry
	logger          *zap.Logger
}

func NewSegmentHandler(uc *usecase.SegmentUseCase, publisher domain.EventPublisher, v *auth.Validator, sr *cache.SessionRegistry, logger *zap.Logger) *SegmentHandler {
	return &SegmentHandler{
		useCase:         uc,
		publisher:       publisher,
		validator:       v,
		sessionRegistry: sr,
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
	api.WriteNoContent(w)
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

	// The short-lived upload credential keeps retries possible after the stream
	// JWT expires, without extending permission to open new recording streams.
	expiresAt := claims.ExpiresAt.Time.UTC().Add(30 * time.Minute)
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
