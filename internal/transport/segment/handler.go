package segment

import (
	"context"
	"crypto/sha256"
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
	assembler       *usecase.AssemblerUseCase
	validator       *auth.Validator
	sessionRegistry *cache.SessionRegistry
	logger          *zap.Logger
}

func NewSegmentHandler(uc *usecase.SegmentUseCase, assembler *usecase.AssemblerUseCase, v *auth.Validator, sr *cache.SessionRegistry, logger *zap.Logger) *SegmentHandler {
	return &SegmentHandler{
		useCase:         uc,
		assembler:       assembler,
		validator:       v,
		sessionRegistry: sr,
		logger:          logger,
	}
}

// Upload handles PUT /stream/sessions/{streamId}/segments/{seq}.
// The body must contain one MP4 segment.
func (h *SegmentHandler) Upload(w http.ResponseWriter, r *http.Request) {
	claims, err := h.validateStreamToken(r)
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
		StreamID:      streamID,
		ParticipantID: claims.CandidateID,
		SessionID:     claims.SessionID,
		ScheduleID:    claims.ScheduleID,
		StreamTypes:   claims.StreamTypes,
		Seq:           seq,
		StartedAt:     startedAt,
		EndedAt:       endedAt,
		SHA256:        checksum,
		Data:          data,
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
	claims, err := h.validateStreamToken(r)
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
		StreamID:      streamID,
		ParticipantID: claims.CandidateID,
		SessionID:     claims.SessionID,
		ScheduleID:    claims.ScheduleID,
		StreamTypes:   claims.StreamTypes,
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

	// Assembly can take a while (download + ffmpeg + upload) - don't block the
	// response on it. r.Context() is cancelled once we return, so use a fresh
	// background context for the detached work.
	go func() {
		if err := h.assembler.Assemble(
			context.Background(),
			session.ScheduleID,
			session.SessionID,
			session.StreamID,
		); err != nil {
			h.logger.Error("assembly after completion failed",
				zap.String("streamId", streamID),
				zap.Error(err),
			)
		}
	}()
	api.WriteNoContent(w)
}

type CreateSessionRequest struct {
	StreamType string `json:"streamType"`
}

type CreateSessionResponse struct {
	StreamID   string    `json:"streamId"`
	StreamType string    `json:"streamType"`
	ExpiresAt  time.Time `json:"expiresAt"`
}

func (h *SegmentHandler) CreateSession(w http.ResponseWriter, r *http.Request) {
	claims, err := h.validateStreamToken(r)
	if err != nil {
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

	expiresAt := claims.ExpiresAt.Time.UTC()

	session := cache.UploadSession{
		StreamID:    streamID.String(),
		CandidateID: claims.CandidateID,
		SessionID:   claims.SessionID,
		ScheduleID:  claims.ScheduleID,
		StreamType:  body.StreamType,
		CreatedAt:   time.Now().UTC(),
		ExpiresAt:   expiresAt,
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
	if err := api.WriteJSON(w, status, CreateSessionResponse{
		StreamID:   registered.StreamID,
		StreamType: registered.StreamType,
		ExpiresAt:  registered.ExpiresAt,
	}); err != nil {
		h.logger.Warn("write create upload session response failed", zap.Error(err))
	}
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
