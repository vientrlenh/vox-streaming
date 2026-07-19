package segment

import (
	"slices"
	"context"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"github.com/vientrlenh/vox-streaming/pkg/auth"
	"go.uber.org/zap"
)

const maxSegmentSize = 50 << 20 // 50 MB

type SegmentHandler struct {
	useCase *usecase.SegmentUseCase
	assembler *usecase.AssemblerUseCase
	validator *auth.Validator
	logger *zap.Logger
}

func NewSegmentHandler(uc *usecase.SegmentUseCase, assembler *usecase.AssemblerUseCase, v *auth.Validator, logger *zap.Logger) *SegmentHandler {
	return &SegmentHandler{
		useCase: uc,
		assembler: assembler,
		validator: v,
		logger: logger,
	}
}

// handle POST /stream/segment
// receive raw WebM binary body
func (h *SegmentHandler) Upload(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}
	claims, err := h.validator.Validate(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	q := r.URL.Query()
	roomID := q.Get("roomId")
	streamID := q.Get("streamId")
	streamType := q.Get("streamType")
	seqStr := q.Get("seq")
	startedAtStr := q.Get("startedAt")
	endedAtStr := q.Get("endedAt")

	allowed := slices.Contains(claims.RoomIDs, roomID)

	if !allowed {
		http.Error(w, "forbidden: wrong room", http.StatusForbidden)
		return
	}

	if !claims.IsStudent() {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if roomID == "" || streamID == "" || streamType == "" || seqStr == "" {
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

	data, err := io.ReadAll(io.LimitReader(r.Body, maxSegmentSize))
	if err != nil {
		http.Error(w, "read body failed", http.StatusBadRequest)
		return
	}
	if len(data) == 0 {
		http.Error(w, "empty body", http.StatusBadRequest)
		return
	}

	req := usecase.SegmentUploadRequest{
		StreamID: streamID, 
		ParticipantID: claims.UserID,
		SessionID: claims.SessionID,
		RoomID: roomID,
		StreamType: streamType, 
		Seq: seq, 
		StartedAt: startedAt, 
		EndedAt: endedAt, 
		Data: data,
	}

	if err := h.useCase.Upload(r.Context(), req); err != nil {
		h.logger.Warn("segment upload failed", 
			zap.String("streamId", streamID),
			zap.Int64("seq", seq),
			zap.Error(err),
		)
		http.Error(w, "upload failed", http.StatusInternalServerError)
		return
	}

	h.logger.Info("segment uploaded",
		zap.String("streamId", streamID),
		zap.Int64("seq", seq),
		zap.Int("sizeBytes", len(data)),
	)
	w.WriteHeader(http.StatusNoContent)
}

// handle POST /stream/segment/complete
// client signals it has finished uploading every chunk for this stream
func (h *SegmentHandler) Complete(w http.ResponseWriter, r *http.Request) {
	token := r.Header.Get("Authorization")
	if len(token) > 7 && token[:7] == "Bearer " {
		token = token[7:]
	}
	claims, err := h.validator.Validate(token)
	if err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	q := r.URL.Query()
	roomID := q.Get("roomId")
	streamID := q.Get("streamId")
	streamType := q.Get("streamType")

	allowed := slices.Contains(claims.RoomIDs, roomID)
	if !allowed {
		http.Error(w, "forbidden: wrong room", http.StatusForbidden)
		return
	}
	if !claims.IsStudent() {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if roomID == "" || streamID == "" || streamType == "" {
		http.Error(w, "missing required params", http.StatusBadRequest)
		return
	}

	req := usecase.SegmentUploadRequest{
		StreamID: streamID,
		ParticipantID: claims.UserID, 
		SessionID: claims.SessionID,
		RoomID: roomID,
		StreamType: streamType,
	}
	if err := h.useCase.MarkComplete(r.Context(), req); err != nil {
		h.logger.Warn("mark segment complete failed",
			zap.String("streamId", streamID),
			zap.Error(err),
		)
		http.Error(w, "mark complete failed", http.StatusInternalServerError)
		return
	}

	h.logger.Info("segment upload marked complete", zap.String("streamId", streamID))

	// Assembly can take a while (download + ffmpeg + upload) - don't block the
	// response on it. r.Context() is cancelled once we return, so use a fresh
	// background context for the detached work.
	go func() {
		if err := h.assembler.Assemble(context.Background(), roomID, claims.SessionID, streamID); err != nil {
			h.logger.Error("assembly after completion failed",
				zap.String("streamId", streamID),
				zap.Error(err),
			)
		}
	}()

	w.WriteHeader(http.StatusNoContent)
}