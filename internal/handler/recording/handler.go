package recording

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/transport/api"
	"go.uber.org/zap"
)

const maxPlaybackBodySize = 4 << 10

type RecordHandler struct {
	storage      *storage.Client
	serviceToken string
	logger       *zap.Logger
}

func NewHandler(storageClient *storage.Client, serviceToken string, logger *zap.Logger) *RecordHandler {
	return &RecordHandler{storage: storageClient, serviceToken: serviceToken, logger: logger}
}

type playbackRequest struct {
	ObjectKey string `json:"objectKey"`
}

type playbackResponse struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expiresAt"`
}

func (h *RecordHandler) CreatePlaybackURL(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxPlaybackBodySize)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var body playbackRequest
	if err := decoder.Decode(&body); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	bodyKey := strings.TrimSpace(body.ObjectKey)
	if !strings.HasPrefix(bodyKey, "schedules/") || !strings.HasSuffix(bodyKey, "/recording.mp4") || strings.Contains(bodyKey, "..") {
		http.Error(w, "invalid recording key", http.StatusBadRequest)
		return
	}

	expiry := h.storage.PresignExpiry()
	url, err := h.storage.PresignRecording(r.Context(), bodyKey, expiry)
	if err != nil {
		h.logger.Error("create recording playback URL failed", zap.String("objectKey", bodyKey), zap.Error(err))
		http.Error(w, "cannot create playback URL", http.StatusInternalServerError)
		return
	}
	_ = api.WriteJSON(w, http.StatusOK, playbackResponse{URL: url, ExpiresAt: time.Now().UTC().Add(expiry)})
}

func (h *RecordHandler) authorized(r *http.Request) bool {
	if h.serviceToken == "" {
		return false
	}
	raw, found := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !found {
		return false
	}
	raw = strings.TrimSpace(raw)
	return len(raw) == len(h.serviceToken) && subtle.ConstantTimeCompare([]byte(raw), []byte(h.serviceToken)) == 1
}
