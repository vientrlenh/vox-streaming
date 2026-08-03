package http

import (
	"encoding/json"
	"net/http"

	"github.com/vientrlenh/vox-streaming/internal/healthcheck"
)

type HealthInfo struct {
	Health 	*healthcheck.HealthChecker
}

func NewHealthInfo(health *healthcheck.HealthChecker) *HealthInfo {
	return &HealthInfo{
		Health: health,
	}
}

type readyResponse struct {
	Status string                 `json:"status"`
	Checks map[string]healthcheck.CheckResult `json:"checks"`
}

// ServeReadyz checks all upstream dependencies in parallel.
// Returns 200 only when all configured dependencies are healthy.
// Intended for Kubernetes readinessProbe on the metrics port.
func (h *HealthInfo) ServeReadyz(w http.ResponseWriter, r *http.Request) {

	ok, results := h.Health.CheckAll(r.Context())
	if !ok {
		buildJson(w, http.StatusServiceUnavailable, "error", results)
		return
	}
	buildJson(w, http.StatusOK, "ok", results)
}

func buildJson(w http.ResponseWriter, code int, status string, results map[string]healthcheck.CheckResult) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(readyResponse{Status: status, Checks: results})
}
