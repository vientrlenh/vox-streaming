package http

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)


func RunMetric(logger *zap.Logger) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	metricAddr := os.Getenv("METRIC_ADDR")
	if metricAddr == "" {
		metricAddr = ":9090"
	}
	logger.Info("metric server started", zap.String("addr", metricAddr))
	if err := http.ListenAndServe(metricAddr, mux); err != nil {
		logger.Error("metrics server error", zap.Error(err))
	}
}