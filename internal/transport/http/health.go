package http

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	grpcclient "github.com/vientrlenh/vox-streaming/internal/transport/grpc/client"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/queue"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
)

type checkResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

type readyResponse struct {
	Status string                 `json:"status"`
	Checks map[string]checkResult `json:"checks"`
}

type HealthChecker struct {
	redis        *redis.Client
	kafkaBrokers []string
	kafkaCfg     queue.Config
	storage      *storage.Client
	exam         *grpcclient.ExamClient
}

func NewHealthChecker(
	redis *redis.Client,
	kafkaBrokers []string,
	kafkaCfg queue.Config,
	storage *storage.Client,
	exam *grpcclient.ExamClient,
) *HealthChecker {
	return &HealthChecker{
		redis:        redis,
		kafkaBrokers: kafkaBrokers,
		kafkaCfg:     kafkaCfg,
		storage:      storage,
		exam:         exam,
	}
}

// ServeReadyz checks all upstream dependencies in parallel.
// Returns 200 only when all configured dependencies are healthy.
// Intended for Kubernetes readinessProbe on the metrics port.
func (h *HealthChecker) ServeReadyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	results := make(map[string]checkResult)
	var mu sync.Mutex
	var wg sync.WaitGroup

	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := checkResult{Status: "ok"}
			if err := fn(ctx); err != nil {
				res = checkResult{Status: "error", Message: err.Error()}
			}
			mu.Lock()
			results[name] = res
			mu.Unlock()
		}()
	}

	run("redis", func(ctx context.Context) error {
		return h.redis.Ping(ctx).Err()
	})

	run("kafka", func(ctx context.Context) error {
		return queue.PingKafka(ctx, h.kafkaCfg, h.kafkaBrokers)
	})

	run("storage", func(ctx context.Context) error {
		return h.storage.Ping(ctx)
	})

	if h.exam != nil && h.exam.Addr() != "" {
		run("exam_service", func(ctx context.Context) error {
			return h.exam.Ping(ctx)
		})
	} else {
		mu.Lock()
		results["exam_service"] = checkResult{Status: "skipped", Message: "EXAM_SERVICE_GRPC_ADDR not set"}
		mu.Unlock()
	}

	wg.Wait()

	overall := "ok"
	code := http.StatusOK
	for _, res := range results {
		if res.Status == "error" {
			overall = "error"
			code = http.StatusServiceUnavailable
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(readyResponse{Status: overall, Checks: results})
}
