package healthcheck

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/queue"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
)

type HealthChecker struct {
	redis        *redis.Client
	kafkaBrokers []string
	kafkaCfg     queue.Config
	storage      *storage.Client
}

func NewHealthChecker(
	redis *redis.Client,
	kafkaBrokers []string,
	kafkaCfg queue.Config,
	storage *storage.Client,
) *HealthChecker {
	return &HealthChecker{
		redis:        redis,
		kafkaBrokers: kafkaBrokers,
		kafkaCfg:     kafkaCfg,
		storage:      storage,
	}
}

type CheckResult struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (h *HealthChecker) CheckAll(ctx context.Context) (ok bool, results map[string]CheckResult) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	results = make(map[string]CheckResult)

	var mu sync.Mutex
	var wg sync.WaitGroup

	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res := CheckResult{Status: "ok"}
			if err := fn(ctx); err != nil {
				res = CheckResult{Status: "error", Message: err.Error()}
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
	wg.Wait()

	ok = true
	for _, res := range results {
		if res.Status == "error" {
			ok = false
			break
		}
	}

	return ok, results
}