package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"go.uber.org/zap"
)

var (
	metricConsumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "lag",
		Help:      "Number of messages waiting to be processed (producer offset - consumer offset)",
	}, []string{"topic", "group_id"})
	metricMessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "messages_processed_total",
		Help:      "Total number of messages successfully processed",
	}, []string{"topic", "group_id"})
	metricMessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "messages_failed_total",
		Help:      "Total number of messages that failed after all retries",
	}, []string{"topic", "group_id", "reason"})
	metricDLQMessage = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "dlq_messages_total",
		Help:      "Total number of messages sent to dead letter queue",
	}, []string{"topic", "group_id", "reason"})
	metricProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "processing_duration_seconds",
		Help:      "Time taken to process a single message",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10},
	}, []string{"topic", "group_id"})
	metricFetchErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "fetch_errors_total",
		Help:      "Total number of message fetch errors",
	}, []string{"topic", "group_id"})
	metricDLQWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kafka",
		Subsystem: "dlq",
		Name:      "write_duration_seconds",
		Help:      "Time taken to write a message to DLQ",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"topic"})
)

type AlertConfig struct {
	LagThreshold int64
	LagDuration  time.Duration
	DLQThreshold int64
}

var DefaultAlertConfigs = map[string]AlertConfig{
	domain.TopicFrameReady: {
		LagThreshold: 1000, // đang chờ 1000 frame
		LagDuration:  2 * time.Minute,
		DLQThreshold: 50,
	},
	domain.TopicStreamStarted: {
		LagThreshold: 20,
		LagDuration:  1 * time.Minute,
		DLQThreshold: 5,
	},
	domain.TopicStreamEnded: {
		LagThreshold: 20,
		LagDuration:  1 * time.Minute,
		DLQThreshold: 5,
	},
	domain.TopicRoomClosed: {
		LagThreshold: 5,
		LagDuration:  30 * time.Second,
		DLQThreshold: 1, // thông báo cho tất cả mọi DLQ
	},
}

type AlertFunc func(alert Alert)

type Alert struct {
	Level     AlertLevel
	Topic     string
	GroupID   string
	Message   string
	Value     int64
	Threshold int64
	At        time.Time
}

type AlertLevel string

const (
	AlertWarning  AlertLevel = "WARNING"
	AlertCritical AlertLevel = "CRITICAL"
)

type Monitor struct {
	consumers    []*Consumer
	dlqWriter    *DLQWriter
	alertConfigs map[string]AlertConfig
	alertFn      AlertFunc
	groupID      string
	logger       *zap.Logger

	lagExceededSince map[string]time.Time
	mu               sync.Mutex

	dlqCountSnapshot map[string]int64
}

func NewMonitor(
	consumers []*Consumer,
	dlqWriter *DLQWriter,
	alertConfigs map[string]AlertConfig,
	alertFn AlertFunc,
	groupID string,
	logger *zap.Logger,
) *Monitor {
	return &Monitor{
		consumers:    consumers,
		dlqWriter:    dlqWriter,
		alertConfigs: alertConfigs,
		alertFn:      alertFn,
		groupID:      groupID,
		logger:       logger,

		lagExceededSince: make(map[string]time.Time),
		dlqCountSnapshot: make(map[string]int64),
	}
}

func (m *Monitor) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	m.logger.Info("kafka monitor started", zap.Int("consumers", len(m.consumers)))

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("kafka monitor stopped")
			return
		case <-ticker.C:
			m.collect()
		}
	}
}

func (m *Monitor) collect() {
	for _, consumer := range m.consumers {
		stats := consumer.Stats()
		m.processStats(stats)
	}
}

func (m *Monitor) processStats(stats kafka.ReaderStats) {
	topic := stats.Topic
	groupID := m.groupID

	metricConsumerLag.WithLabelValues(topic, groupID).Set(float64(stats.Lag))
	metricMessagesProcessed.WithLabelValues(topic, groupID).Add(float64(stats.Messages))
	metricFetchErrors.WithLabelValues(topic, groupID).Add(float64(stats.Errors))

	m.logger.Debug("kafka consumer stats",
		zap.String("topic", topic),
		zap.Int64("lag", stats.Lag),
		zap.Int64("messages", stats.Messages),
		zap.Int64("fetch_errors", stats.Errors),
		zap.Duration("read_time_avg", stats.ReadTime.Avg),
		zap.Duration("wait_time_avg", stats.WaitTime.Avg),
		zap.Int64("bytes_read", stats.Bytes),
	)

	alertCfg, hasAlert := m.alertConfigs[topic]
	if !hasAlert {
		return
	}

	m.checkLagAlert(topic, groupID, stats.Lag, alertCfg)
}

func (m *Monitor) checkLagAlert(topic, groupID string, lag int64, cfg AlertConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if lag > cfg.LagThreshold {
		if since, alreadyExceeded := m.lagExceededSince[topic]; !alreadyExceeded {
			// lag bắt đầu vượt ngưỡng -> ghi lại thời điểm
			m.lagExceededSince[topic] = time.Now()
			m.logger.Warn("kafka lag exceeded threshold",
				zap.String("topic", topic),
				zap.Int64("lag", lag),
				zap.Int64("threshold", cfg.LagThreshold),
			)
		} else if time.Since(since) > cfg.LagDuration {
			level := AlertWarning
			if lag > cfg.LagThreshold*3 {
				level = AlertCritical // lag vuợt ngưỡng gấp 3 lần -> critical
			}
			m.fireAlert(Alert{
				Level:     level,
				Topic:     topic,
				GroupID:   groupID,
				Message:   fmt.Sprintf("Consumer lag %d vuợt ngưỡng %d trong %s ", lag, cfg.LagThreshold, cfg.LagDuration),
				Value:     lag,
				Threshold: cfg.LagThreshold,
				At:        time.Now(),
			})
		}
	} else {
		if _, wasExceeded := m.lagExceededSince[topic]; wasExceeded {
			delete(m.lagExceededSince, topic)
			m.logger.Info("kafka lag recovered",
				zap.String("topic", topic),
				zap.Int64("lag", lag),
				zap.Int64("threshold", cfg.LagThreshold),
			)
		}
	}
}

func (m *Monitor) RecordProcessing(topic, groupID string, duration time.Duration, err error, reason string) {
	metricProcessingDuration.WithLabelValues(topic, groupID).Observe(duration.Seconds())
	if err != nil {
		metricMessagesFailed.WithLabelValues(topic, groupID, reason).Inc()
	}
}

func (m *Monitor) RecordDLQ(topic, groupID, reason string, writeDuration time.Duration) {
	metricDLQMessage.WithLabelValues(topic, groupID, reason).Inc()
	metricDLQWriteDuration.WithLabelValues(topic, groupID, reason).Observe(writeDuration.Seconds())

	m.mu.Lock()
	m.dlqCountSnapshot[topic]++
	count := m.dlqCountSnapshot[topic]
	m.mu.Unlock()

	alertCfg, hasAlert := m.alertConfigs[topic]
	if !hasAlert {
		return
	}

	if count >= alertCfg.DLQThreshold {
		m.fireAlert(Alert{
			Level:     AlertCritical,
			Topic:     topic,
			GroupID:   groupID,
			Message:   fmt.Sprintf("DLQ %s có %d message - handler đang fail liên tục", topic+dlqSuffix, count),
			Value:     count,
			Threshold: alertCfg.DLQThreshold,
			At:        time.Now(),
		})

		m.mu.Lock()
		m.dlqCountSnapshot[topic] = 0
		m.mu.Unlock()
	}
}

func (m *Monitor) fireAlert(alert Alert) {
	m.logger.Error("KAFKA ALERT",
		zap.String("level", string(alert.Level)),
		zap.String("topic", alert.Topic),
		zap.String("group_id", alert.GroupID),
		zap.String("message", alert.Message),
		zap.Int64("value", alert.Value),
		zap.Int64("threshold", alert.Threshold),
	)

	if m.alertFn != nil {
		go m.alertFn(alert) // async - không block vòng lặp monitoring
	}
}
