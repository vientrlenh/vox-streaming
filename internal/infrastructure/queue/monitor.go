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
	}, []string{"topic", "groupId"})
	metricMessagesProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "messagesProcessedTotal",
		Help:      "Total number of messages successfully processed",
	}, []string{"topic", "groupId"})
	metricMessagesFailed = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "messagesFailedTotal",
		Help:      "Total number of messages that failed after all retries",
	}, []string{"topic", "groupId", "reason"})
	metricDLQMessage = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "dlqMessagesTotal",
		Help:      "Total number of messages sent to dead letter queue",
	}, []string{"topic", "groupId", "reason"})
	metricProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "kafka",
		Subsystem: "consumer",
		Name:      "processingDurationSeconds",
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
		Name:      "writeDurationSeconds",
		Help:      "Time taken to write a message to DLQ",
		Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5},
	}, []string{"topic", "groupId", "reason"})
)

type ConsumerKey struct {
	Topic string
	GroupID string
}

type AlertConfig struct {
	LagThreshold        int64
	LagDuration         time.Duration
	DLQThreshold        int64
	FetchErrorThreshold int64
}

var DefaultAlertConfigs = map[string]AlertConfig{
	domain.TopicFrameReady: {
		LagThreshold:        1000, // waiting 1000 frames
		LagDuration:         2 * time.Minute,
		DLQThreshold:        50,
		FetchErrorThreshold: 20,
	},
	domain.TopicStreamStarted: {
		LagThreshold:        20,
		LagDuration:         1 * time.Minute,
		DLQThreshold:        5,
		FetchErrorThreshold: 5,
	},
	domain.TopicStreamEnded: {
		LagThreshold:        20, // assembler processing, higher lag throughput
		LagDuration:         1 * time.Minute,
		DLQThreshold:        3,
		FetchErrorThreshold: 5,
	},
	domain.TopicScheduleClosed: {
		LagThreshold:        5,
		LagDuration:         30 * time.Second,
		DLQThreshold:        1, // alert to all
		FetchErrorThreshold: 3,
	},
}

// fallbackAlertConfig applies to any consumer whose topic has neither an
// explicit override nor an entry in DefaultAlertConfigs — without this,
// BuildAlertConfigs silently drops the consumer from alertConfigs and it
// gets no lag/DLQ/fetch-error alerting at all (see TopicRecordingAssemblyRequested,
// which shipped with no entry until this fallback was added).
var fallbackAlertConfig = AlertConfig{
	LagThreshold:        100,
	LagDuration:         2 * time.Minute,
	DLQThreshold:        5,
	FetchErrorThreshold: 10,
}

func BuildAlertConfigs(consumers []*Consumer, defaults map[string]AlertConfig, overrides map[ConsumerKey]AlertConfig) map[ConsumerKey]AlertConfig {
	result := make(map[ConsumerKey]AlertConfig)
	for _, c := range consumers {
		key := ConsumerKey{Topic: c.topic, GroupID: c.groupID}
		if override, ok := overrides[key]; ok {
			result[key] = override
		} else if def, ok := defaults[c.topic]; ok {
			result[key] = def
		} else {
			result[key] = fallbackAlertConfig
		}
	}
	return result
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
	alertConfigs map[ConsumerKey]AlertConfig
	alertFn      AlertFunc
	logger       *zap.Logger

	lagExceededSince map[ConsumerKey]time.Time
	mu               sync.Mutex

	dlqCountSnapshot        map[ConsumerKey]int64
	fetchErrorCountSnapshot map[ConsumerKey]int64
}

func NewMonitor(
	consumers []*Consumer,
	dlqWriter *DLQWriter,
	alertConfigs map[ConsumerKey]AlertConfig,
	alertFn AlertFunc,
	logger *zap.Logger,
) *Monitor {
	return &Monitor{
		consumers:    consumers,
		dlqWriter:    dlqWriter,
		alertConfigs: alertConfigs,
		alertFn:      alertFn,
		logger:       logger,

		lagExceededSince:        make(map[ConsumerKey]time.Time),
		dlqCountSnapshot:        make(map[ConsumerKey]int64),
		fetchErrorCountSnapshot: make(map[ConsumerKey]int64),
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
		m.processStats(stats, consumer.groupID)
	}
}

func (m *Monitor) processStats(stats kafka.ReaderStats, groupID string) {
	topic := stats.Topic

	key := ConsumerKey{Topic: topic, GroupID: groupID}
	metricConsumerLag.WithLabelValues(topic, groupID).Set(float64(stats.Lag))

	m.logger.Debug("kafka consumer stats",
		zap.String("topic", topic), 
		zap.String("groupId", groupID),
		zap.Int64("lag", stats.Lag),
		zap.Int64("messages", stats.Messages),
		zap.Int64("fetchErrors", stats.Errors),
		zap.Duration("readTimeAvg", stats.ReadTime.Avg),
		zap.Duration("waitTimeAvg", stats.WaitTime.Avg),
		zap.Int64("bytesRead", stats.Bytes),
	)

	alertCfg, hasAlert := m.alertConfigs[key]
	if !hasAlert {
		return
	}

	m.checkLagAlert(topic, groupID, stats.Lag, alertCfg)
}

func (m *Monitor) checkLagAlert(topic, groupID string, lag int64, cfg AlertConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := ConsumerKey {
		Topic: topic, 
		GroupID: groupID,
	}
	if lag > cfg.LagThreshold {
		if since, alreadyExceeded := m.lagExceededSince[key]; !alreadyExceeded {
			// lag bắt đầu vượt ngưỡng -> ghi lại thời điểm
			m.lagExceededSince[key] = time.Now()
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
			m.lagExceededSince[key] = time.Now() // start to count lag duration
		}
	} else {
		if _, wasExceeded := m.lagExceededSince[key]; wasExceeded {
			delete(m.lagExceededSince, key)
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
	key := ConsumerKey {Topic: topic, GroupID: groupID}

	metricDLQMessage.WithLabelValues(topic, groupID, reason).Inc()
	metricDLQWriteDuration.WithLabelValues(topic, groupID, reason).Observe(writeDuration.Seconds())

	m.mu.Lock()
	m.dlqCountSnapshot[key]++
	count := m.dlqCountSnapshot[key]
	m.mu.Unlock()

	alertCfg, hasAlert := m.alertConfigs[key]
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
		m.dlqCountSnapshot[key] = 0
		m.mu.Unlock()
	}
}

func (m *Monitor) RecordFetchError(topic, groupID string) {
	metricFetchErrors.WithLabelValues(topic, groupID).Inc()

	key := ConsumerKey{Topic: topic, GroupID: groupID}

	alertCfg, hasAlert := m.alertConfigs[key]
	if !hasAlert {
		return
	}

	m.mu.Lock()
	m.fetchErrorCountSnapshot[key]++
	count := m.fetchErrorCountSnapshot[key]
	m.mu.Unlock()

	if count >= alertCfg.FetchErrorThreshold {
		m.fireAlert(Alert{
			Level:     AlertWarning,
			Topic:     topic,
			GroupID:   groupID,
			Message:   fmt.Sprintf("Kafka fetch lỗi %d lần liên tiếp - kiểm tra kết nối broker/consumer group", count),
			Value:     count,
			Threshold: alertCfg.FetchErrorThreshold,
			At:        time.Now(),
		})

		m.mu.Lock()
		m.fetchErrorCountSnapshot[key] = 0
		m.mu.Unlock()
	}
}

func (m *Monitor) fireAlert(alert Alert) {
	m.logger.Error("KAFKA ALERT",
		zap.String("level", string(alert.Level)),
		zap.String("topic", alert.Topic),
		zap.String("groupId", alert.GroupID),
		zap.String("message", alert.Message),
		zap.Int64("value", alert.Value),
		zap.Int64("threshold", alert.Threshold),
	)

	if m.alertFn != nil {
		go m.alertFn(alert) // async - không block vòng lặp monitoring
	}
}
