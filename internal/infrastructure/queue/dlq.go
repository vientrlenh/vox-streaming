package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
)

const dlqSuffix = ".dlq"

type DLQMessage struct {
	FailedAt     time.Time `json:"failedAt"`
	Reason       string    `json:"reason"`
	ErrorDetail  string    `json:"errorDetail"`
	AttemptCount int       `json:"attemptCount"`

	OriginalTopic     string `json:"originalTopic"`
	OriginalPartition int    `json:"originalPartition"`
	OriginalOffset    int64  `json:"originalOffset"`
	OriginalKey       string `json:"originalKey"`

	OriginalPayload json.RawMessage `json:"originalPayload"`

	OriginalHeaders map[string]string `json:"originalHeaders,omitempty"`
	ConsumerGroup   string            `json:"consumerGroup"`
}

type DLQWriter struct {
	writers map[string]*kafka.Writer
	logger  *zap.Logger
	groupID string
}

func NewDLQWriter(cfg Config, topics []string, logger *zap.Logger) (*DLQWriter, error) {
	writers := make(map[string]*kafka.Writer, len(topics))

	for _, topic := range topics {
		dlqTopic := topic + dlqSuffix
		writers[topic] = &kafka.Writer{
			Addr:  kafka.TCP(cfg.Brokers...),
			Topic: dlqTopic,

			BatchSize:    1,
			BatchTimeout: 0,

			Async:        false,
			RequiredAcks: kafka.RequireAll,
			MaxAttempts:  10,

			Compression: kafka.Snappy,

			ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
				logger.Error("kafka dlq writer error",
					zap.String("dlqTopic", dlqTopic),
					zap.String("msg", fmt.Sprintf(msg, args...)),
				)
			}),
		}
		logger.Info("dlq writer initialized",
			zap.String("originalTopic", topic),
			zap.String("dlqTopic", dlqTopic),
		)
	}

	return &DLQWriter{
		writers: writers,
		logger:  logger,
		groupID: cfg.GroupID,
	}, nil
}

func (d *DLQWriter) Send(ctx context.Context, originalMsg kafka.Message, handlerErr error, attemptCount int) error {
	writer, ok := d.writers[originalMsg.Topic]
	if !ok {
		d.logger.Error("dlq writer not found for topic - message lost",
			zap.String("topic", originalMsg.Topic),
			zap.Int64("offset", originalMsg.Offset),
			zap.Error(handlerErr),
		)
		return fmt.Errorf("dlq: no writer for topic %q", originalMsg.Topic)
	}

	headers := make(map[string]string, len(originalMsg.Headers))
	for _, h := range originalMsg.Headers {
		headers[h.Key] = string(h.Value)
	}

	dlqMsg := DLQMessage{
		FailedAt:          time.Now().UTC(),
		Reason:            classifyError(handlerErr),
		ErrorDetail:       handlerErr.Error(),
		AttemptCount:      attemptCount,
		OriginalTopic:     originalMsg.Topic,
		OriginalPartition: originalMsg.Partition,
		OriginalOffset:    originalMsg.Offset,
		OriginalKey:       string(originalMsg.Key),
		OriginalPayload:   json.RawMessage(originalMsg.Value),
		OriginalHeaders:   headers,
		ConsumerGroup:     d.groupID,
	}

	data, err := json.Marshal(dlqMsg)
	if err != nil {
		return fmt.Errorf("dlq: marshal message: %w", err)
	}

	writeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := writer.WriteMessages(writeCtx, kafka.Message{
		Key:   []byte(originalMsg.Key),
		Value: data,
		Headers: []kafka.Header{
			{Key: "dlq-reason", Value: []byte(dlqMsg.Reason)},
			{Key: "dlq-original-topic", Value: []byte(originalMsg.Topic)},
			{Key: "dlq-failed-at", Value: []byte(dlqMsg.FailedAt.Format(time.RFC3339))},
		},
	}); err != nil {
		d.logger.Error("CRITICAL: failed to write to DLQ - message permanently lost",
			zap.String("original_topic", originalMsg.Topic),
			zap.Int64("original_offset", originalMsg.Offset),
			zap.String("original_key", string(originalMsg.Key)),
			zap.Error(err),
		)
		return fmt.Errorf("dlq write failed: %w", err)
	}

	d.logger.Warn("message sent to DLQ",
		zap.String("dlqTopic", originalMsg.Topic+dlqSuffix),
		zap.String("originalTopic", originalMsg.Topic),
		zap.Int64("originalOffset", originalMsg.Offset),
		zap.String("reason", dlqMsg.Reason),
		zap.Int("attempts", attemptCount),
	)
	return nil
}

func (d *DLQWriter) Close() error {
	var lastErr error
	for topic, w := range d.writers {
		if err := w.Close(); err != nil {
			d.logger.Error("dlq writer close error",
				zap.String("topic", topic+dlqSuffix),
				zap.Error(err),
			)
			lastErr = err
		}
	}
	return lastErr
}

func classifyError(err error) string {
	if err == nil {
		return "unknown"
	}
	msg := err.Error()
	switch {
	case contains(msg, "unmarshal", "invalid", "syntax", "json"):
		return "deserialization_error"
	case contains(msg, "timeout", "deadline", "context"):
		return "timeout_error"
	case contains(msg, "connection", "dial", "network", "refused"):
		return "network_error"
	case contains(msg, "not found", "404"):
		return "not_found_error"
	case contains(msg, "unauthorized", "forbidden", "401", "403"):
		return "auth_error"
	default:
		return "processing_error"
	}
}

func contains(s string, subs ...string) bool {
	for _, sub := range subs {
		for i := 0; i <= len(s)-len(sub); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
	}
	return false
}

type ReProecessHandler func(ctx context.Context, msg DLQMessage) error

type DLQReprocessor struct {
	reader    *kafka.Reader
	publisher *Publisher
	logger    *zap.Logger
}

func NewDLQReprocessor(cfg Config, originalTopic string, publisher *Publisher, logger *zap.Logger) *DLQReprocessor {
	dlqTopic := originalTopic + dlqSuffix
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID + ".dlq-reprocessor",
		Topic:    dlqTopic,
		MinBytes: 1,
		MaxBytes: 10 * 1024 * 1024,
		MaxWait:  500 * time.Millisecond,
	})

	return &DLQReprocessor{
		reader:    reader,
		publisher: publisher,
		logger:    logger,
	}
}

func (r *DLQReprocessor) ReplayToOriginalTopic(ctx context.Context, filter func(DLQMessage) bool) (replayed int, skipped int, err error) {
	r.logger.Info("dlq replay started")

	for {
		rawMsg, fetchErr := r.reader.FetchMessage(ctx)
		if fetchErr != nil {
			if fetchErr == context.Canceled {
				break
			}
			return replayed, skipped, fmt.Errorf("dlq fetch: %w", fetchErr)
		}

		var dlqMsg DLQMessage
		if jsonErr := json.Unmarshal(rawMsg.Value, &dlqMsg); jsonErr != nil {
			r.logger.Error("dlq: failed to parse DLQ message - skipping",
				zap.Int64("offset", rawMsg.Offset),
				zap.Error(jsonErr),
			)

			_ = r.reader.CommitMessages(ctx, rawMsg)
			skipped++
			continue
		}

		if filter != nil && !filter(dlqMsg) {
			_ = r.reader.CommitMessages(ctx, rawMsg)
			skipped++
			continue
		}

		replayCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		writeErr := r.publisher.publish(replayCtx, dlqMsg.OriginalTopic, dlqMsg.OriginalKey, dlqMsg.OriginalPayload)
		cancel()

		if writeErr != nil {
			r.logger.Error("dlq replay: failed to republish",
				zap.String("originalTopic", dlqMsg.OriginalTopic),
				zap.String("originalKey", dlqMsg.OriginalKey),
				zap.Error(writeErr),
			)
			return replayed, skipped, writeErr
		}

		_ = r.reader.CommitMessages(ctx, rawMsg)
		replayed++

		r.logger.Info("dlq message replayed",
			zap.String("originalTopic", dlqMsg.OriginalTopic),
			zap.Int64("originalOffset", dlqMsg.OriginalOffset),
			zap.String("reason", dlqMsg.Reason),
		)
	}

	r.logger.Info("dlq replay finished", zap.Int("replayed", replayed), zap.Int("skipped", skipped))

	return replayed, skipped, nil
}

func (r *DLQReprocessor) Close() error {
	return r.reader.Close()
}
