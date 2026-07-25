package queue

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"
	"go.uber.org/zap"
)

type HandlerFunc func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader              *kafka.Reader
	topic               string
	groupID             string
	handler             HandlerFunc
	dlq                 *DLQWriter
	monitor             *Monitor
	logger              *zap.Logger
	maxRetries          int
	retryDelay          time.Duration
	commitOnDLQFailure bool
	dlqFailCount        int
	lastFailOffset      int64
	maxDLQFails         int
}

type ConsumerOptions struct {
	MaxRetries          int
	RetryDelay          time.Duration
	DLQ                 *DLQWriter
	Monitor             *Monitor
	CommitOnDLQFailure bool
	MaxDLQFails         int
}

type consumerTransport struct {
	TLS *tls.Config
	SASL sasl.Mechanism
}

func NewConsumer(cfg Config, topic string, handler HandlerFunc, logger *zap.Logger, opts *ConsumerOptions) *Consumer {
	var transport consumerTransport
	if cfg.TLSEnabled {
		transport.TLS = &tls.Config{}
	}
	if cfg.SASLUser != "" && cfg.SASLPass != "" {
		mechanism, _ := scram.Mechanism(scram.SHA256, cfg.SASLUser, cfg.SASLPass)
		transport.SASL = mechanism
	}
	dialer := &kafka.Dialer{
		TLS: transport.TLS, 
		SASLMechanism: transport.SASL,
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  cfg.Brokers,
		GroupID:  cfg.GroupID,
		Topic:    topic,
		MinBytes: cfg.MinBytes,
		MaxBytes: cfg.MaxBytes,
		MaxWait:  cfg.MaxWait,

		CommitInterval: 0,
		StartOffset:    cfg.StartOffset,
		Logger: kafka.LoggerFunc(func(msg string, args ...any) {
			logger.Debug("kafka reader",
				zap.String("topic", topic),
				zap.String("msg", fmt.Sprintf(msg, args...)),
			)
		}),
		ErrorLogger: kafka.LoggerFunc(func(msg string, args ...any) {
			logger.Error("kafka reader error",
				zap.String("topic", topic),
				zap.String("msg", fmt.Sprintf(msg, args...)),
			)
		}),
		Dialer: dialer,
	})

	maxRetries := 3
	retryDelay := 500 * time.Millisecond
	if opts != nil {
		if opts.MaxRetries > 0 {
			maxRetries = opts.MaxRetries
		}
		if opts.RetryDelay > 0 {
			retryDelay = opts.RetryDelay
		}
	}

	c := &Consumer{
		reader:              reader,
		topic:               topic,
		groupID:             cfg.GroupID,
		handler:             handler,
		logger:              logger,
		maxRetries:          maxRetries,
		retryDelay:          retryDelay,
	}
	if opts != nil {
		c.dlq = opts.DLQ
		c.monitor = opts.Monitor
		c.commitOnDLQFailure = opts.CommitOnDLQFailure
		c.maxDLQFails = opts.MaxDLQFails
	}
	return c
}

func (c *Consumer) SetMonitor(monitor *Monitor) {
	c.monitor = monitor
}

func (c *Consumer) Run(ctx context.Context) error {
	c.logger.Info("kafka consumer started",
		zap.String("topic", c.topic),
	)
	defer c.logger.Info("kafka consumer stopped", zap.String("topic", c.topic))

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil
			}
			c.logger.Error("kafka fetch failed", zap.String("topic", c.topic), zap.Error(err))
			if c.monitor != nil {
				c.monitor.RecordFetchError(c.topic, c.groupID)
			}

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}
		c.logger.Debug("kafka message received",
			zap.String("topic", c.topic),
			zap.Int("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
			zap.String("key", string(msg.Key)),
			zap.Int("bytes", len(msg.Value)),
		)

		safeToCommit := c.processMessage(ctx, msg)
		if safeToCommit || c.commitOnDLQFailure {
			if err := c.reader.CommitMessages(ctx, msg); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				c.logger.Error("kafka commit failed",
					zap.String("topic", c.topic),
					zap.Int64("offset", msg.Offset),
					zap.Error(err),
				)
			}
		} else {
			c.logger.Warn("skipping commit - DLQ fail",
				zap.String("topic", c.topic),
				zap.Int64("offset", msg.Offset),
			)
		}
	}

}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) bool {
	start := time.Now()
	handlerErr, attempts := c.handleWithRetry(ctx, msg)
	duration := time.Since(start)

	if handlerErr == nil {
		if c.monitor != nil {
			c.monitor.RecordProcessing(c.topic, c.groupID, duration, nil, "")
			metricMessagesProcessed.WithLabelValues(c.topic, c.groupID).Inc()
		}
		return true
	}

	reason := classifyError(handlerErr)
	c.logger.Error("kafka handler failed after all retries",
		zap.String("topic", c.topic),
		zap.Int64("offset", msg.Offset),
		zap.String("key", string(msg.Key)),
		zap.Int("attempts", attempts),
		zap.String("reason", reason),
		zap.Error(handlerErr),
	)

	if c.monitor != nil {
		c.monitor.RecordProcessing(c.topic, c.groupID, duration, handlerErr, reason)
	}

	if c.dlq == nil {
		c.logger.Warn("message skipped - no DLQ configured",
			zap.String("topic", c.topic),
			zap.Int64("offset", msg.Offset),
			zap.Error(handlerErr),
		)
		return true
	}

	dlqStart := time.Now()
	dlqErr := c.dlq.Send(ctx, msg, handlerErr, attempts)
	dlqDuration := time.Since(dlqStart)

	if dlqErr != nil {
		c.logger.Error("CRITICAL: handler and DLQ both failed",
			zap.String("topic", c.topic),
			zap.Int64("offset", msg.Offset),
			zap.Error(dlqErr),
		)

		// reset counter nếu đây là offset mới
		if msg.Offset != c.lastFailOffset {
			c.dlqFailCount = 0
			c.lastFailOffset = msg.Offset
		}
		c.dlqFailCount++

		// force commit sau maxDLQ fails lần thất bại liên tiếp trên cùng offset để không block các message quan trọng khác
		if c.maxDLQFails > 0 && c.dlqFailCount >= c.maxDLQFails {
			c.logger.Error("POISON PILL: force commiting after max DLQ failures - message permanently lost",
				zap.String("topic", c.topic),
				zap.Int64("offset", msg.Offset),
				zap.Int("dlqFailCount", c.dlqFailCount),
				zap.String("payload", previewBytes(msg.Value, 500)),
			)
			c.dlqFailCount = 0
			return true
		}
		return false
	} else if c.monitor != nil {
		c.monitor.RecordDLQ(c.topic, c.groupID, reason, dlqDuration)
	}
	return true
}

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message) (error, int) {
	var lastErr error

	for attempt := 1; attempt <= c.maxRetries+1; attempt++ {
		if attempt > 1 {
			delay := c.retryDelay * time.Duration(1<<uint(attempt-2)) // khởi đầu 0.5s, lên 1s, 2s, 4s,...
			c.logger.Warn("kafka handler retry",
				zap.String("topic", c.topic),
				zap.Int("attempt", attempt),
				zap.Int("max", c.maxRetries+1),
				zap.Duration("delay", delay),
				zap.Error(lastErr),
			)
			select {
			case <-ctx.Done():
				return ctx.Err(), attempt
			case <-time.After(delay):
			}
		}

		if err := c.handler(ctx, msg); err != nil {
			lastErr = err
			continue
		}
		return nil, attempt
	}
	return fmt.Errorf("all %d handler attepts failed: %w", c.maxRetries+1, lastErr), c.maxRetries + 1
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func (c *Consumer) Stats() kafka.ReaderStats {
	return c.reader.Stats()
}

func previewBytes(b []byte, maxLen int) string {
	if len(b) <= maxLen {
		return string(b)
	}
	return string(b[:maxLen]) + fmt.Sprintf("... [%d bytes total]", len(b))
}
