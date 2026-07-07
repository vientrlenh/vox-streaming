package queue

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"go.uber.org/zap"
)

type Publisher struct {
	writers map[string]*kafka.Writer
	logger  *zap.Logger
}

func NewPublisher(cfg Config, logger *zap.Logger) (*Publisher, error) {
	topics := []string{
		domain.TopicFrameReady,
		domain.TopicStreamStarted,
		domain.TopicStreamEnded,
		domain.TopicRoomClosed, 
		domain.TopicAlertRaised,
	}

	// frame events are high-volume and loss-tolerant — async is fine
	// stream lifecycle events carry segment keys used by assembler — must always be sync
	asyncAllowed := map[string]bool{
		domain.TopicFrameReady: true,
	}

	writers := make(map[string]*kafka.Writer, len(topics))

	for _, topic := range topics {
		var transport *kafka.Transport
		if cfg.TLSEnabled || cfg.SASLUser != "" {
			mechanism, _ := scram.Mechanism(scram.SHA256, cfg.SASLUser, cfg.SASLPass)
			transport = &kafka.Transport{
				SASL: mechanism,
				TLS:  &tls.Config{},
			}
		}
		async := cfg.Async && asyncAllowed[topic]
		w := &kafka.Writer{
			Addr:         kafka.TCP(cfg.Brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{}, // đảm bảo ordering per room khi cùng room ID và cùng partition
			BatchSize:    cfg.BatchSize,
			BatchTimeout: cfg.BatchTimeout,
			Async:        async,
			RequiredAcks: kafka.RequiredAcks(cfg.RequiredAcks),
			MaxAttempts:  5,
			Compression:  kafka.Snappy,
			ErrorLogger: kafka.LoggerFunc(func(msg string, args ...interface{}) {
				logger.Error("kafka writer error",
					zap.String("topic", topic),
					zap.String("msg", fmt.Sprintf(msg, args...)),
				)
			}),
			Transport: transport,
		}

		writers[topic] = w
		logger.Info("kafka writer initialized",
			zap.String("topic", topic),
			zap.Bool("async", async),
		)
	}
	return &Publisher{
		writers: writers,
		logger:  logger,
	}, nil
}

func (p *Publisher) PublishFrameReady(ctx context.Context, event domain.FrameReadyEvent) error {
	return p.publish(ctx, domain.TopicFrameReady, event.RoomID, event)
}

func (p *Publisher) PublishStreamStarted(ctx context.Context, event domain.StreamStartedEvent) error {
	return p.publish(ctx, domain.TopicStreamStarted, event.RoomID, event)
}

func (p *Publisher) PublishStreamEnded(ctx context.Context, event domain.StreamEndedEvent) error {
	return p.publish(ctx, domain.TopicStreamEnded, event.RoomID, event)
}

func (p *Publisher) PublishRoomClosed(ctx context.Context, event domain.RoomClosedEvent) error {
	return p.publish(ctx, domain.TopicRoomClosed, event.RoomID, event)
}

func (p *Publisher) PublishAlertRaised(ctx context.Context, event domain.AlertRaisedEvent) error {
	return p.publish(ctx, domain.TopicAlertRaised, event.RoomID, event)
}

func (p *Publisher) publish(ctx context.Context, topic, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka published: marshal %T: %w", payload, err)
	}

	writer, ok := p.writers[topic]
	if !ok {
		return fmt.Errorf("kafka publish: no writer for topic %q", topic)
	}

	msg := kafka.Message{
		Key:   []byte(key),
		Value: data,
		Headers: []kafka.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "produced-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
		},
	}

	start := time.Now()
	if err := writer.WriteMessages(ctx, msg); err != nil {
		p.logger.Error("kafka published failed",
			zap.String("topic", topic),
			zap.String("key", key),
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(err),
		)
		return fmt.Errorf("kafka publish to %q: %w", topic, err)
	}

	p.logger.Debug("kafka published successfully",
		zap.String("topic", topic),
		zap.String("key", key),
		zap.Duration("elapsed", time.Since(start)),
		zap.Int("bytes", len(data)),
	)
	return nil
}

func (p *Publisher) Close() error {
	var lastErr error
	for topic, w := range p.writers {
		if err := w.Close(); err != nil {
			p.logger.Error("kafka writer close error",
				zap.String("topic", topic),
				zap.Error(err),
			)
			lastErr = err
		}
	}
	return lastErr
}
