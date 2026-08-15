package queue

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"go.uber.org/zap"
)

const (
	// Shortest gap between two EnsureTopics passes driven by publish failures. A
	// broker that came back without its topics fails every publish at once, and one
	// pass repairs all of them, so the rest must not each fire their own
	// CreateTopics round trip.
	topicRecoveryCooldown = 30 * time.Second

	// Ceiling on a single recovery pass. Publishes on other topics queue behind it,
	// so it has to fail fast rather than inherit the caller's deadline.
	topicRecoveryTimeout = 10 * time.Second
)

type Publisher struct {
	writers map[string]*kafka.Writer
	logger  *zap.Logger

	// Topics are otherwise only ensured once, at startup. A broker that restarts
	// on empty storage therefore stays broken for the whole remaining lifetime of
	// this process, with every publish failing on a topic nothing will recreate --
	// so the publisher keeps what it needs to run EnsureTopics again itself.
	cfg            Config
	recoveryMu     sync.Mutex
	lastRecovery   time.Time
	lastRecoveryOK bool
}

func NewPublisher(cfg Config, logger *zap.Logger) (*Publisher, error) {
	topics := []string{
		domain.TopicFrameReady,
		domain.TopicStreamStarted,
		domain.TopicStreamEnded,
		domain.TopicRecordingAssemblyRequested,
		domain.TopicRecordingPartChanged,
		domain.TopicScheduleClosed,
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
			Balancer:     &kafka.Hash{}, // make sure per schedule ordering when scheduleID matches and partition matches
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
		}
		// Only attach a Transport when one was actually built (TLS/SASL). Assigning
		// a typed-nil *kafka.Transport to the RoundTripper interface field makes the
		// interface non-nil, so kafka-go skips its DefaultTransport fallback and then
		// dereferences the nil pointer -> panic on the first WriteMessages.
		if transport != nil {
			w.Transport = transport
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
		cfg:     cfg,
	}, nil
}

func (p *Publisher) PublishFrameReady(ctx context.Context, event domain.FrameReadyEvent) error {
	return p.publish(ctx, domain.TopicFrameReady, event.ScheduleID, event)
}

func (p *Publisher) PublishStreamStarted(ctx context.Context, event domain.StreamStartedEvent) error {
	return p.publish(ctx, domain.TopicStreamStarted, event.ScheduleID, event)
}

func (p *Publisher) PublishStreamEnded(ctx context.Context, event domain.StreamEndedEvent) error {
	return p.publish(ctx, domain.TopicStreamEnded, event.ScheduleID, event)
}

func (p *Publisher) PublishScheduleClosed(ctx context.Context, event domain.ScheduleClosedEvent) error {
	return p.publish(ctx, domain.TopicScheduleClosed, event.ScheduleID, event)
}

func (p *Publisher) PublishRecordingAssemblyRequested(ctx context.Context, event domain.RecordingAssemblyRequestedEvent) error {
	return p.publish(ctx, domain.TopicRecordingAssemblyRequested, event.StreamID, event)
}

func (p *Publisher) PublishRecordingPartChanged(ctx context.Context, event domain.RecordingPartChangedEvent) error {
	return p.publish(ctx, domain.TopicRecordingPartChanged, event.StreamID, event)
}

func (p *Publisher) PublishAlertRaised(ctx context.Context, event domain.AlertRaisedEvent) error {
	return p.publish(ctx, domain.TopicAlertRaised, event.SessionID, event)
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

	headers := []kafka.Header{
		{Key: "content-type", Value: []byte("application/json")},
		{Key: "produced-at", Value: []byte(time.Now().UTC().Format(time.RFC3339))},
	}
	if topic == domain.TopicRecordingPartChanged {
		// Spring's JSON consumer uses this standard type header to deserialize
		// events produced by kafka-go into a generic map.
		headers = append(headers, kafka.Header{Key: "__TypeId__", Value: []byte("java.util.LinkedHashMap")})
	}
	msg := kafka.Message{
		Key:     []byte(key),
		Value:   data,
		Headers: headers,
	}

	start := time.Now()
	err = writer.WriteMessages(ctx, msg)
	// A missing topic is the one publish failure this service can repair on the
	// spot, so it gets exactly one retry behind a recovery pass. Every other error
	// (broker down, timeout, serialization) falls straight through -- retrying those
	// here would only duplicate what the writer's own MaxAttempts already does.
	if isUnknownTopicError(err) && p.recoverTopics(ctx, topic) {
		err = writer.WriteMessages(ctx, msg)
	}
	if err != nil {
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

// isUnknownTopicError reports whether err is the broker refusing a produce for a
// topic it does not have.
//
// Matched on the wire description as well as on the sentinel because kafka-go
// reaches this code through more than one shape: the bare Error from a direct
// produce, and aggregate types wrapping per-message failures that do not always
// unwrap back to it.
func isUnknownTopicError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kafka.UnknownTopicOrPartition) {
		return true
	}
	return strings.Contains(err.Error(), kafka.UnknownTopicOrPartition.Error())
}

// recoverTopics re-runs EnsureTopics after a publish found its topic missing, and
// reports whether the caller has any reason to retry.
//
// Serialized and rate-limited on purpose. A broker that lost its topics fails
// every in-flight publish at once, and a single pass repairs all of them, so
// callers that arrive during a pass wait for it and then reuse its verdict rather
// than each issuing their own CreateTopics.
//
// Only reachable for synchronous writers: an async writer returns nil from
// WriteMessages and surfaces failures through its ErrorLogger instead, so
// frame.ready gets no repair from here when cfg.Async is on.
func (p *Publisher) recoverTopics(ctx context.Context, topic string) bool {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()

	if time.Since(p.lastRecovery) < topicRecoveryCooldown {
		// The cooldown exists to throttle CreateTopics, not to deny a retry that a
		// just-completed pass already earned -- so callers queued behind a successful
		// one are told to go ahead.
		return p.lastRecoveryOK
	}
	p.lastRecovery = time.Now()
	p.lastRecoveryOK = false

	p.logger.Warn("publish hit a missing topic, re-ensuring topics", zap.String("topic", topic))

	// Detached from the caller's cancellation. The publish that triggers this is
	// often the last act of a closing peer, and a topic that needs creating must
	// still get created -- if not for this message, then for every one after it.
	ensureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), topicRecoveryTimeout)
	defer cancel()

	if err := EnsureTopics(ensureCtx, p.cfg, p.cfg.Brokers, p.logger); err != nil {
		p.logger.Error("topic re-ensure failed",
			zap.String("topic", topic),
			zap.Error(err),
		)
		return false
	}

	p.lastRecoveryOK = true
	p.logger.Info("topics re-ensured after missing-topic publish failure", zap.String("topic", topic))
	return true
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
