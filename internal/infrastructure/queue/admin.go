package queue

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"go.uber.org/zap"
)

type TopicSpec struct {
	Name              string
	NumPartitions     int
	ReplicationFactor int

	// Time to keep the message, -1 if it is unlimited
	// Default frame event keeping time is 1 hour
	RetentionMS int64
}

var RequiredTopics = []TopicSpec{
	{
		Name:              domain.TopicFrameReady,
		NumPartitions:     12, // keep 12 consumers parallelism
		ReplicationFactor: 1,
		RetentionMS:       3600000,
	},
	{
		Name:              domain.TopicStreamStarted,
		NumPartitions:     4,
		ReplicationFactor: 1,
		RetentionMS:       86400000,
	},
	{
		Name:              domain.TopicStreamEnded,
		NumPartitions:     4,
		ReplicationFactor: 1,
		RetentionMS:       86400000,
	},
	{
		Name:              domain.TopicRecordingAssemblyRequested,
		NumPartitions:     4,
		ReplicationFactor: 1,
		RetentionMS:       604800000,
	},
	{
		Name:              domain.TopicRecordingPartChanged,
		NumPartitions:     4,
		ReplicationFactor: 1,
		RetentionMS:       604800000,
	},
	{
		Name:              domain.TopicScheduleClosed,
		NumPartitions:     2,
		ReplicationFactor: 1,
		RetentionMS:       3600000,
	},
	{
		Name:              domain.TopicAlertRaised,
		NumPartitions:     6,
		ReplicationFactor: 1,
		RetentionMS:       604800000, // 7 days
	},
}

// Floor on dead-letter retention. A DLQ message is only worth writing if someone
// can still find it afterwards, and some source topics keep their own data for as
// little as an hour.
const dlqRetentionMS int64 = 604800000 // 7 days

// dlqTopicSpecs derives the dead-letter topic for each required topic.
//
// These must be created explicitly: DLQWriter produces to "<topic>.dlq" (see
// NewDLQWriter) and nothing else ever declares them, so without this every DLQ
// write fails with UNKNOWN_TOPIC_OR_PARTITION -- the safety net looks wired up
// but catches nothing, and the only sign is the consumer's own "CRITICAL: handler
// and DLQ both failed" line, at exactly the moment you can least afford to lose
// the message.
//
// Derived from RequiredTopics instead of listed by hand so the two can never
// drift. A spare unused .dlq for a topic this service only ever produces to costs
// nothing next to a missing one.
func dlqTopicSpecs() []TopicSpec {
	specs := make([]TopicSpec, 0, len(RequiredTopics))
	for _, spec := range RequiredTopics {
		retention := spec.RetentionMS
		// Leave -1 (unlimited) alone rather than clamping it down to the floor.
		if retention >= 0 && retention < dlqRetentionMS {
			retention = dlqRetentionMS
		}
		specs = append(specs, TopicSpec{
			Name:              spec.Name + dlqSuffix,
			NumPartitions:     spec.NumPartitions,
			ReplicationFactor: spec.ReplicationFactor,
			RetentionMS:       retention,
		})
	}
	return specs
}

func EnsureTopics(ctx context.Context, cfg Config, brokers []string, logger *zap.Logger) error {
	if len(brokers) == 0 {
		return fmt.Errorf("broker is empty")
	}

	dialer := dialer(cfg)
	conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("kafka admin connect: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("kafka get controller: %w", err)
	}

	controllerCon, err := dialer.DialContext(ctx,
		"tcp",
		net.JoinHostPort(controller.Host, fmt.Sprint(controller.Port)),
	)
	if err != nil {
		return fmt.Errorf("kafka connect controller: %w", err)
	}
	defer controllerCon.Close()

	allSpecs := make([]TopicSpec, 0, len(RequiredTopics)*2)
	allSpecs = append(allSpecs, RequiredTopics...)
	allSpecs = append(allSpecs, dlqTopicSpecs()...)

	topicConfigs := make([]kafka.TopicConfig, 0, len(allSpecs))
	for _, spec := range allSpecs {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             spec.Name,
			NumPartitions:     spec.NumPartitions,
			ReplicationFactor: spec.ReplicationFactor,
			ConfigEntries: []kafka.ConfigEntry{
				{
					ConfigName:  "retention.ms",
					ConfigValue: fmt.Sprint(spec.RetentionMS),
				},
				{
					ConfigName:  "compression.type",
					ConfigValue: "snappy",
				},
			},
		})
	}

	if err := controllerCon.CreateTopics(topicConfigs...); err != nil {
		if !isTopicExistsError(err) {
			return fmt.Errorf("kafka create topics: %w", err)
		}
	}

	for _, spec := range allSpecs {
		logger.Info("kafka topic ensured",
			zap.String("topic", spec.Name),
			zap.Int("partitions", spec.NumPartitions),
			zap.Int64("retentionMs", spec.RetentionMS),
		)
	}
	return nil
}

func WaitForKafka(ctx context.Context, cfg Config, brokers []string, logger *zap.Logger) error {
	if len(brokers) == 0 {
		return fmt.Errorf("broker is empty")
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	logger.Info("waiting for kafka...", zap.Strings("brokers", brokers))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			dialer := dialer(cfg)
			conn, err := dialer.DialContext(ctx, "tcp", brokers[0])
			if err != nil {
				logger.Warn("kafka not ready, retrying...", zap.Error(err))
				continue
			}
			conn.Close()
			logger.Info("kafka is ready")
			return nil
		}
	}
}

// PingKafka dials the first broker to verify Kafka is reachable.
func PingKafka(ctx context.Context, cfg Config, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no brokers configured")
	}
	conn, err := dialer(cfg).DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func isTopicExistsError(err error) bool {
	return strings.Contains(err.Error(), "Topic with this name already exists")
}

func dialer(cfg Config) *kafka.Dialer {
	dialer := &kafka.Dialer{}
	if cfg.TLSEnabled && cfg.SASLUser != "" && cfg.SASLPass != "" {
		mechanism, _ := scram.Mechanism(scram.SHA256, cfg.SASLUser, cfg.SASLPass)
		dialer = &kafka.Dialer{
			SASLMechanism: mechanism,
			TLS:           &tls.Config{},
		}
	}
	return dialer
}
