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

	// Thời gian giữ message, nếu để -1 thì là không giới hạn
	// Mặc định thời gian giữ frame event là 1 giờ, record thì giữ lâu hơn
	RetentionMS int64
}

var RequiredTopics = []TopicSpec{
	{
		Name:              domain.TopicFrameReady,
		NumPartitions:     12, // giữ tối đa 12 consumer song song
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
		Name:              domain.TopicRoomClosed,
		NumPartitions:     2,
		ReplicationFactor: 1,
		RetentionMS:       3600000,
	},
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

	topicConfigs := make([]kafka.TopicConfig, 0, len(RequiredTopics))
	for _, spec := range RequiredTopics {
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

	for _, spec := range RequiredTopics {
		logger.Info("kafka topic ensured",
			zap.String("topic", spec.Name),
			zap.Int("partitions", spec.NumPartitions),
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


func isTopicExistsError(err error) bool {
	return strings.Contains(err.Error(), "Topic with this name already exists")
}

func dialer(cfg Config) *kafka.Dialer {
	dialer := &kafka.Dialer{}
	if cfg.TLSEnabled || cfg.SASLUser != "" {
		mechanism, _ := scram.Mechanism(scram.SHA256, cfg.SASLUser, cfg.SASLPass)
		dialer = &kafka.Dialer{
			SASLMechanism: mechanism, 
			TLS: &tls.Config{},
		}
	}
	return dialer
}