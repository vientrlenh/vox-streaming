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

const (
	// How long EnsureTopics waits for a freshly created topic to become visible in
	// broker metadata. Creation is asynchronous, so a read immediately after
	// CreateTopics can legitimately still miss it.
	topicVerifyTimeout  = 5 * time.Second
	topicVerifyInterval = 250 * time.Millisecond
)

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

	existing, err := topicPartitionCounts(controllerCon)
	if err != nil {
		return fmt.Errorf("kafka read topics: %w", err)
	}

	// Only genuinely absent topics go into the CreateTopics batch. Including ones
	// that already exist earns a TOPIC_ALREADY_EXISTS back, and kafka-go collapses
	// the broker's per-topic response array into the first error it finds -- so a
	// single pre-existing topic used to mask a real failure on every other topic in
	// the same request, after which the loop below reported all of them as ensured.
	var missing []kafka.TopicConfig
	for _, spec := range allSpecs {
		partitions, present := existing[spec.Name]
		if !present {
			missing = append(missing, topicConfigOf(spec))
			continue
		}
		if partitions < spec.NumPartitions {
			// Reported rather than repaired: CreateTopics cannot widen a topic that
			// already exists. Worth saying out loud because the usual cause is a
			// topic auto-created by whichever producer touched it first, which lands
			// with the broker's default single partition -- capping consumer
			// parallelism and quietly making the Hash balancer's per-schedule
			// ordering guarantee vacuous.
			logger.Warn("kafka topic has fewer partitions than required",
				zap.String("topic", spec.Name),
				zap.Int("actual", partitions),
				zap.Int("required", spec.NumPartitions),
			)
		}
	}

	if len(missing) == 0 {
		logger.Info("kafka topics verified", zap.Int("topics", len(allSpecs)))
		return nil
	}

	names := make([]string, 0, len(missing))
	for _, topicCfg := range missing {
		names = append(names, topicCfg.Topic)
	}
	logger.Info("creating missing kafka topics", zap.Strings("topics", names))

	// A concurrent instance may have created the same topic between the read above
	// and this call; that specific race is the only reason to still tolerate the
	// already-exists error here.
	if err := controllerCon.CreateTopics(missing...); err != nil && !isTopicExistsError(err) {
		return fmt.Errorf("kafka create topics %v: %w", names, err)
	}

	// CreateTopics returning cleanly is not proof the topics are usable: creation is
	// asynchronous and metadata takes a moment to reach the broker we are talking
	// to. Confirm against the broker instead of declaring success on the strength of
	// the request having been accepted.
	stillMissing, err := waitForTopics(ctx, controllerCon, names)
	if err != nil {
		return fmt.Errorf("kafka verify topics: %w", err)
	}
	if len(stillMissing) > 0 {
		return fmt.Errorf("kafka topics still missing after create: %s", strings.Join(stillMissing, ", "))
	}

	logger.Info("kafka topics created", zap.Strings("topics", names))
	return nil
}

func topicConfigOf(spec TopicSpec) kafka.TopicConfig {
	return kafka.TopicConfig{
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
	}
}

// topicPartitionCounts maps every topic the broker knows about to its partition
// count. Partition counts rather than a bare set because an existing topic with
// too few partitions is a distinct problem from a missing one, and only the
// former is invisible without counting.
func topicPartitionCounts(conn *kafka.Conn) (map[string]int, error) {
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int, len(partitions))
	for _, partition := range partitions {
		counts[partition.Topic]++
	}
	return counts, nil
}

// waitForTopics polls broker metadata until every name is visible, returning
// whichever are still absent once it stops waiting. A non-empty result is a real
// failure, not impatience: the poll only gives up after topicVerifyTimeout.
func waitForTopics(ctx context.Context, conn *kafka.Conn, names []string) ([]string, error) {
	deadline := time.Now().Add(topicVerifyTimeout)
	for {
		counts, err := topicPartitionCounts(conn)
		if err != nil {
			return nil, err
		}
		var absent []string
		for _, name := range names {
			if _, ok := counts[name]; !ok {
				absent = append(absent, name)
			}
		}
		if len(absent) == 0 || time.Now().After(deadline) {
			return absent, nil
		}
		select {
		case <-ctx.Done():
			return absent, ctx.Err()
		case <-time.After(topicVerifyInterval):
		}
	}
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
