package queue

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers        []string      `mapstructure:"brokers"`
	GroupID        string        `mapstructure:"groupId"`
	BatchSize      int           `mapstructure:"batchSize"`
	BatchTimeout   time.Duration `mapstructure:"batchTimeout"`
	Async          bool          `mapstructure:"async"`
	RequiredAcks   int           `mapstructure:"requiredAcks"`
	MinBytes       int           `mapstructure:"minBytes"`
	MaxBytes       int           `mapstructure:"maxBytes"`
	CommitInterval time.Duration `mapstructure:"commitInterval"`
	StartOffset    int64         `mapstructure:"startOffset"`
	MaxWait        time.Duration `mapstructure:"maxWait"`
	TLSEnabled     bool          `mapstructure:"tlsEnabled"`
	SASLUser       string        `mapstructure:"saslUser"`
	SASLPass       string        `mapstructure:"saslPass"`
}

func DefaultConfig(brokers []string, groupID string) Config {
	return Config{
		Brokers:        brokers,
		GroupID:        groupID,
		BatchSize:      100,
		BatchTimeout:   10 * time.Millisecond,
		Async:          false,
		RequiredAcks:   -1,
		MinBytes:       1,
		MaxBytes:       10 * 1024 * 1024,
		CommitInterval: time.Second,
		// FirstOffset, not LastOffset, is the only safe default for a group that has no
		// committed offset yet -- which is every group on a fresh deployment, and every
		// group right up until its first successful commit. With LastOffset such a group
		// silently jumps to the end of the log, so anything already produced is never
		// seen. That quietly voided the whole CommitOnDLQFailure=false contract ("skip
		// the commit so the message gets redelivered") for the very first message a
		// group ever handled: the message stayed in the topic uncommitted, and was
		// skipped anyway on the next start. Consumers whose data genuinely goes stale
		// (frame.ready) override this back to LastOffset at their own call site.
		StartOffset:    kafka.FirstOffset,
		MaxWait:        500 * time.Millisecond,
	}
}

func NewConfig(cfg Config, tlsEnabled bool, saslUser, saslPass string) Config {
	return Config{
		Brokers: cfg.Brokers, 
		GroupID: cfg.GroupID, 
		BatchSize: cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout, 
		Async: cfg.Async, 
		RequiredAcks: cfg.RequiredAcks, 
		MinBytes: cfg.MinBytes, 
		MaxBytes: cfg.MaxBytes, 
		CommitInterval: cfg.CommitInterval, 
		StartOffset: cfg.StartOffset, 
		MaxWait: cfg.MaxWait, 
		TLSEnabled: tlsEnabled, 
		SASLUser: saslUser, 
		SASLPass: saslPass,
	}
}