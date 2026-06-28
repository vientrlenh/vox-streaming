package queue

import (
	"time"
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
		StartOffset:    -1,
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