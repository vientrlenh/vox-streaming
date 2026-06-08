package queue

import (
	"time"
)

type Config struct {
	Brokers        []string      `mapstructure:"brokers"`
	GroupID        string        `mapstructure:"group_id"`
	BatchSize      int           `mapstructure:"batch_size"`
	BatchTimeout   time.Duration `mapstructure:"batch_timeout"`
	Async          bool          `mapstructure:"async"`
	RequiredAcks   int           `mapstructure:"required_acks"`
	MinBytes       int           `mapstructure:"min_bytes"`
	MaxBytes       int           `mapstructure:"max_bytes"`
	CommitInterval time.Duration `mapstructure:"commit_interval"`
	StartOffset    int64         `mapstructure:"start_offset"`
	MaxWait        time.Duration `mapstructure:"max_wait"`
	TLSEnabled     bool          `mapstructure:"tls_enabled"`
	SASLUser       string        `mapstructure:"sasl_user"`
	SASLPass       string        `mapstructure:"sasl_pass"`
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