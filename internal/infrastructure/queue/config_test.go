package queue

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	brokers := []string{"broker-1:9092", "broker-2:9092"}
	got := DefaultConfig(brokers, "my-group")

	if len(got.Brokers) != 2 || got.Brokers[0] != brokers[0] {
		t.Errorf("got Brokers=%v, want %v", got.Brokers, brokers)
	}
	if got.GroupID != "my-group" {
		t.Errorf("got GroupID=%q, want my-group", got.GroupID)
	}
	if got.BatchSize != 100 {
		t.Errorf("got BatchSize=%d, want 100", got.BatchSize)
	}
	if got.BatchTimeout != 10*time.Millisecond {
		t.Errorf("got BatchTimeout=%v, want 10ms", got.BatchTimeout)
	}
	if got.Async {
		t.Error("got Async=true, want false (default is synchronous, safer delivery)")
	}
	if got.RequiredAcks != -1 {
		t.Errorf("got RequiredAcks=%d, want -1 (require all replicas)", got.RequiredAcks)
	}
	if got.MinBytes != 1 {
		t.Errorf("got MinBytes=%d, want 1", got.MinBytes)
	}
	if got.MaxBytes != 10*1024*1024 {
		t.Errorf("got MaxBytes=%d, want 10MB", got.MaxBytes)
	}
	if got.CommitInterval != time.Second {
		t.Errorf("got CommitInterval=%v, want 1s", got.CommitInterval)
	}
	if got.StartOffset != -1 {
		t.Errorf("got StartOffset=%d, want -1", got.StartOffset)
	}
	if got.MaxWait != 500*time.Millisecond {
		t.Errorf("got MaxWait=%v, want 500ms", got.MaxWait)
	}
	if got.TLSEnabled {
		t.Error("got TLSEnabled=true, want false by default")
	}
}

func TestNewConfig(t *testing.T) {
	base := DefaultConfig([]string{"broker-1:9092"}, "my-group")

	got := NewConfig(base, true, "user1", "pass1")

	if got.Brokers[0] != base.Brokers[0] || got.GroupID != base.GroupID ||
		got.BatchSize != base.BatchSize || got.BatchTimeout != base.BatchTimeout ||
		got.RequiredAcks != base.RequiredAcks || got.MinBytes != base.MinBytes ||
		got.MaxBytes != base.MaxBytes || got.CommitInterval != base.CommitInterval ||
		got.StartOffset != base.StartOffset || got.MaxWait != base.MaxWait {
		t.Errorf("got %+v, want base fields preserved from %+v", got, base)
	}
	if !got.TLSEnabled {
		t.Error("got TLSEnabled=false, want true (as passed in)")
	}
	if got.SASLUser != "user1" || got.SASLPass != "pass1" {
		t.Errorf("got SASLUser=%q SASLPass=%q, want user1/pass1", got.SASLUser, got.SASLPass)
	}
}
