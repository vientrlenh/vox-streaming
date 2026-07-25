package queue

import (
	"errors"
	"testing"
)

func TestIsTopicExistsError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"exact topic-exists message", errors.New("Topic with this name already exists"), true},
		{"topic-exists message wrapped with more context", errors.New("kafka create topics: Topic with this name already exists"), true},
		{"unrelated error", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTopicExistsError(tt.err); got != tt.want {
				t.Errorf("isTopicExistsError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// Every topic a DLQWriter can produce to has to exist, or the safety net silently
// catches nothing (UNKNOWN_TOPIC_OR_PARTITION on write). Derived from
// RequiredTopics so the two lists cannot drift apart.
func TestDLQTopicSpecsCoverEveryRequiredTopic(t *testing.T) {
	specs := dlqTopicSpecs()
	if len(specs) != len(RequiredTopics) {
		t.Fatalf("got %d dlq specs for %d required topics", len(specs), len(RequiredTopics))
	}

	byName := make(map[string]TopicSpec, len(specs))
	for _, s := range specs {
		byName[s.Name] = s
	}
	for _, src := range RequiredTopics {
		want := src.Name + dlqSuffix
		got, ok := byName[want]
		if !ok {
			t.Errorf("no dlq topic derived for %q (expected %q)", src.Name, want)
			continue
		}
		if got.NumPartitions != src.NumPartitions || got.ReplicationFactor != src.ReplicationFactor {
			t.Errorf("%s: got %d partitions / rf %d, want %d / %d",
				want, got.NumPartitions, got.ReplicationFactor, src.NumPartitions, src.ReplicationFactor)
		}
		// A dead letter has to outlive a short-lived source topic: frame.ready keeps
		// only an hour, which would evaporate long before anyone looked at the DLQ.
		if got.RetentionMS >= 0 && got.RetentionMS < dlqRetentionMS {
			t.Errorf("%s: retention %d is below the %d floor", want, got.RetentionMS, dlqRetentionMS)
		}
	}
}

func TestDLQTopicSpecsKeepUnlimitedRetention(t *testing.T) {
	original := RequiredTopics
	t.Cleanup(func() { RequiredTopics = original })
	RequiredTopics = []TopicSpec{
		{Name: "forever", NumPartitions: 1, ReplicationFactor: 1, RetentionMS: -1},
	}

	specs := dlqTopicSpecs()
	if len(specs) != 1 || specs[0].RetentionMS != -1 {
		t.Fatalf("unlimited retention must not be clamped to the floor, got %+v", specs)
	}
}
