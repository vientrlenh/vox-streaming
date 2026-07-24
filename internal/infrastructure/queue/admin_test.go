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
