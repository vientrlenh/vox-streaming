package queue

import (
	"errors"
	"testing"
)

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil error", nil, "unknown"},
		{"unmarshal error", errors.New("json: cannot unmarshal string into Go value"), "deserialization_error"},
		{"invalid payload", errors.New("invalid payload syntax"), "deserialization_error"},
		{"timeout error", errors.New("operation timeout after 5s"), "timeout_error"},
		{"deadline exceeded", errors.New("context deadline exceeded"), "timeout_error"},
		{"network error", errors.New("dial tcp: connection refused"), "network_error"},
		{"not found error", errors.New("record not found"), "not_found_error"},
		{"http 404", errors.New("upstream returned 404"), "not_found_error"},
		{"auth error", errors.New("request unauthorized"), "auth_error"},
		{"http 403", errors.New("upstream returned 403"), "auth_error"},
		{"unclassified error", errors.New("something else went wrong"), "processing_error"},
		{
			"deserialization takes priority over timeout when both match",
			errors.New("json unmarshal failed: context deadline exceeded"),
			"deserialization_error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyError(tt.err); got != tt.want {
				t.Errorf("classifyError(%v) = %q, want %q", tt.err, got, tt.want)
			}
		})
	}
}

func TestContains(t *testing.T) {
	tests := []struct {
		name string
		s    string
		subs []string
		want bool
	}{
		{"match in middle", "hello world", []string{"lo wo"}, true},
		{"match at start", "hello world", []string{"hello"}, true},
		{"match at end", "hello world", []string{"world"}, true},
		{"no match", "hello world", []string{"xyz"}, false},
		{"sub longer than s", "hi", []string{"hello"}, false},
		{"empty s", "", []string{"x"}, false},
		{"matches any of several subs", "hello world", []string{"nope", "world"}, true},
		{"empty subs list matches nothing", "hello world", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := contains(tt.s, tt.subs...); got != tt.want {
				t.Errorf("contains(%q, %v) = %v, want %v", tt.s, tt.subs, got, tt.want)
			}
		})
	}
}
