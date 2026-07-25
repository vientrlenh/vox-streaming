package queue

import (
	"fmt"
	"testing"
)

func TestPreviewBytes(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		maxLen int
		want   string
	}{
		{"empty input", []byte{}, 10, ""},
		{"shorter than max is unchanged", []byte("hello"), 10, "hello"},
		{"exactly at max is unchanged", []byte("hello"), 5, "hello"},
		{
			"over max is truncated with a total-size suffix",
			[]byte("hello world"), 5,
			"hello" + fmt.Sprintf("... [%d bytes total]", len("hello world")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := previewBytes(tt.input, tt.maxLen); got != tt.want {
				t.Errorf("previewBytes(%q, %d) = %q, want %q", tt.input, tt.maxLen, got, tt.want)
			}
		})
	}
}
