package stream

import "time"

type StreamAudit struct {
	StreamID         string
	TotalSegments    int
	RecordedDuration time.Duration
	Gaps             []SegmentGap
	HasGaps          bool
	Coverage         StreamCoverage
}