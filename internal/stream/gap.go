package stream

import (
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

type SegmentGap struct {
	FromSeq int64
	ToSeq   int64
	Missing time.Duration
}

// auditGaps computes gaps (>2s between consecutive segments) and total
// recorded duration (sum of each segment's own span). metas must already be
// sorted by Seq — cache.SegmentRegistry.List guarantees this. Shared between
// Audit and AssemblerUseCase.Assemble so both use the same gap definition.
func AuditGaps(metas []cache.SegmentMeta) ([]SegmentGap, time.Duration) {
	var gaps []SegmentGap
	var totalRecorded time.Duration
	for i, m := range metas {
		totalRecorded += m.EndedAt.Sub(m.StartedAt)
		if i == 0 {
			continue
		}
		prev := metas[i-1]
		gap := m.StartedAt.Sub(prev.EndedAt)
		if gap > 2*time.Second {
			gaps = append(gaps, SegmentGap{
				FromSeq: prev.Seq,
				ToSeq:   m.Seq,
				Missing: gap,
			})
		}
	}
	return gaps, totalRecorded
}