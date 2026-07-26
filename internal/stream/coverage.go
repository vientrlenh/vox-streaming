package stream

import (
	"fmt"
	"slices"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

// Anomaly kinds. Deliberately descriptive rather than a verdict: every one of these has an innocent
// explanation as well as a damning one, and deciding which is a human's job.
const (
	AnomalyMissing      = "missing"        // declared by the client, never arrived
	AnomalyNotCaptured  = "not_captured"   // segment exists but contains no video frames at all
	AnomalyLowFrameRate = "low_frame_rate" // far fewer frames than the rest of this stream
	AnomalyEmpty        = "empty"          // zero bytes
)

// A segment whose frame rate falls below this fraction of the stream's own median is flagged. The
// baseline is the stream's median rather than a configured frame rate so it self-calibrates to
// whatever the machine actually managed, and so a uniformly slow machine is not reported as
// hundreds of anomalies.
const lowFrameRateFraction = 0.5

type CoverageAnomaly struct {
	Seq    int64  `json:"seq"`
	Kind   string `json:"kind"`
	Detail string `json:"detail"`
}

// StreamCoverage is what the client says it captured measured against what actually arrived.
//
// Declared == 0 means the client never sent an inventory (an older build, or one that died before
// its first declaration). Everything below then falls back to what can be inferred from the
// received segments alone, which is strictly weaker: a missing head or tail is invisible that way,
// because a set of segments that simply starts late or stops early looks exactly like a complete
// one.
type StreamCoverage struct {
	DeclaredSegments int               `json:"declaredSegments"`
	ReceivedSegments int               `json:"receivedSegments"`
	MissingSeqs      []int64           `json:"missingSeqs"`
	Anomalies        []CoverageAnomaly `json:"anomalies"`
	HasInventory     bool              `json:"hasInventory"`
	ClientComplete   bool              `json:"clientComplete"`
}

func (c StreamCoverage) HasGaps() bool {
	return len(c.MissingSeqs) > 0
}

// Reconcile subtracts what arrived from what the client declared.
//
// This is the whole point of the inventory: with it, a gap is a set difference over sequence
// numbers -- exact, and immune to the two blind spots of inferring gaps from timestamps alone
// (a missing first or last segment leaves no interval to measure). Without it, the timestamp
// heuristic is all there is.
func Reconcile(inventory *cache.StreamInventory, received []cache.SegmentMeta) StreamCoverage {
	coverage := StreamCoverage{
		ReceivedSegments: len(received),
		MissingSeqs:      []int64{},
		Anomalies:        []CoverageAnomaly{},
	}

	receivedSeqs := make(map[int64]struct{}, len(received))
	for _, meta := range received {
		receivedSeqs[meta.Seq] = struct{}{}
	}

	if inventory == nil || len(inventory.Segments) == 0 {
		// Fall back to the interior-gap heuristic. It cannot see a missing head or tail, which is
		// exactly why the inventory exists.
		gaps, _ := AuditGaps(received)
		for _, gap := range gaps {
			coverage.Anomalies = append(coverage.Anomalies, CoverageAnomaly{
				Seq:  gap.ToSeq,
				Kind: AnomalyMissing,
				Detail: fmt.Sprintf("%.0fs unaccounted for between seq %d and %d",
					gap.Missing.Seconds(), gap.FromSeq, gap.ToSeq),
			})
			coverage.MissingSeqs = append(coverage.MissingSeqs, gap.ToSeq)
		}
		return coverage
	}

	coverage.HasInventory = true
	coverage.ClientComplete = inventory.Complete
	coverage.DeclaredSegments = len(inventory.Segments)

	for _, declared := range inventory.Segments {
		if _, arrived := receivedSeqs[declared.Seq]; !arrived {
			coverage.MissingSeqs = append(coverage.MissingSeqs, declared.Seq)
			coverage.Anomalies = append(coverage.Anomalies, CoverageAnomaly{
				Seq:    declared.Seq,
				Kind:   AnomalyMissing,
				Detail: "the client captured this segment but it never reached storage",
			})
		}
	}
	slices.Sort(coverage.MissingSeqs)

	coverage.Anomalies = append(coverage.Anomalies, captureAnomalies(inventory.Segments)...)
	return coverage
}

// captureAnomalies looks for segments that arrived intact yet hold no usable picture -- a frozen
// capture, a covered camera, an encoder starved of CPU. Sequence-level gap analysis cannot see any
// of this: the segment is present, correctly numbered and correctly sized in every respect except
// the one that matters.
func captureAnomalies(declared []cache.DeclaredSegment) []CoverageAnomaly {
	anomalies := []CoverageAnomaly{}
	baseline := medianFrameRate(declared)

	for _, segment := range declared {
		if segment.SizeBytes == 0 {
			anomalies = append(anomalies, CoverageAnomaly{
				Seq:    segment.Seq,
				Kind:   AnomalyEmpty,
				Detail: "segment is zero bytes",
			})
			continue
		}

		duration := segment.EndedAt.Sub(segment.StartedAt)
		if duration <= 0 {
			continue
		}

		if segment.FramesWritten == 0 {
			anomalies = append(anomalies, CoverageAnomaly{
				Seq:  segment.Seq,
				Kind: AnomalyNotCaptured,
				Detail: fmt.Sprintf("no video frames across %.0fs; the capture source produced nothing",
					duration.Seconds()),
			})
			continue
		}

		if baseline <= 0 {
			continue
		}

		rate := float64(segment.FramesWritten) / duration.Seconds()
		if rate < baseline*lowFrameRateFraction {
			anomalies = append(anomalies, CoverageAnomaly{
				Seq:  segment.Seq,
				Kind: AnomalyLowFrameRate,
				Detail: fmt.Sprintf("%.1f fps against this stream's usual %.1f fps", rate, baseline),
			})
		}
	}
	return anomalies
}

// medianFrameRate is the stream's own typical frames per second. Median rather than mean so a
// handful of frozen segments cannot drag the baseline down to meet themselves.
func medianFrameRate(declared []cache.DeclaredSegment) float64 {
	rates := make([]float64, 0, len(declared))
	for _, segment := range declared {
		duration := segment.EndedAt.Sub(segment.StartedAt)
		if duration <= 0 || segment.FramesWritten <= 0 {
			continue
		}
		rates = append(rates, float64(segment.FramesWritten)/duration.Seconds())
	}
	if len(rates) == 0 {
		return 0
	}

	slices.Sort(rates)
	middle := len(rates) / 2
	if len(rates)%2 == 1 {
		return rates[middle]
	}
	return (rates[middle-1] + rates[middle]) / 2
}

// RecordedDuration is the wall-clock span covered by the recording: first start to last end, the
// same measure the service has always reported.
//
// Measured over the client's declared segments when there are any, so that a recording truncated by
// segments that never arrived still reports how long the exam actually ran rather than only the
// part that survived the upload. Falls back to the received segments otherwise.
func RecordedDuration(inventory *cache.StreamInventory, received []cache.SegmentMeta) time.Duration {
	if inventory != nil && len(inventory.Segments) > 0 {
		segments := inventory.Segments
		return segments[len(segments)-1].EndedAt.Sub(segments[0].StartedAt)
	}
	if len(received) == 0 {
		return 0
	}
	return received[len(received)-1].EndedAt.Sub(received[0].StartedAt)
}
