package stream

import (
	"testing"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

var base = time.Date(2026, 7, 26, 3, 32, 0, 0, time.UTC)

func declared(seq int64, frames int64) cache.DeclaredSegment {
	return cache.DeclaredSegment{
		Seq:           seq,
		StartedAt:     base.Add(time.Duration(seq) * 10 * time.Second),
		EndedAt:       base.Add(time.Duration(seq+1) * 10 * time.Second),
		SizeBytes:     1_500_000,
		FramesWritten: frames,
	}
}

func received(seqs ...int64) []cache.SegmentMeta {
	metas := make([]cache.SegmentMeta, 0, len(seqs))
	for _, seq := range seqs {
		metas = append(metas, cache.SegmentMeta{
			Seq:       seq,
			StartedAt: base.Add(time.Duration(seq) * 10 * time.Second),
			EndedAt:   base.Add(time.Duration(seq+1) * 10 * time.Second),
		})
	}
	return metas
}

func inventoryOf(complete bool, segments ...cache.DeclaredSegment) *cache.StreamInventory {
	return &cache.StreamInventory{StreamID: "stream-1", Complete: complete, Segments: segments}
}

// The exact failure that lost a camera recording: every segment uploaded except the last, so the
// received set simply stops early and has no interior gap to find.
func TestReconcile_DetectsMissingTail(t *testing.T) {
	inventory := inventoryOf(true, declared(0, 300), declared(1, 300), declared(2, 300))

	coverage := Reconcile(inventory, received(0, 1))

	if !coverage.HasGaps() {
		t.Fatal("a recording missing its final segment must report gaps")
	}
	if len(coverage.MissingSeqs) != 1 || coverage.MissingSeqs[0] != 2 {
		t.Fatalf("got MissingSeqs=%v, want [2]", coverage.MissingSeqs)
	}
	if coverage.DeclaredSegments != 3 || coverage.ReceivedSegments != 2 {
		t.Fatalf("got declared=%d received=%d, want 3 and 2",
			coverage.DeclaredSegments, coverage.ReceivedSegments)
	}
}

// The blind spot the inventory exists to remove. Without it the same truncated recording looks
// complete, which is why this asserts the weaker behaviour rather than pretending it is fine.
func TestReconcile_WithoutInventoryCannotSeeMissingTail(t *testing.T) {
	coverage := Reconcile(nil, received(0, 1))

	if coverage.HasInventory {
		t.Fatal("HasInventory should be false when the client never declared one")
	}
	if coverage.HasGaps() {
		t.Fatal("timestamp-only detection cannot see a missing tail; this test documents that limit")
	}
}

func TestReconcile_DetectsMissingHeadAndInterior(t *testing.T) {
	inventory := inventoryOf(true, declared(0, 300), declared(1, 300), declared(2, 300), declared(3, 300))

	coverage := Reconcile(inventory, received(1, 3))

	if len(coverage.MissingSeqs) != 2 ||
		coverage.MissingSeqs[0] != 0 || coverage.MissingSeqs[1] != 2 {
		t.Fatalf("got MissingSeqs=%v, want [0 2]", coverage.MissingSeqs)
	}
}

func TestReconcile_CompleteRecordingHasNoGaps(t *testing.T) {
	inventory := inventoryOf(true, declared(0, 300), declared(1, 300))

	coverage := Reconcile(inventory, received(0, 1))

	if coverage.HasGaps() {
		t.Fatalf("a complete recording must not report gaps, got %v", coverage.MissingSeqs)
	}
	if len(coverage.Anomalies) != 0 {
		t.Fatalf("a healthy recording must not report anomalies, got %+v", coverage.Anomalies)
	}
}

// A segment can arrive intact, correctly numbered and correctly sized, and still contain no picture.
// Nothing about sequence numbers can reveal that.
func TestReconcile_FlagsSegmentWithNoFrames(t *testing.T) {
	inventory := inventoryOf(true, declared(0, 300), declared(1, 0), declared(2, 300))

	coverage := Reconcile(inventory, received(0, 1, 2))

	if coverage.HasGaps() {
		t.Fatal("nothing is missing here; the segment arrived, it is just empty of picture")
	}
	if len(coverage.Anomalies) != 1 ||
		coverage.Anomalies[0].Kind != AnomalyNotCaptured ||
		coverage.Anomalies[0].Seq != 1 {
		t.Fatalf("got %+v, want a single not_captured anomaly on seq 1", coverage.Anomalies)
	}
}

func TestReconcile_FlagsFrameRateCollapseAgainstStreamsOwnMedian(t *testing.T) {
	inventory := inventoryOf(true,
		declared(0, 300), declared(1, 300), declared(2, 20), declared(3, 300), declared(4, 300))

	coverage := Reconcile(inventory, received(0, 1, 2, 3, 4))

	if len(coverage.Anomalies) != 1 ||
		coverage.Anomalies[0].Kind != AnomalyLowFrameRate ||
		coverage.Anomalies[0].Seq != 2 {
		t.Fatalf("got %+v, want a single low_frame_rate anomaly on seq 2", coverage.Anomalies)
	}
}

// A machine that is uniformly slow is doing its best, not malfunctioning. Calibrating against the
// stream's own median rather than a fixed frame rate is what keeps that from being reported as
// hundreds of anomalies.
func TestReconcile_UniformlySlowStreamIsNotFlagged(t *testing.T) {
	inventory := inventoryOf(true, declared(0, 50), declared(1, 50), declared(2, 50), declared(3, 50))

	coverage := Reconcile(inventory, received(0, 1, 2, 3))

	if len(coverage.Anomalies) != 0 {
		t.Fatalf("a consistently low but stable frame rate is not an anomaly, got %+v", coverage.Anomalies)
	}
}

func TestReconcile_FlagsEmptySegment(t *testing.T) {
	empty := declared(1, 0)
	empty.SizeBytes = 0
	inventory := inventoryOf(true, declared(0, 300), empty)

	coverage := Reconcile(inventory, received(0, 1))

	if len(coverage.Anomalies) != 1 || coverage.Anomalies[0].Kind != AnomalyEmpty {
		t.Fatalf("got %+v, want a single empty anomaly", coverage.Anomalies)
	}
}

// The duration reported for a truncated recording should still be what the exam actually ran, not
// only the part that survived the upload.
func TestRecordedDuration_UsesDeclaredSpanWhenSegmentsAreMissing(t *testing.T) {
	inventory := inventoryOf(true, declared(0, 300), declared(1, 300), declared(2, 300))

	got := RecordedDuration(inventory, received(0, 1))

	if got != 30*time.Second {
		t.Fatalf("got %v, want 30s (all three declared segments)", got)
	}
}
