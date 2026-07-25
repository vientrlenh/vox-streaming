package usecase

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
)

func seg(seq int64, startedAt time.Time, dur time.Duration) cache.SegmentMeta {
	return cache.SegmentMeta{
		Seq:       seq,
		StartedAt: startedAt,
		EndedAt:   startedAt.Add(dur),
	}
}

func TestAuditGaps(t *testing.T) {
	base := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	t.Run("empty input has no gaps or duration", func(t *testing.T) {
		gaps, dur := auditGaps(nil)
		if len(gaps) != 0 || dur != 0 {
			t.Fatalf("got gaps=%v dur=%v, want none", gaps, dur)
		}
	})

	t.Run("single segment has no gaps", func(t *testing.T) {
		metas := []cache.SegmentMeta{seg(0, base, 5*time.Second)}
		gaps, dur := auditGaps(metas)
		if len(gaps) != 0 {
			t.Errorf("got gaps=%v, want none", gaps)
		}
		if dur != 5*time.Second {
			t.Errorf("got duration=%v, want 5s", dur)
		}
	})

	t.Run("contiguous segments produce no gaps", func(t *testing.T) {
		metas := []cache.SegmentMeta{
			seg(0, base, 5*time.Second),
			seg(1, base.Add(5*time.Second), 5*time.Second),
			seg(2, base.Add(10*time.Second), 5*time.Second),
		}
		gaps, dur := auditGaps(metas)
		if len(gaps) != 0 {
			t.Errorf("got gaps=%v, want none", gaps)
		}
		if dur != 15*time.Second {
			t.Errorf("got duration=%v, want 15s", dur)
		}
	})

	t.Run("gap of exactly 2s is not flagged", func(t *testing.T) {
		metas := []cache.SegmentMeta{
			seg(0, base, 5*time.Second),
			seg(1, base.Add(5*time.Second+2*time.Second), 5*time.Second),
		}
		gaps, _ := auditGaps(metas)
		if len(gaps) != 0 {
			t.Errorf("got gaps=%v, want none (boundary is exclusive)", gaps)
		}
	})

	t.Run("gap over 2s is flagged with from/to seq", func(t *testing.T) {
		metas := []cache.SegmentMeta{
			seg(0, base, 5*time.Second),
			seg(1, base.Add(5*time.Second+3*time.Second), 5*time.Second),
		}
		gaps, dur := auditGaps(metas)
		if len(gaps) != 1 {
			t.Fatalf("got %d gaps, want 1", len(gaps))
		}
		if gaps[0].FromSeq != 0 || gaps[0].ToSeq != 1 || gaps[0].Missing != 3*time.Second {
			t.Errorf("got %+v, want FromSeq=0 ToSeq=1 Missing=3s", gaps[0])
		}
		if dur != 10*time.Second {
			t.Errorf("got recorded duration=%v, want sum of segment spans (10s), not wall time", dur)
		}
	})

	t.Run("multiple gaps are all reported", func(t *testing.T) {
		metas := []cache.SegmentMeta{
			seg(0, base, time.Second),
			seg(1, base.Add(10*time.Second), time.Second),
			seg(2, base.Add(11*time.Second), time.Second),
			seg(3, base.Add(30*time.Second), time.Second),
		}
		gaps, _ := auditGaps(metas)
		if len(gaps) != 2 {
			t.Fatalf("got %d gaps, want 2: %+v", len(gaps), gaps)
		}
		if gaps[0].FromSeq != 0 || gaps[0].ToSeq != 1 {
			t.Errorf("gap 0: got %+v, want FromSeq=0 ToSeq=1", gaps[0])
		}
		if gaps[1].FromSeq != 2 || gaps[1].ToSeq != 3 {
			t.Errorf("gap 1: got %+v, want FromSeq=2 ToSeq=3", gaps[1])
		}
	})
}

func uploadToken(t *testing.T, token string) *cache.UploadSession {
	t.Helper()
	hash := sha256.Sum256([]byte(token))
	return &cache.UploadSession{
		StreamID:        "stream-1",
		UploadTokenHash: hex.EncodeToString(hash[:]),
	}
}

func TestValidateUploadOwnership(t *testing.T) {
	t.Run("matching token and stream id succeeds", func(t *testing.T) {
		session := uploadToken(t, "correct-token")
		req := SegmentUploadRequest{StreamID: "stream-1", UploadToken: "correct-token"}
		if err := validateUploadOwnership(session, req); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("wrong token rejected", func(t *testing.T) {
		session := uploadToken(t, "correct-token")
		req := SegmentUploadRequest{StreamID: "stream-1", UploadToken: "wrong-token"}
		if err := validateUploadOwnership(session, req); err != ErrUploadSessionOwnership {
			t.Fatalf("got %v, want ErrUploadSessionOwnership", err)
		}
	})

	t.Run("mismatched stream id rejected", func(t *testing.T) {
		session := uploadToken(t, "correct-token")
		req := SegmentUploadRequest{StreamID: "stream-2", UploadToken: "correct-token"}
		if err := validateUploadOwnership(session, req); err != ErrUploadSessionOwnership {
			t.Fatalf("got %v, want ErrUploadSessionOwnership", err)
		}
	})

	t.Run("malformed stored hash rejected", func(t *testing.T) {
		session := &cache.UploadSession{StreamID: "stream-1", UploadTokenHash: "not-hex!!"}
		req := SegmentUploadRequest{StreamID: "stream-1", UploadToken: "correct-token"}
		if err := validateUploadOwnership(session, req); err != ErrUploadSessionOwnership {
			t.Fatalf("got %v, want ErrUploadSessionOwnership", err)
		}
	})
}
