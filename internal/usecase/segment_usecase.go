package usecase

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"go.uber.org/zap"
)

var (
	ErrUploadSessionNotFound  = cache.ErrUploadSessionNotFound
	ErrUploadSessionExpired   = cache.ErrUploadSessionExpired
	ErrUploadSessionCompleted = errors.New("upload session already completed")
	ErrUploadSessionOwnership = errors.New("upload session ownership mismatch")
	ErrSegmentConflict        = errors.New("segment sequence already contains different content")
)

type SegmentUploadRequest struct {
	StreamID    string
	UploadToken string
	Seq         int64
	StartedAt   time.Time
	EndedAt     time.Time
	SHA256      string
	Data        []byte
}

type SegmentGap struct {
	FromSeq int64
	ToSeq   int64
	Missing time.Duration
}

type StreamAudit struct {
	StreamID         string
	TotalSegments    int
	RecordedDuration time.Duration
	Gaps             []SegmentGap
	HasGaps          bool
}

type SegmentUseCase struct {
	storage  *storage.Client
	segments *cache.SegmentRegistry
	sessions *cache.SessionRegistry
	logger   *zap.Logger
}

func NewSegmentUseCase(
	storage *storage.Client,
	segments *cache.SegmentRegistry,
	sessions *cache.SessionRegistry,
	logger *zap.Logger,
) *SegmentUseCase {
	return &SegmentUseCase{
		storage:  storage,
		segments: segments,
		sessions: sessions,
		logger:   logger,
	}
}

func (u *SegmentUseCase) Upload(ctx context.Context, req SegmentUploadRequest) error {
	session, err := u.sessions.LookupUpload(ctx, req.StreamID)
	if err != nil {
		return err
	}
	if err := validateUploadOwnership(session, req); err != nil {
		return err
	}
	if session.Completed {
		return ErrUploadSessionCompleted
	}

	existing, err := u.segments.Get(ctx, req.StreamID, req.Seq)
	if err != nil {
		return fmt.Errorf("lookup existing segment: %w", err)
	}
	if existing != nil {
		if existing.SHA256 == req.SHA256 {
			return nil
		}
		return ErrSegmentConflict
	}

	key, err := u.storage.UploadSegment(
		ctx,
		session.ScheduleID,
		session.SessionID,
		session.StreamID,
		req.Seq,
		req.Data,
	)
	if err != nil {
		return fmt.Errorf("upload segment: %w", err)
	}
	return u.segments.Add(ctx, req.StreamID, cache.SegmentMeta{
		Seq:        req.Seq,
		S3Key:      key,
		SHA256:     req.SHA256,
		StartedAt:  req.StartedAt,
		EndedAt:    req.EndedAt,
		SizeBytes:  int64(len(req.Data)),
		UploadedAt: time.Now().UTC(),
	})
}

func (u *SegmentUseCase) Audit(ctx context.Context, streamID string) (*StreamAudit, error) {
	metas, err := u.segments.List(ctx, streamID)
	if err != nil {
		return nil, err
	}
	audit := &StreamAudit{
		StreamID:      streamID,
		TotalSegments: len(metas),
	}
	if len(metas) == 0 {
		return audit, nil
	}

	audit.Gaps, audit.RecordedDuration = auditGaps(metas)
	audit.HasGaps = len(audit.Gaps) > 0
	return audit, nil
}

// auditGaps computes gaps (>2s between consecutive segments) and total
// recorded duration (sum of each segment's own span). metas must already be
// sorted by Seq — cache.SegmentRegistry.List guarantees this. Shared between
// Audit and AssemblerUseCase.Assemble so both use the same gap definition.
func auditGaps(metas []cache.SegmentMeta) ([]SegmentGap, time.Duration) {
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

// MarkComplete records that the client has finished uploading all segments
// for this stream, letting AssemblerUseCase.OnStreamEnded take the fast path
// instead of waiting out the grace period.
func (u *SegmentUseCase) MarkComplete(ctx context.Context, req SegmentUploadRequest) (*cache.UploadSession, bool, error) {
	session, err := u.sessions.LookupUpload(ctx, req.StreamID)
	if err != nil {
		return nil, false, err
	}
	if err := validateUploadOwnership(session, req); err != nil {
		return nil, false, err
	}

	newlyCompleted, err := u.sessions.MarkUploadComplete(ctx, req.StreamID)
	if err != nil {
		return nil, false, err
	}
	if err := u.segments.MarkComplete(ctx, req.StreamID); err != nil {
		return nil, false, fmt.Errorf("mark segment stream complete: %w", err)
	}

	return session, newlyCompleted, nil
}

func validateUploadOwnership(session *cache.UploadSession, req SegmentUploadRequest) error {
	actual := sha256.Sum256([]byte(req.UploadToken))
	expected, err := hex.DecodeString(session.UploadTokenHash)
	if err != nil || session.StreamID != req.StreamID || len(expected) != len(actual) ||
		subtle.ConstantTimeCompare(expected, actual[:]) != 1 {
		return ErrUploadSessionOwnership
	}
	return nil
}
