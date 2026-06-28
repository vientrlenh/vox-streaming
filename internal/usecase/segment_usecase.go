package usecase

import (
	"context"
	"fmt"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"go.uber.org/zap"
)

type SegmentUploadRequest struct {
	StreamID      string
	ParticipantID string
	RoomID        string
	StreamType    string
	Seq           int64
	StartedAt     time.Time
	EndedAt time.Time
	Data []byte
}

type SegmentGap struct {
	FromSeq int64
	ToSeq int64
	Missing time.Duration
}

type StreamAudit struct {
	StreamID string
	TotalSegments int
	RecordedDuration time.Duration
	Gaps []SegmentGap 
	HasGaps bool
}

type SegmentUseCase struct {
	storage *storage.Client
	segments *cache.SegmentRegistry
	sessions *cache.SessionRegistry
	logger *zap.Logger
}

func NewSegmentUseCase(
	storage *storage.Client, 
	segments *cache.SegmentRegistry,
	sessions *cache.SessionRegistry, 
	logger *zap.Logger,
) *SegmentUseCase {
	return &SegmentUseCase{
		storage: storage, 
		segments: segments, 
		sessions: sessions, 
		logger: logger,
	}
}

func (u *SegmentUseCase) Upload(ctx context.Context, req SegmentUploadRequest) error {
	// stream_id must belong to active session of participant
	session, err := u.sessions.Lookup(ctx, req.RoomID, req.ParticipantID, req.StreamType)
	if err != nil || session == nil {
		return fmt.Errorf("no active session for participant %s in room %s", req.ParticipantID, req.RoomID)
	}
	if session.StreamID != req.StreamID {
		return fmt.Errorf("streamId mismatch: expected %s, got %s", session.StreamID, req.StreamID)
	}

	key, err := u.storage.UploadSegment(ctx, req.RoomID, req.StreamID, req.Seq, req.Data)
	if err != nil {
		return fmt.Errorf("upload segment: %w", err)
	}
	return u.segments.Add(ctx, req.StreamID, cache.SegmentMeta{
		Seq: req.Seq, 
		S3Key: key, 
		StartedAt: req.StartedAt,
		EndedAt: req.EndedAt,
		SizeBytes: int64(len(req.Data)),
		UploadedAt: time.Now().UTC(),
	})
}

func (u *SegmentUseCase) Audit(ctx context.Context, streamID string) (*StreamAudit, error) {
	metas, err := u.segments.List(ctx, streamID)
	if err != nil {
		return nil, err
	}
	audit := &StreamAudit{
		StreamID: streamID, 
		TotalSegments: len(metas),
	}
	if len(metas) == 0 {
		return audit, nil
	}

	var totalRecorded time.Duration
	for i, m := range metas {
		dur := m.EndedAt.Sub(m.StartedAt)
		totalRecorded += dur

		if i == 0 {
			continue
		}
		prev := metas[i-1]
		gap := m.StartedAt.Sub(prev.EndedAt)
		if gap > 2*time.Second {
			audit.Gaps = append(audit.Gaps, SegmentGap{
				FromSeq: prev.Seq, 
				ToSeq: m.Seq,
				Missing: gap,
			})
		}
	}
	audit.RecordedDuration = totalRecorded
	audit.HasGaps = len(audit.Gaps) > 0
	return audit, nil
}