package usecase

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/stream"
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
	// StopReason is only read by MarkComplete. Empty is valid and expected.
	StopReason string
}

// InventoryDeclaration is the client telling the server what it has captured, uploaded or not.
type InventoryDeclaration struct {
	StreamID    string
	UploadToken string
	Complete    bool
	DeclaredAt  time.Time
	Segments    []cache.DeclaredSegment
}

type SegmentUseCase struct {
	storage   *storage.Client
	segments  *cache.SegmentRegistry
	sessions  *cache.SessionRegistry
	inventory *cache.InventoryRegistry
	logger    *zap.Logger
}

func NewSegmentUseCase(
	storage *storage.Client,
	segments *cache.SegmentRegistry,
	sessions *cache.SessionRegistry,
	inventory *cache.InventoryRegistry,
	logger *zap.Logger,
) *SegmentUseCase {
	return &SegmentUseCase{
		storage:   storage,
		segments:  segments,
		sessions:  sessions,
		inventory: inventory,
		logger:    logger,
	}
}

// DeclareInventory records what the client says this stream contains. Gated on the same upload-token
// ownership as Upload: an inventory is what gap detection is measured against, so letting anyone who
// knows a streamId rewrite it would let them declare a truncated recording complete.
func (u *SegmentUseCase) DeclareInventory(ctx context.Context, req InventoryDeclaration) error {
	session, err := u.sessions.LookupUpload(ctx, req.StreamID)
	if err != nil {
		return err
	}
	if err := validateUploadOwnership(session, SegmentUploadRequest{
		StreamID:    req.StreamID,
		UploadToken: req.UploadToken,
	}); err != nil {
		return err
	}

	declaredAt := req.DeclaredAt
	if declaredAt.IsZero() {
		declaredAt = time.Now().UTC()
	}

	return u.inventory.Put(ctx, cache.StreamInventory{
		StreamID:   req.StreamID,
		Complete:   req.Complete,
		DeclaredAt: declaredAt,
		Segments:   req.Segments,
	})
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

// Audit requires the same upload-token ownership as Upload/MarkComplete: streamIDs are UUIDs
// visible in the segment upload URL, so without this check anyone who observed a candidate's
// streamId could read another candidate's segment coverage.
func (u *SegmentUseCase) Audit(ctx context.Context, req SegmentUploadRequest) (*stream.StreamAudit, error) {
	session, err := u.sessions.LookupUpload(ctx, req.StreamID)
	if err != nil {
		return nil, err
	}
	if err := validateUploadOwnership(session, req); err != nil {
		return nil, err
	}

	metas, err := u.segments.List(ctx, req.StreamID)
	if err != nil {
		return nil, err
	}
	inventory, err := u.inventory.Get(ctx, req.StreamID)
	if err != nil {
		return nil, err
	}

	audit := &stream.StreamAudit{
		StreamID:      req.StreamID,
		TotalSegments: len(metas),
		Coverage:      stream.Reconcile(inventory, metas),
	}
	audit.RecordedDuration = stream.RecordedDuration(inventory, metas)
	if len(metas) > 0 {
		audit.Gaps, _ = stream.AuditGaps(metas)
	}
	// Taken from the reconciliation rather than from auditGaps: the timestamp heuristic cannot see
	// a missing first or last segment, which is exactly the shape of gap a client that died mid-run
	// leaves behind.
	audit.HasGaps = audit.Coverage.HasGaps()
	return audit, nil
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

	newlyCompleted, err := u.sessions.MarkUploadComplete(ctx, req.StreamID, req.StopReason)
	if err != nil {
		return nil, false, err
	}
	if err := u.segments.MarkComplete(ctx, req.StreamID); err != nil {
		return nil, false, fmt.Errorf("mark segment stream complete: %w", err)
	}

	// session was read before the write above, so reflect the reason the caller just recorded
	// rather than handing back a copy that says the stream ended for no stated reason.
	session.Completed = true
	if req.StopReason != "" {
		session.StopReason = req.StopReason
	}
	return session, newlyCompleted, nil
}

// KnownStopReasons are the values the desktop client's RecordingStopReason can take. Anything else
// is dropped rather than stored: this string ends up in operational logs and, later, in the audit
// trail behind a grading decision, and an endpoint that echoes arbitrary client text into both is
// a log-injection hole for the sake of a diagnostic.
var KnownStopReasons = map[string]struct{}{
	"Submitted":           {},
	"Expired":             {},
	"UserClosed":          {},
	"ApplicationShutdown": {},
	"CaptureFailure":      {},
	// Not a client RecordingStopReason: OrphanedUploadRecoveryService finishes a stream whose
	// original run died without ever reaching /complete, so no stop reason was observed at the
	// time. Recording that explicitly beats recording nothing, which is indistinguishable from an
	// old client.
	"RecoveredAfterCrash": {},
}

// NormalizeStopReason keeps only reasons this server recognises.
func NormalizeStopReason(reason string) string {
	reason = strings.TrimSpace(reason)
	if _, ok := KnownStopReasons[reason]; ok {
		return reason
	}
	return ""
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
