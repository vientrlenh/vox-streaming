package watcher

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"go.uber.org/zap"
)

const (
	defaultWatchdogInterval = time.Minute
	watchdogBatchLimit      = 32

	// A due assembly that errors out is retried on a slow cadence rather than every sweep: the
	// plausible failures here are S3, disk and ffmpeg, none of which recover in seconds.
	watchdogRetryInterval = 2 * time.Minute
	watchdogMaxAttempts   = 5
)

// AssemblyWatchdog assembles recordings whose client never called /complete.
//
// The desktop upload path used to depend entirely on the client asking for assembly, which makes
// every recording hostage to the exam machine surviving to the end of its own upload: a crash, a
// shutdown, a force-close or a network that never comes back all leave the segments already in S3
// as loose parts that nothing will ever turn into a recording.mp4. The WebRTC path never had this
// problem because stream.ended already carries a grace-period fallback (see
// AssemblerUseCase.OnStreamEnded); this is the equivalent for uploads.
//
// Timing is deliberately the upload session's own expiry, not an inactivity timeout. Assembly is
// effectively one-shot -- AssemblerUseCase.assemble short-circuits on an existing recording.mp4 --
// so assembling early does not merely produce a short recording, it permanently prevents the
// complete one from ever being built. Expiry is the first moment the stream provably cannot grow
// any further: the upload credential is dead, so no segment can arrive after it.
type AssemblyWatchdog struct {
	pending   *cache.PendingAssemblyRegistry
	segments  *cache.SegmentRegistry
	assembler recordingAssembler
	interval  time.Duration
	logger    *zap.Logger
}

// The one thing the watchdog needs from AssemblerUseCase. Narrowed to an interface so the
// watchdog's own decisions -- skip an empty stream, retry a failure, give up after N -- can be
// tested without standing up storage, Kafka and ffmpeg behind them.
type recordingAssembler interface {
	AssembleRequested(ctx context.Context, event domain.RecordingAssemblyRequestedEvent) error
}

func NewAssemblyWatchdog(
	pending *cache.PendingAssemblyRegistry,
	segments *cache.SegmentRegistry,
	assembler recordingAssembler,
	interval time.Duration,
	logger *zap.Logger,
) *AssemblyWatchdog {
	if interval <= 0 {
		interval = defaultWatchdogInterval
	}
	return &AssemblyWatchdog{
		pending:   pending,
		segments:  segments,
		assembler: assembler,
		interval:  interval,
		logger:    logger,
	}
}

func (w *AssemblyWatchdog) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	w.logger.Info("assembly watchdog started", zap.Duration("interval", w.interval))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.SweepOnce(ctx)
		}
	}
}

// SweepOnce is exported so it can be driven directly by tests and by any operational trigger,
// without waiting out a tick.
func (w *AssemblyWatchdog) SweepOnce(ctx context.Context) {
	due, err := w.pending.Due(ctx, time.Now().UTC(), watchdogBatchLimit)
	if err != nil {
		w.logger.Error("assembly watchdog sweep failed", zap.Error(err))
		return
	}

	for _, pending := range due {
		w.handle(ctx, pending)
	}
}

func (w *AssemblyWatchdog) handle(ctx context.Context, pending cache.PendingAssembly) {
	log := w.logger.With(
		zap.String("streamId", pending.StreamID),
		zap.String("scheduleId", pending.ScheduleID),
		zap.String("streamType", pending.StreamType),
		zap.Int("attempt", pending.Attempts+1),
	)

	// A stream that produced nothing (a camera that never opened, an attempt abandoned before the
	// first segment) has nothing to assemble. Dropping it quietly is right: running the assembler
	// would only fail and publish a FAILED recording state for a recording that was never started.
	metas, err := w.segments.List(ctx, pending.StreamID)
	if err != nil {
		log.Error("assembly watchdog could not list segments", zap.Error(err))
		return
	}
	if len(metas) == 0 {
		log.Info("assembly watchdog dropped a stream with no uploaded segments")
		w.cancel(ctx, pending.StreamID, log)
		return
	}

	log.Warn("assembling a stream the client never completed",
		zap.Int("segmentCount", len(metas)),
	)

	eventID, err := uuid.NewV7()
	if err != nil {
		log.Error("assembly watchdog could not create event id", zap.Error(err))
		return
	}

	err = w.assembler.AssembleRequested(ctx, domain.RecordingAssemblyRequestedEvent{
		EventID:       eventID.String(),
		StreamID:      pending.StreamID,
		ScheduleID:    pending.ScheduleID,
		SessionID:     pending.SessionID,
		ParticipantID: pending.ParticipantID,
		StreamType:    pending.StreamType,
		// Distinct from DESKTOP_SEGMENT_UPLOAD on purpose: downstream needs to be able to tell a
		// recording the client vouched for from one salvaged after the client went silent.
		Source:      "SERVER_WATCHDOG",
		RequestedAt: time.Now().UTC(),
	})
	if err == nil {
		log.Info("assembly watchdog completed a stream the client abandoned")
		w.cancel(ctx, pending.StreamID, log)
		return
	}

	if pending.Attempts+1 >= watchdogMaxAttempts {
		log.Error("assembly watchdog giving up after repeated failures; segments remain in storage unassembled",
			zap.Error(err),
		)
		w.cancel(ctx, pending.StreamID, log)
		return
	}

	log.Warn("assembly watchdog attempt failed, will retry", zap.Error(err))
	pending.Attempts++
	pending.DueAt = time.Now().UTC().Add(watchdogRetryInterval)
	if err := w.pending.Schedule(ctx, pending); err != nil {
		log.Error("assembly watchdog could not reschedule", zap.Error(err))
	}
}

func (w *AssemblyWatchdog) cancel(ctx context.Context, streamID string, log *zap.Logger) {
	if err := w.pending.Cancel(ctx, streamID); err != nil {
		// Left in place it would simply be retried on the next sweep, and assemble is idempotent,
		// so this is noisy rather than harmful.
		log.Warn("assembly watchdog could not clear its pending entry", zap.Error(err))
	}
}
