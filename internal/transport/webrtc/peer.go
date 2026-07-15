package webrtc

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/recorder"
	"github.com/vientrlenh/vox-streaming/internal/usecase"
	"go.uber.org/zap"
)

const (
	StreamTypeCamera = "camera"
	StreamTypeScreen = "screen"

	defaultFrameInterval = 5 * time.Second
)

type PeerConfig struct {
	API           *webrtc.API
	ICEServers    []webrtc.ICEServer
	FrameInterval time.Duration
	TempDir       string
	AIRelay       AIRelayOptions
	FFmpegIngest  FFmpegIngestOptions
}


type FFmpegIngestOptions struct {
	Allocator          *recorder.PortAllocator
	RecordSem          chan struct{} // capacity = max concurrent ffmpeg recorders across all peers
	SegmentSeconds     int
	ReorderQueueSize   int // ffmpeg -reorder_queue_size; <= 0 uses ffmpegingest's default (256)
	MaxDelayMicros     int // ffmpeg -max_delay (microseconds); <= 0 uses ffmpegingest's default (500000)
	MaxRestartAttempts int
	StopTimeout        time.Duration // grace period for ffmpeg to quit gracefully at stream end before being force-killed
}

// coordinates waiting for both the video and audio track
// (which arrive on separate OnTrack callbacks, in no guaranteed order)
// before starting the ffmpeg ingest bridge session and recorder.
type ffmpegIngestState struct {
	mu            sync.Mutex
	videoTrack    *webrtc.TrackRemote
	videoReceiver *webrtc.RTPReceiver
	audioTrack    *webrtc.TrackRemote
	audioReceiver *webrtc.RTPReceiver
	session       *recorder.Session
	sessionStarting bool // claimed under mu while StartSession/StartRecorderSupervisor run, before session is set — closes the race where video's and audio's OnTrack goroutines both see session == nil
	supervisor    *recorder.RecorderSupervisor
	recordSlotHeld bool // true once a RecordSem slot was acquired for this stream

	segMu      sync.Mutex
	segs       []cache.SegmentMeta
	uploadDone chan struct{}

	// incomplete is set true the moment we know the final recording can't
	// possibly cover the whole stream — a segment upload failed permanently,
	// a segment failed to register in SegmentRegistry, or close() gave up
	// waiting for the uploader to drain. Recording must not be marked
	// complete once this is set, even though the ffmpeg process itself may
	// have run and exited perfectly cleanly.
	incomplete atomic.Bool
}


func (st *ffmpegIngestState) forwardVideo(raw []byte) {
	st.mu.Lock()
	session := st.session
	st.mu.Unlock()
	if session != nil {
		session.ForwardVideoRTP(raw)
	}
}

func (st *ffmpegIngestState) forwardAudio(raw []byte) {
	st.mu.Lock()
	session := st.session
	st.mu.Unlock()
	if session != nil {
		session.ForwardAudioRTP(raw)
	}
}

type Peer struct {
	pc            *webrtc.PeerConnection
	roomID        string
	participantID string
	streamID      string
	streamType    string
	startedAt     time.Time
	frameSeq      atomic.Int64
	frameInterval time.Duration

	pendingCandidates []*webrtc.ICECandidateInit
	candidateMu       sync.Mutex

	disconnectTimer *time.Timer
	disconnectMu    sync.Mutex

	streamUseCase *usecase.StreamUseCase
	monitorUseCase *usecase.MonitorUseCase
	storage *storage.Client
	segments *cache.SegmentRegistry

	aiRelay AIRelayOptions
	ffmpegIngestCfg FFmpegIngestOptions
	ffmpegIngest    *ffmpegIngestState // nil when FFmpegIngest is disabled
	tempDir         string
	logger  *zap.Logger

	closedByFailure atomic.Bool

	done      chan struct{}
	once      sync.Once
	startOnce sync.Once
}

func NewPeer(
	cfg PeerConfig,
	roomID, participantID, streamType string,
	streamUseCase *usecase.StreamUseCase, 
	monitorUseCase *usecase.MonitorUseCase,
	storage *storage.Client, 
	segments *cache.SegmentRegistry, 
	logger *zap.Logger,
) (*Peer, error) {

	pc, err := cfg.API.NewPeerConnection(webrtc.Configuration{
		ICEServers: cfg.ICEServers,
	})
	if err != nil {
		return nil, fmt.Errorf("new peer connection: %w", err)
	}

	fi := cfg.FrameInterval
	if fi == 0 {
		fi = defaultFrameInterval
	}

	streamIDUuid, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("stream ID generation: %w", err)
	}

	streamID := streamIDUuid.String()
	p := &Peer{
		pc:            pc,
		roomID:        roomID,
		participantID: participantID,
		streamID:      streamID,
		streamType:    streamType,
		startedAt:     time.Now().UTC(),
		frameInterval: fi,
		streamUseCase: streamUseCase,
		monitorUseCase: monitorUseCase,
		storage: storage,
		segments: segments,
		aiRelay:       cfg.AIRelay,
		ffmpegIngestCfg: cfg.FFmpegIngest,
		tempDir:         cfg.TempDir,
		done:          make(chan struct{}),
		logger: logger.With(
			zap.String("roomId", roomID),
			zap.String("participantId", participantID),
			zap.String("streamId", streamID),
			zap.String("streamType", streamType),
		),
	}
	p.closedByFailure.Store(false)
	if cfg.FFmpegIngest.Allocator != nil {
		p.ffmpegIngest = &ffmpegIngestState{}
	}
	p.setupCallbacks()
	return p, nil
}

func (p *Peer) setupCallbacks() {
	p.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		p.logger.Info("connection state changed", zap.String("state", state.String()))
		switch state {
		case webrtc.PeerConnectionStateConnected:
			p.cancelDisconnectTimer()
			p.startOnce.Do(func() {
				ctx := context.Background()
				if err := p.streamUseCase.NotifyStreamStarted(
					ctx, p.roomID, p.participantID, p.streamID, p.streamType,
				); err != nil {
					p.logger.Error("notify stream started failed", zap.Error(err))
				}
			})
		case webrtc.PeerConnectionStateDisconnected:
			p.logger.Warn("peer disconnected, starting grace period")
			p.scheduleClose(30 * time.Second)

		case webrtc.PeerConnectionStateFailed:
			p.logger.Error("peer connection failed, closing")
			p.closedByFailure.Store(true)
			p.close()

		case webrtc.PeerConnectionStateClosed:
			p.close() // cleanup
		}

	})

	p.pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		p.logger.Info("track received",
			zap.String("kind",
				track.Kind().String()),
			zap.String("codec", track.Codec().MimeType),
		)
		switch track.Kind() {
		case webrtc.RTPCodecTypeVideo:
			p.noteFFmpegIngestTrack(true, track, receiver)
			go p.handleVideoTrack(track)
		case webrtc.RTPCodecTypeAudio:
			p.noteFFmpegIngestTrack(false, track, receiver)
			go p.handleAudioTrack(track)
		}
	})
}


func (p *Peer) noteFFmpegIngestTrack(video bool, track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
	if p.ffmpegIngest == nil {
		return
	}
	st := p.ffmpegIngest
	st.mu.Lock()
	if video {
		st.videoTrack, st.videoReceiver = track, receiver
	} else {
		st.audioTrack, st.audioReceiver = track, receiver
	}
	vt, vr, at, ar := st.videoTrack, st.videoReceiver, st.audioTrack, st.audioReceiver

	ready := vt != nil && at != nil && st.session == nil && !st.sessionStarting
	if ready {
		st.sessionStarting = true
	}
	st.mu.Unlock()
	if !ready {
		return
	}

	// cheap fast-path: don't bother spinning up ffmpeg at all if the peer is
	// already gone (e.g. both tracks arrived right as the client disconnected).
	// Not required for correctness on its own — the commit-time check below is
	// what actually closes the race — but avoids wasted work in the common case.
	select {
	case <-p.done:
		p.abortFFmpegIngestStart(st)
		return
	default:
	}

	session, err := recorder.StartSession(context.Background(), p.ffmpegIngestCfg.Allocator, p.tempDir, vt, vr, at, ar, p.logger)
	if err != nil {
		p.logger.Warn("ffmpeg ingest bridge start failed", zap.Error(err))
		p.abortFFmpegIngestStart(st)
		return
	}

	select {
	case p.ffmpegIngestCfg.RecordSem <- struct{}{}:
	default:
		p.logger.Warn("ffmpeg ingest max concurrent recorders reached, skipping ffmpeg recording for this stream")
		session.Close()
		p.abortFFmpegIngestStart(st)
		return
	}

	requestKeyframe := func() {
		if err := p.pc.WriteRTCP([]rtcp.Packet{
			&rtcp.PictureLossIndication{MediaSSRC: uint32(vt.SSRC())},
		}); err != nil {
			p.logger.Warn("ffmpeg ingest keyframe request failed", zap.Error(err))
		}
	}

	outDir := filepath.Join(p.tempDir, "ffmpeg-ingest", p.streamID)
	sup, err := recorder.StartRecorderSupervisor(session.SDPPath(), outDir, p.ffmpegIngestCfg.SegmentSeconds, p.ffmpegIngestCfg.ReorderQueueSize, p.ffmpegIngestCfg.MaxDelayMicros, p.ffmpegIngestCfg.MaxRestartAttempts, p.ffmpegIngestCfg.StopTimeout, requestKeyframe, p.logger)
	if err != nil {
		p.logger.Warn("ffmpeg recorder start failed", zap.Error(err))
		<-p.ffmpegIngestCfg.RecordSem
		session.Close()
		p.abortFFmpegIngestStart(st)
		return
	}

	time.Sleep(200 * time.Millisecond)

	// commit — but only if the peer hasn't closed while we were setting up.
	// close() reads session/supervisor under this same mutex, and closes
	// p.done unconditionally before ever touching it, so this check is
	// race-free: whichever of close()/this commit acquires st.mu first
	// determines whether close() sees a live session to stop, or this
	// goroutine sees a closed peer and rolls back itself.
	st.mu.Lock()
	select {
	case <-p.done:
		st.sessionStarting = false
		st.mu.Unlock()
		p.logger.Warn("peer closed while ffmpeg ingest was starting, rolling back")
		go func() {
			// runFFmpegSegmentUploader was never started on this path — drain
			// Segments() ourselves before/while calling Stop(), otherwise a
			// segment ffmpeg does manage to flush during shutdown has no
			// consumer, blocking the unbuffered segCh send forever and
			// hanging Stop() (same hazard as the bounded-upload fix, just
			// with zero consumers instead of a slow one).
			drainDone := make(chan struct{})
			go func() {
				defer close(drainDone)
				for path := range sup.Segments() {
					os.Remove(path)
				}
			}()
			sup.Stop()
			<-drainDone
			sup.Cleanup()
			session.Close()
			<-p.ffmpegIngestCfg.RecordSem
		}()
		return
	default:
	}
	st.session = session
	st.supervisor = sup
	st.recordSlotHeld = true
	st.uploadDone = make(chan struct{})
	st.mu.Unlock()
	go p.runFFmpegSegmentUploader(st, sup)

	requestKeyframe()
}

// resets sessionStarting after a failed or aborted
// ffmpeg ingest init, so the claim in noteFFmpegIngestTrack doesn't wedge
// this peer out of recording forever (it had no way back to false before).
func (p *Peer) abortFFmpegIngestStart(st *ffmpegIngestState) {
	st.mu.Lock()
	st.sessionStarting = false
	st.mu.Unlock()
}


func (p *Peer) runFFmpegSegmentUploader(st *ffmpegIngestState, sup *recorder.RecorderSupervisor) {
	defer close(st.uploadDone)
	if p.storage == nil {
		return
	}
	seq := int64(0)
	prevAt := time.Now().UTC()
	for path := range sup.Segments() {
		startedAt := prevAt
		endedAt := time.Now().UTC()
		prevAt = endedAt

		ctx := context.Background()
		s3Key, size, err := p.uploadFFmpegSegmentWithRetry(ctx, path, seq)
		if err != nil {
			p.logger.Error("ffmpeg segment upload failed permanently, recording will be marked incomplete",
				zap.Int64("seq", seq),
				zap.String("path", path),
				zap.Error(err),
			)
			// leave the file on disk (not uploaded, not removed) — it's the
			// only remaining copy of this segment; sup.Cleanup() is skipped
			// for the whole attempt dir once st.incomplete is set, so it's
			// still there for manual recovery afterward.
			st.incomplete.Store(true)
			seq++
			continue
		}
		os.Remove(path)

		st.segMu.Lock()
		st.segs = append(st.segs, cache.SegmentMeta{
			Seq:        seq,
			S3Key:      s3Key,
			StartedAt:  startedAt,
			EndedAt:    endedAt,
			SizeBytes:  size,
			UploadedAt: time.Now().UTC(),
		})
		st.segMu.Unlock()
		seq++
	}
}

const ffmpegSegmentUploadMaxAttempts = 3

// bounds how long close() waits for the uploader to
// finish after the supervisor stops. Sized comfortably above the worst case
// for a single segment (ffmpegSegmentUploadMaxAttempts attempts at
// ffmpegSegmentUploadAttemptTimeout each, plus backoff between them —
// roughly 3*15s + 2s + 4s ≈ 51s) since sup.Stop() already guarantees segCh is
// closed by the time this wait starts, leaving at most one in-flight upload.
const ffmpegUploadDrainTimeout = 60 * time.Second

// bounds a single upload attempt. Without
// this, a stuck connection (dead S3 endpoint, network partition) blocks the
// uploader goroutine forever — which in turn blocks the unbuffered segCh send
// in RecorderSupervisor.forwardSegments, which blocks RecorderSupervisor.Stop
// forever, which blocks Peer.close forever. Bounding every attempt guarantees
// the whole chain eventually unwinds even in the worst case.
const ffmpegSegmentUploadAttemptTimeout = 15 * time.Second

func (p *Peer) uploadFFmpegSegmentWithRetry(ctx context.Context, path string, seq int64) (string, int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return "", 0, fmt.Errorf("stat segment: %w", err)
	}
	size := stat.Size()

	var lastErr error
	for attempt := range ffmpegSegmentUploadMaxAttempts {
		if attempt > 0 {
			delay := time.Duration(attempt) * 2 * time.Second // 2s, 4s
			select {
			case <-ctx.Done():
				return "", 0, ctx.Err()
			case <-time.After(delay):
			}
		}

		key, err := p.uploadFFmpegSegmentOnce(ctx, path, seq)
		if err == nil {
			return key, size, nil
		}
		lastErr = err
		p.logger.Warn("ffmpeg segment upload attempt failed",
			zap.Int64("seq", seq),
			zap.Int("attempt", attempt+1),
			zap.Error(err),
		)
	}
	return "", 0, fmt.Errorf("upload failed after %d attempts: %w", ffmpegSegmentUploadMaxAttempts, lastErr)
}

func (p *Peer) uploadFFmpegSegmentOnce(ctx context.Context, path string, seq int64) (string, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, ffmpegSegmentUploadAttemptTimeout)
	defer cancel()

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open segment: %w", err)
	}
	defer f.Close()
	return p.storage.UploadFFmpegSegment(attemptCtx, p.roomID, p.streamID, seq, f)
}

func (p *Peer) scheduleClose(d time.Duration) {
	p.disconnectMu.Lock()
	defer p.disconnectMu.Unlock()
	if p.disconnectTimer != nil {
		p.disconnectTimer.Stop()
	}
	p.disconnectTimer = time.AfterFunc(d, func() {
		p.logger.Warn("grace period expired, closing peer")
		p.closedByFailure.Store(true)
		p.close()
	})
}

func (p *Peer) cancelDisconnectTimer() {
	p.disconnectMu.Lock()
	defer p.disconnectMu.Unlock()
	if p.disconnectTimer != nil {
		p.disconnectTimer.Stop()
		p.disconnectTimer = nil
	}
}

func (p *Peer) handleVideoTrack(track *webrtc.TrackRemote) {
	if track.Codec().MimeType != webrtc.MimeTypeH264 {
		p.logger.Error("unexpected video codec, recording disabled", 
			zap.String("codec", track.Codec().MimeType), 
			zap.String("expected", webrtc.MimeTypeH264),
		)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	fe := recorder.NewFrameExtractor(track, p.pc, p.logger)
	if p.ffmpegIngest != nil {
		fe.SetRawSink(p.ffmpegIngest.forwardVideo)
	}

	// tier 3: fan-out RTP to AI service for realtime proctoring (if turned on).
	// always return non-fatal relay error, recording + capture frame does not depend on AI
	if p.aiRelay.Enabled && p.aiRelay.URL != "" {
		relay, err := NewAIRelay(ctx, AIRelayConfig{
			BaseURL:     p.aiRelay.URL,
			QueueSize:   p.aiRelay.QueueSize,
			ICEServers:  p.aiRelay.ICEServers,
			OnConnected: func() { fe.RequestKeyFrame() }, // After connected -> get keyframe from browser
			OnPLI:       func() { fe.RequestKeyFrame() }, // Get keyframe -> redirect PLI to browser
		},
			track.Codec().RTPCodecCapability,
			RelayMeta{
				RoomID:        p.roomID,
				ParticipantID: p.participantID,
				StreamID:      p.streamID,
				StreamType:    p.streamType,
			},
			p.logger,
		)
		if err != nil {
			p.logger.Warn("ai relay init failed, tiếp tục không relay", zap.Error(err))
		} else {
			fe.SetRTPSink(func(pkt *rtp.Packet) {
				raw, err := pkt.Marshal() // copy buf
				if err != nil {
					return
				}
				relay.Enqueue(raw)
			})
			go func() {
				<-p.done
				relay.Close()
			}()
		}
	}

	go func() {
		<-p.done
		cancel()
	}()

	// ReadLoop running parallelism
	go func() {
		fe.ReadLoop(ctx)
		cancel() // track error -> stop ticker
	}()

	var capturing atomic.Bool
	ticker := time.NewTicker(p.frameInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !capturing.CompareAndSwap(false, true) {
				p.logger.Debug("frame capture skipped: previous capture still in progress")
				continue
			}
			seq := p.frameSeq.Add(1)
			go func(s int64) {
				defer capturing.Store(false)
				frameURL := p.captureAndUpload(ctx, fe, s)
				if err := p.streamUseCase.PublishFrame(
					ctx, p.roomID, p.participantID, p.streamID, p.streamType, frameURL, s,
				); err != nil {
					p.logger.Warn("publish frame failed",
						zap.Int64("seq", s),
						zap.Error(err),
					)
				}
			}(seq)
		}
	}
}

func (p *Peer) captureAndUpload(ctx context.Context, fe *recorder.FrameExtractor, seq int64) string {
	if p.storage == nil {
		return ""
	}

	fe.RequestKeyFrame()
	select {
	case <-fe.IDRReady():
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return ""
	}


	h264Frame := fe.CaptureKeyFrame()
	if h264Frame == nil {
		return ""
	}

	key, err := p.storage.UploadFrame(ctx, p.roomID, p.streamID, seq, h264Frame)
	if err != nil {
		p.logger.Warn("frame upload failed", 
			zap.Int64("seq", seq),
			zap.Error(err),
		)
		return ""
	}

	url, err := p.storage.PresignFrame(ctx, key, p.storage.PresignExpiry())
	if err != nil {
		p.logger.Warn("frame presign failed",
			zap.Int64("seq", seq), 
			zap.Error(err),
		)
		return key // let AI Service resolve raw key
	}
	return url
}

func (p *Peer) handleAudioTrack(track *webrtc.TrackRemote) {
	buf := make([]byte, 4096)
	for {
		n, _, err := track.Read(buf)
		if err != nil {
			return
		}
		if p.ffmpegIngest != nil {
			p.ffmpegIngest.forwardAudio(buf[:n])
		}
	}
}

func (p *Peer) HandleOffer(sdp string) (string, error) {
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer, SDP: sdp,
	}); err != nil {
		return "", fmt.Errorf("set remote description: %w", err)
	}

	p.candidateMu.Lock()
	pending := p.pendingCandidates
	p.pendingCandidates = nil
	p.candidateMu.Unlock()

	for _, c := range pending {
		if err := p.pc.AddICECandidate(*c); err != nil {
			p.logger.Warn("add pending candidate failed", zap.Error(err))
		}
	}

	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return "", fmt.Errorf("create answer: %w", err)
	}

	if err := p.pc.SetLocalDescription(answer); err != nil {
		return "", fmt.Errorf("set local description: %w", err)
	}

	return answer.SDP, nil
}

func (p *Peer) AddICECandidate(c webrtc.ICECandidateInit) {
	p.candidateMu.Lock()
	if p.pc.RemoteDescription() == nil {
		p.pendingCandidates = append(p.pendingCandidates, &c)
		p.candidateMu.Unlock()
		return
	}
	p.candidateMu.Unlock()

	if err := p.pc.AddICECandidate(c); err != nil {
		p.logger.Warn("add ICE candidate failed", zap.Error(err))
	}
}

func (p *Peer) Close() {
	p.close()
}

func (p *Peer) close() {
	p.once.Do(func() {
		close(p.done)
		duration := int64(time.Since(p.startedAt).Seconds())
		ctx := context.Background()

		var ffmpegSegments []cache.SegmentMeta
		ffmpegGaveUp := false
		hadVideoTrack := false
		if p.ffmpegIngest != nil {
			p.ffmpegIngest.mu.Lock()
			session := p.ffmpegIngest.session
			sup := p.ffmpegIngest.supervisor
			slotHeld := p.ffmpegIngest.recordSlotHeld
			uploadDone := p.ffmpegIngest.uploadDone
			hadVideoTrack = p.ffmpegIngest.videoTrack != nil
			p.ffmpegIngest.mu.Unlock()
			// stop the recorder first so ffmpeg can flush its current segment
			// before its RTP source (session) gets cut off.
			if sup != nil {
				// StopProcess is bounded by stopTimeout alone (Recorder.shutdown's
				// own timer + force-kill) — the ffmpeg OS process is confirmed
				// dead by the time this returns, independent of whether the
				// supervisor's internal segment-drain bookkeeping (Stop/doneCh)
				// has finished. Gate the semaphore release on this alone, not on
				// the full teardown, so a slow/stuck upload can't also hold a
				// capacity slot hostage.
				sup.StopProcess()
				if sup.LastAttemptForceKilled() {
					p.logger.Warn("ffmpeg had to be force-killed at shutdown, marking recording incomplete (last segment may be truncated)")
					p.ffmpegIngest.incomplete.Store(true)
				}
				if slotHeld {
					<-p.ffmpegIngestCfg.RecordSem
					slotHeld = false
				}

				if uploadDone != nil {
					select {
					case <-uploadDone:
					case <-time.After(ffmpegUploadDrainTimeout):
						p.logger.Warn("ffmpeg segment upload did not finish in time, marking recording incomplete",
							zap.Duration("waited", ffmpegUploadDrainTimeout),
						)
						p.ffmpegIngest.incomplete.Store(true)
					}
				}
				p.ffmpegIngest.segMu.Lock()
				ffmpegSegments = append([]cache.SegmentMeta(nil), p.ffmpegIngest.segs...)
				p.ffmpegIngest.segMu.Unlock()
				ffmpegGaveUp = sup.GaveUp()

				go func() {
					sup.Stop() // idempotent — StopProcess already ran; this just waits for doneCh (segCh drain) now
					if uploadDone != nil {
						<-uploadDone
					}
					if p.ffmpegIngest.incomplete.Load() {
						p.logger.Warn("ffmpeg ingest had a failed/incomplete segment, skipping temp dir cleanup for manual recovery",
							zap.String("outDir", sup.BaseOutDir()),
						)
						return
					}
					sup.Cleanup()
				}()
			}
			if slotHeld {
				<-p.ffmpegIngestCfg.RecordSem
			}
			if session != nil {
				session.Close()
			}
		}

		segmentKeys := []string{}
		for _, s := range ffmpegSegments {
			segmentKeys = append(segmentKeys, s.S3Key)
			if p.segments != nil {
				if err := p.segments.Add(ctx, p.streamID, s); err != nil {
					p.logger.Error("register ffmpeg segment failed, recording will be marked incomplete",
						zap.Int64("seq", s.Seq), zap.Error(err),
					)
					p.ffmpegIngest.incomplete.Store(true)
				}
			}
		}
		incomplete := p.ffmpegIngest != nil && p.ffmpegIngest.incomplete.Load()
		// a video track arrived — this stream had something worth recording —
		// but zero segments resulted, whether because ffmpeg ingest never got
		// far enough to even start (port allocator, StartSession, semaphore,
		// or StartRecorderSupervisor failures; see noteFFmpegIngestTrack) or
		// because the supervisor ran and exited cleanly (GaveUp() == false)
		// without ever producing output. Either way this must not read as
		// "nothing to record" without being flagged.
		emptyRecording := hadVideoTrack && len(ffmpegSegments) == 0 && !p.closedByFailure.Load()
		if !p.closedByFailure.Load() && !ffmpegGaveUp && !incomplete && len(ffmpegSegments) > 0 && p.segments != nil {
			if err := p.segments.MarkComplete(ctx, p.streamID); err != nil {
				p.logger.Warn("mark recording complete failed", zap.Error(err))
			}
		}
		p.logger.Info("recording finalized from ffmpeg pipeline", zap.Int("segments", len(ffmpegSegments)))
		if (ffmpegGaveUp || incomplete || emptyRecording) && !p.closedByFailure.Load() {
			p.logger.Warn("ffmpeg recording incomplete",
				zap.Bool("supervisorGaveUp", ffmpegGaveUp),
				zap.Bool("uploadOrRegistryFailure", incomplete),
				zap.Bool("emptyRecording", emptyRecording),
				zap.Int("segmentsCaptured", len(ffmpegSegments)),
			)
			if err := p.monitorUseCase.PublishAlert(
				ctx, domain.AlertEvent{
					Source: domain.AlertSourceStreaming,
					RoomID: p.roomID,
					ParticipantID: p.participantID,
					StreamID: p.streamID,
					StreamType: p.streamType,
					AlertType: domain.AlertRecordingIncomplete,
					Confidence: 1.0,
					CapturedAt: time.Now().UTC(),
				}, "",
			); err != nil {
				p.logger.Warn("recording incomplete alert failed", zap.Error(err))
			}
		}
		if p.closedByFailure.Load() {
			if err := p.monitorUseCase.PublishAlert(
				ctx, domain.AlertEvent{
					Source: domain.AlertSourceStreaming, 
					RoomID: p.roomID, 
					ParticipantID: p.participantID, 
					StreamID: p.streamID, 
					StreamType: p.streamType, 
					AlertType: domain.AlertStreamDropped, 
					Confidence: 1.0, 
					CapturedAt: time.Now().UTC(),
				}, "",
			); err != nil {
				p.logger.Warn("stream dropped alert failed", zap.Error(err)) // stream continue running, no return
			}
		}
		if err := p.streamUseCase.NotifyStreamEnded(
			ctx, p.roomID, p.participantID, p.streamID, p.streamType, segmentKeys, duration,
		); err != nil {
			p.logger.Error("notify stream ended failed", zap.Error(err))
		}
		if err := p.pc.Close(); err != nil {
			p.logger.Error("stream end close failed", 
				zap.String("streamId", p.streamID), 
				zap.String("roomId", p.roomID), 
				zap.Error(err),
			)
		}
		p.logger.Info("peer closed", 
			zap.Int64("durationSecs", duration),
		)
	})
}

