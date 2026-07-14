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
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/vientrlenh/vox-streaming/internal/domain"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/cache"
	"github.com/vientrlenh/vox-streaming/internal/infrastructure/storage"
	"github.com/vientrlenh/vox-streaming/internal/recorder"
	"github.com/vientrlenh/vox-streaming/internal/recorder/ffmpegingest"
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
	Enabled            bool
	Allocator          *ffmpegingest.PortAllocator
	RecordSem          chan struct{} // capacity = max concurrent ffmpeg recorders across all peers
	SegmentSeconds     int
	MaxRestartAttempts int 
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
	session       *ffmpegingest.Session
	supervisor    *ffmpegingest.RecorderSupervisor
	recordSlotHeld bool // true once a RecordSem slot was acquired for this stream

	segMu      sync.Mutex
	segs       []cache.SegmentMeta
	uploadDone chan struct{}
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

	recorder *recorder.SegmentedRecorder
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
	if cfg.FFmpegIngest.Enabled && cfg.FFmpegIngest.Allocator != nil {
		p.ffmpegIngest = &ffmpegIngestState{}
	}
	if storage != nil {
		rec, err := recorder.NewSegmentedRecorder(roomID, streamID, cfg.TempDir, storage, logger)
		if err != nil {
			logger.Warn("recorder init failed, recording disabled", zap.Error(err))
		} else {
			p.recorder = rec
		}
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
	ready := vt != nil && at != nil && st.session == nil
	st.mu.Unlock()
	if !ready {
		return
	}

	session, err := ffmpegingest.StartSession(context.Background(), p.ffmpegIngestCfg.Allocator, p.tempDir, vt, vr, at, ar, p.logger)
	if err != nil {
		p.logger.Warn("ffmpeg ingest bridge start failed", zap.Error(err))
		return
	}
	st.mu.Lock()
	st.session = session
	st.mu.Unlock()

	select {
	case p.ffmpegIngestCfg.RecordSem <- struct{}{}:
		outDir := filepath.Join(p.tempDir, "ffmpeg-ingest", p.streamID)
		sup, err := ffmpegingest.StartRecorderSupervisor(session.SDPPath(), outDir, p.ffmpegIngestCfg.SegmentSeconds, p.ffmpegIngestCfg.MaxRestartAttempts, p.logger)
		if err != nil {
			p.logger.Warn("ffmpeg recorder start failed", zap.Error(err))
			<-p.ffmpegIngestCfg.RecordSem
			return
		}
		st.mu.Lock()
		st.supervisor = sup
		st.recordSlotHeld = true
		st.uploadDone = make(chan struct{})
		st.mu.Unlock()
		go p.runFFmpegSegmentUploader(st, sup)
	default:
		p.logger.Warn("ffmpeg ingest max concurrent recorders reached, skipping ffmpeg recording for this stream")
	}
}


func (p *Peer) runFFmpegSegmentUploader(st *ffmpegIngestState, sup *ffmpegingest.RecorderSupervisor) {
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
			p.logger.Warn("ffmpeg segment upload failed permanently", zap.Int64("seq", seq), zap.Error(err))
			os.Remove(path)
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

		f, err := os.Open(path)
		if err != nil {
			return "", 0, fmt.Errorf("open segment: %w", err)
		}
		key, err := p.storage.UploadFFmpegSegment(ctx, p.roomID, p.streamID, seq, f)
		f.Close()
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

	// tier 2: wire NALSink into recorder
	if p.recorder != nil {
		fe.SetNALSink(func(nals [][]byte, hasIDR bool, dur uint32) {
			p.recorder.WriteVideoNALs(nals, hasIDR, dur)
		})
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
	if p.recorder != nil {
		if err := p.recorder.StartAudio(2); err != nil {
			p.logger.Warn("start audio recorder failed", zap.Error(err))
		}
	}
	buf := make([]byte, 4096)
	var prevTS uint32
	var hasFirst bool

	for {
		n, _, err := track.Read(buf)
		if err != nil {
			return
		}
		if p.ffmpegIngest != nil {
			p.ffmpegIngest.forwardAudio(buf[:n])
		}
		if p.recorder == nil {
			continue
		}
		var pkt rtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		sampleCount := uint32(recorder.OpusFrameSize) // 960 default (20ms at 48 kHz)
		if hasFirst && pkt.Timestamp > prevTS {
			diff := pkt.Timestamp - prevTS
			if diff <= 5760 { // sanity: max 120ms
				sampleCount = diff
			}
		}
		prevTS = pkt.Timestamp
		hasFirst = true
		p.recorder.WriteAudioPacket(pkt.Payload, sampleCount)
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

		// ffmpegSegments/ffmpegGaveUp decide, below, whether ffmpeg's output
		// replaces SegmentedRecorder's for this stream (cutover plan Part C).
		var ffmpegSegments []cache.SegmentMeta
		ffmpegGaveUp := false
		if p.ffmpegIngest != nil {
			p.ffmpegIngest.mu.Lock()
			session := p.ffmpegIngest.session
			sup := p.ffmpegIngest.supervisor
			slotHeld := p.ffmpegIngest.recordSlotHeld
			uploadDone := p.ffmpegIngest.uploadDone
			p.ffmpegIngest.mu.Unlock()
			// stop the recorder first so ffmpeg can flush its current segment
			// before its RTP source (session) gets cut off.
			if sup != nil {
				sup.Stop(5 * time.Second)
				if uploadDone != nil {
					select {
					case <-uploadDone:
					case <-time.After(20 * time.Second):
						p.logger.Warn("ffmpeg segment upload did not finish in time, using partial results")
					}
				}
				p.ffmpegIngest.segMu.Lock()
				ffmpegSegments = append([]cache.SegmentMeta(nil), p.ffmpegIngest.segs...)
				p.ffmpegIngest.segMu.Unlock()
				ffmpegGaveUp = sup.GaveUp()
				sup.Cleanup()
			}
			if slotHeld {
				<-p.ffmpegIngestCfg.RecordSem
			}
			if session != nil {
				session.Close()
			}
		}

		segmentKeys := []string{}
		useFFmpeg := !p.closedByFailure.Load() && len(ffmpegSegments) > 0 && !ffmpegGaveUp
		if useFFmpeg {
			for _, s := range ffmpegSegments {
				segmentKeys = append(segmentKeys, s.S3Key)
				if p.segments != nil {
					if err := p.segments.Add(ctx, p.streamID, s); err != nil {
						p.logger.Warn("register ffmpeg segment failed", zap.Int64("seq", s.Seq), zap.Error(err))
					}
				}
			}
			if p.segments != nil {
				if err := p.segments.MarkComplete(ctx, p.streamID); err != nil {
					p.logger.Warn("mark recording complete failed", zap.Error(err))
				}
			}
			if p.recorder != nil {
				p.recorder.Close() // ffmpeg won for this stream, discard SegmentedRecorder's output
			}
			p.logger.Info("recording finalized from ffmpeg pipeline", zap.Int("segments", len(ffmpegSegments)))
		} else if p.recorder != nil {
			if p.closedByFailure.Load() {
				p.recorder.Close() // stream failed, no upload
			} else {
				segments, err := p.recorder.Finalize(ctx)
				if err != nil {
					p.logger.Warn("recording finalize failed", zap.Error(err))
				} else {
					for _, s := range segments {
						segmentKeys = append(segmentKeys, s.S3Key)
						if p.segments != nil {
							if err := p.segments.Add(ctx, p.streamID, cache.SegmentMeta{
								Seq: s.Seq,
								S3Key: s.S3Key,
								StartedAt: s.StartedAt,
								EndedAt: s.EndedAt,
								UploadedAt: time.Now().UTC(),
							}); err != nil {
								p.logger.Warn("register server segment failed",
									zap.Int64("seq", s.Seq),
									zap.Error(err),
								)
							}
						}
					}
					if p.segments != nil && len(segments) > 0 {
						if err := p.segments.MarkComplete(ctx, p.streamID); err != nil {
							p.logger.Warn("mark server recording complete failed", zap.Error(err))
						}
					}
				}
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

