package webrtc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/vientrlenh/vox-streaming/internal/domain"
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
	ICEServers    []webrtc.ICEServer
	FrameInterval time.Duration
	TempDir       string
	AIRelay       AIRelayOptions
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
	recorder *recorder.SegmentedRecorder
	aiRelay AIRelayOptions
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
	logger *zap.Logger,
) (*Peer, error) {
	me := &webrtc.MediaEngine{}
	
	// Firefox, old Chrome
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264, 
			ClockRate: 90000,
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
		},
		PayloadType: 102,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("H.264 codec register with level id 42e01f failed: %w", err)
	}

	// high profile: Chrome/Safari, better quality and bitrate
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeH264,
			ClockRate: 90000, 
			SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640028",
		},
		PayloadType: 104,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("H.264 codec register with level id 640028 failed: %w", err)
	}

	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType: webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels: 2,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("Opus codec register failed: %w", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(me))
	pc, err := api.NewPeerConnection(webrtc.Configuration{
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
		aiRelay:       cfg.AIRelay,
		done:          make(chan struct{}),
		logger: logger.With(
			zap.String("roomId", roomID),
			zap.String("participantId", participantID),
			zap.String("streamId", streamID),
			zap.String("streamType", streamType),
		),
	}
	p.closedByFailure.Store(false)
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

	p.pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		p.logger.Info("track received",
			zap.String("kind",
				track.Kind().String()),
			zap.String("codec", track.Codec().MimeType),
		)
		switch track.Kind() {
		case webrtc.RTPCodecTypeVideo:
			go p.handleVideoTrack(track)
		case webrtc.RTPCodecTypeAudio:
			go p.handleAudioTrack(track)
		}
	})
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

		segmentKeys := []string{}
		if p.recorder != nil {
			if p.closedByFailure.Load() {
				p.recorder.Close() // stream failed, no upload
			} else {
				segments, err := p.recorder.Finalize(ctx)
				if err != nil {
					p.logger.Warn("recording finalize failed", zap.Error(err))
				} else {
					for _, s := range segments {
						segmentKeys = append(segmentKeys, s.S3Key)
					}
				}
			}
		}
		if p.closedByFailure.Load() {
			if err := p.monitorUseCase.PublishAlert(
				ctx, p.roomID, p.participantID, p.streamID, domain.AlertStreamDropped, 1.0, time.Now().UTC(),
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

