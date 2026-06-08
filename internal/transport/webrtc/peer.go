package webrtc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
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

	useCase *usecase.StreamUseCase
	logger  *zap.Logger

	done      chan struct{}
	once      sync.Once
	startOnce sync.Once
}

func NewPeer(
	cfg PeerConfig,
	roomID, participantID, streamType string,
	useCase *usecase.StreamUseCase,
	logger *zap.Logger,
) (*Peer, error) {
	me := &webrtc.MediaEngine{}
	if err := me.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register codecs: %w", err)
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
		useCase:       useCase,
		done:          make(chan struct{}),
		logger: logger.With(
			zap.String("room_id", roomID),
			zap.String("participant_id", participantID),
			zap.String("stream_id", streamID),
			zap.String("stream_type", streamType),
		),
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
				if err := p.useCase.NotifyStreamStarted(
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
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-p.done
		cancel()
	}()

	go func() {
		ticker := time.NewTicker(p.frameInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				seq := p.frameSeq.Add(1)
				frameURL := ""
				if err := p.useCase.PublishFrame(ctx, p.roomID, p.participantID, p.streamID, p.streamType, frameURL, seq); err != nil {
					p.logger.Warn("publish frame failed", zap.Int64("seq", seq), zap.Error(err))
				}
			}
		}
	}()

	buf := make([]byte, 4096)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
		}
	}
}

func (p *Peer) handleAudioTrack(track *webrtc.TrackRemote) {
	p.logger.Info("audio track drain started", 
		zap.String("codec", track.Codec().MimeType), 
	)
	buf := make([]byte, 4096)
	for {
		if _, _, err := track.Read(buf); err != nil {
			return
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
		if err := p.useCase.NotifyStreamEnded(
			ctx, p.roomID, p.participantID, p.streamID, "", duration,
		); err != nil {
			p.logger.Error("notify stream ended failed", zap.Error(err))
		}
		_ = p.pc.Close()
		p.logger.Info("peer closed", zap.Int64("duration_secs", duration))
	})
}
