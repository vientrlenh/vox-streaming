package webrtc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
)

const (
	defaultRelayQueueSize = 256
	relaySignalTimeout    = 10 * time.Second
)


type AIRelayOptions struct {
	Enabled    bool
	URL        string             // AI service base url
	QueueSize  int                // queue size before dropping, default <= 0
	ICEServers []webrtc.ICEServer // often empty since it is internal communication
}

// make sure AI detect the correct student
type RelayMeta struct {
	ScheduleID        string
	ParticipantID string
	StreamID      string
	StreamType    string
}

// Contain runtime option, monitor keyframe
type AIRelayConfig struct {
	BaseURL     string
	QueueSize   int
	ICEServers  []webrtc.ICEServer
	OnPLI       func() // redirect PLI to browser
	OnConnected func() // get first keyframe from browser
}


// forward RTP video (H.264) packet from browser to AI service through a peer connection S2S (Pion's SFU pattern). Does not decode media
// Isolation rule: relay never slow down or broke recording flow. Any error become non-fatal, when AI slow then the packet dropped instead of blocked
type AIRelay struct {
	pc     *webrtc.PeerConnection
	track  *webrtc.TrackLocalStaticRTP
	queue  chan []byte
	logger *zap.Logger
	httpc  *http.Client

	baseURL string
	meta    RelayMeta

	mu     sync.Mutex
	connID string

	done      chan struct{}
	closeOnce sync.Once
	dropped   atomic.Uint64
}


// relay and start signaling async. Response immediately so it does not block track handler. If signaling encounter errors, relay close and caller still run
func NewAIRelay(ctx context.Context, cfg AIRelayConfig, codec webrtc.RTPCodecCapability, meta RelayMeta, logger *zap.Logger) (*AIRelay, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("ai relay: empty base URL")
	}
	queueSize := cfg.QueueSize
	if queueSize <= 0 {
		queueSize = defaultRelayQueueSize
	}

	me := &webrtc.MediaEngine{}
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: codec,
		PayloadType:        102,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return nil, fmt.Errorf("ai relay: register codec: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me))

	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: cfg.ICEServers})
	if err != nil {
		return nil, fmt.Errorf("ai relay: new peer connection: %w", err)
	}

	track, err := webrtc.NewTrackLocalStaticRTP(codec, "video", "vox-relay-"+meta.StreamID)
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("ai relay: new local track: %w", err)
	}

	sender, err := pc.AddTrack(track)
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("ai relay: add track: %w", err)
	}

	r := &AIRelay{
		pc:      pc,
		track:   track,
		queue:   make(chan []byte, queueSize),
		logger:  logger.With(zap.String("component", "ai-relay")),
		httpc:   &http.Client{Timeout: relaySignalTimeout},
		baseURL: strings.TrimRight(cfg.BaseURL, "/"),
		meta:    meta,
		done:    make(chan struct{}),
	}


	// RTCP feedback: AI send PLI/FIR -> get keyframe from browser
	go r.drainRTCP(sender, cfg.OnPLI)

	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		r.logger.Info("relay connection state", zap.String("state", s.String()))
		switch s {
		case webrtc.PeerConnectionStateConnected:
			if cfg.OnConnected != nil {
				cfg.OnConnected()
			}
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			r.Close()
		}
	})

	go r.writeLoop()

	// signaling run async -> does not block track video process goroutine
	go func() {
		if err := r.negotiate(ctx); err != nil {
			r.logger.Warn("ai relay signaling failed, closing", zap.Error(err))
			r.Close()
		}
	}()

	return r, nil
}


// receive marshaled RTP packet and push to queue, if full, drop with non-blocking
func (r *AIRelay) Enqueue(raw []byte) {
	select {
	case r.queue <- raw:
	default:
		r.dropped.Add(1)  // AI slow or YOLO skip frame so packet get lost here
	}
}

func (r *AIRelay) writeLoop() {
	for {
		select {
		case <-r.done:
			return
		case raw := <-r.queue:
			var pkt rtp.Packet
			if err := pkt.Unmarshal(raw); err != nil {
				continue
			}
			// overwrite SSRC + payload type base on negotiated track
			if err := r.track.WriteRTP(&pkt); err != nil {
				if errors.Is(err, io.ErrClosedPipe) {
					return
				}
				r.logger.Debug("relay write failed", zap.Error(err))
			}
		}
	}
}

func (r *AIRelay) drainRTCP(sender *webrtc.RTPSender, onPLI func()) {
	buf := make([]byte, 1500)
	for {
		n, _, err := sender.Read(buf)
		if err != nil {
			return // sender đóng
		}
		pkts, err := rtcp.Unmarshal(buf[:n])
		if err != nil {
			continue
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				if onPLI != nil {
					onPLI()
				}
			}
		}
	}
}

func (r *AIRelay) negotiate(ctx context.Context) error {
	offer, err := r.pc.CreateOffer(nil)
	if err != nil {
		return fmt.Errorf("create offer: %w", err)
	}
	gatherComplete := webrtc.GatheringCompletePromise(r.pc)
	if err := r.pc.SetLocalDescription(offer); err != nil {
		return fmt.Errorf("set local description: %w", err)
	}

	// non-trickle: wait for done gathering then send full SDP to AI HTTP endpoint
	select {
	case <-gatherComplete:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(relaySignalTimeout):
		return fmt.Errorf("ice gathering timeout")
	}

	answer, connID, err := r.postOffer(ctx, r.pc.LocalDescription().SDP)
	if err != nil {
		return err
	}
	r.setConnID(connID)

	if err := r.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	}); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}
	return nil
}

// aiOfferRequest / aiOfferResponse là contract signaling với AI service.
// Endpoint AI (`POST /webrtc/offer`) phải khớp shape này (kèm metadata định danh).
type aiOfferRequest struct {
	SDP           string `json:"sdp"`
	Type          string `json:"type"`
	ScheduleID        string `json:"scheduleId"`
	ParticipantID string `json:"participantId"`
	StreamID      string `json:"streamId"`
	StreamType    string `json:"streamType"`
}

type aiOfferResponse struct {
	SDP          string `json:"sdp"`
	Type         string `json:"type"`
	ConnectionID string `json:"connectionId"`
}

func (r *AIRelay) postOffer(ctx context.Context, sdp string) (answerSDP, connID string, err error) {
	reqBody, err := json.Marshal(aiOfferRequest{
		SDP:           sdp,
		Type:          "offer",
		ScheduleID:        r.meta.ScheduleID,
		ParticipantID: r.meta.ParticipantID,
		StreamID:      r.meta.StreamID,
		StreamType:    r.meta.StreamType,
	})
	if err != nil {
		return "", "", fmt.Errorf("marshal offer: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, relaySignalTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.baseURL+"/webrtc/offer", bytes.NewReader(reqBody))
	if err != nil {
		return "", "", fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.httpc.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("post offer: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", "", fmt.Errorf("ai offer status %d: %s", resp.StatusCode, string(body))
	}

	var out aiOfferResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", fmt.Errorf("decode answer: %w", err)
	}
	if out.SDP == "" {
		return "", "", fmt.Errorf("ai answer empty sdp")
	}
	return out.SDP, out.ConnectionID, nil
}

// stop relay with idempotence
func (r *AIRelay) Close() {
	r.closeOnce.Do(func() {
		close(r.done)
		if id := r.getConnID(); id != "" {
			go r.deleteConnection(id) // delete connection
		}
		if err := r.pc.Close(); err != nil {
			r.logger.Debug("relay pc close", zap.Error(err))
		}
		if d := r.dropped.Load(); d > 0 {
			r.logger.Info("ai relay closed", zap.Uint64("droppedPackets", d))
		}
	})
}

func (r *AIRelay) deleteConnection(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), relaySignalTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.baseURL+"/webrtc/connections/"+id, nil)
	if err != nil {
		return
	}
	resp, err := r.httpc.Do(req)
	if err != nil {
		r.logger.Debug("relay delete connection failed", zap.Error(err))
		return
	}
	resp.Body.Close()
}

func (r *AIRelay) setConnID(id string) {
	r.mu.Lock()
	r.connID = id
	r.mu.Unlock()
}

func (r *AIRelay) getConnID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connID
}
