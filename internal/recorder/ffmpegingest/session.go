package ffmpegingest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
)


type Session struct {
	ports   allocatedPorts
	alloc   *PortAllocator
	sdpPath string
	cancel  context.CancelFunc
	done    chan struct{}

	videoConn *net.UDPConn
	audioConn *net.UDPConn
	logger    *zap.Logger
}


func StartSession(
	ctx context.Context,
	alloc *PortAllocator,
	tempDir string,
	videoTrack *webrtc.TrackRemote,
	videoReceiver *webrtc.RTPReceiver,
	audioTrack *webrtc.TrackRemote,
	audioReceiver *webrtc.RTPReceiver,
	logger *zap.Logger,
) (*Session, error) {
	ports, err := alloc.Allocate()
	if err != nil {
		return nil, fmt.Errorf("allocate ports: %w", err)
	}

	sdp := buildSDP(codecFromTrack(videoTrack), codecFromTrack(audioTrack), ports)
	sdpPath := filepath.Join(tempDir, fmt.Sprintf("ffmpeg-ingest-%s.sdp", uuid.NewString()))
	if err := os.WriteFile(sdpPath, []byte(sdp), 0o644); err != nil {
		alloc.Release(ports)
		return nil, fmt.Errorf("write sdp: %w", err)
	}

	videoConn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: ports.videoRTP})
	if err != nil {
		alloc.Release(ports)
		_ = os.Remove(sdpPath)
		return nil, fmt.Errorf("dial video RTP forward: %w", err)
	}
	audioConn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: ports.audioRTP})
	if err != nil {
		videoConn.Close()
		alloc.Release(ports)
		_ = os.Remove(sdpPath)
		return nil, fmt.Errorf("dial audio RTP forward: %w", err)
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	s := &Session{
		ports:     ports,
		alloc:     alloc,
		sdpPath:   sdpPath,
		cancel:    cancel,
		done:      make(chan struct{}),
		videoConn: videoConn,
		audioConn: audioConn,
		logger:    logger,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		logForward(logger, "video RTCP", forwardRTCP(sessionCtx, videoReceiver, ports.videoRTCP))
	}()
	go func() {
		defer wg.Done()
		logForward(logger, "audio RTCP", forwardRTCP(sessionCtx, audioReceiver, ports.audioRTCP))
	}()
	go func() {
		wg.Wait()
		close(s.done)
	}()

	logger.Info("ffmpeg ingest bridge started", zap.String("sdpPath", sdpPath))
	return s, nil
}


func (s *Session) ForwardVideoRTP(raw []byte) {
	if _, err := s.videoConn.Write(raw); err != nil {
		s.logger.Debug("ffmpeg ingest video RTP forward failed", zap.Error(err))
	}
}

func (s *Session) ForwardAudioRTP(raw []byte) {
	if _, err := s.audioConn.Write(raw); err != nil {
		s.logger.Debug("ffmpeg ingest audio RTP forward failed", zap.Error(err))
	}
}


func (s *Session) SDPPath() string {
	return s.sdpPath
}

func (s *Session) Close() {
	s.cancel()
	<-s.done
	s.videoConn.Close()
	s.audioConn.Close()
	s.alloc.Release(s.ports)
	_ = os.Remove(s.sdpPath)
}

func logForward(logger *zap.Logger, name string, err error) {
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Debug("ffmpeg ingest forward stopped", zap.String("stream", name), zap.Error(err))
	}
}

func codecFromTrack(track *webrtc.TrackRemote) negotiatedCodec {
	codec := track.Codec()
	if strings.EqualFold(codec.MimeType, webrtc.MimeTypeOpus) {
		return negotiatedCodec{
			payloadType: uint8(track.PayloadType()),
			clockRate:   codec.ClockRate,
			channels:    uint16(codec.Channels),
		}
	}
	return negotiatedCodec{
		payloadType: uint8(track.PayloadType()),
		clockRate:   codec.ClockRate,
		fmtp:        codec.SDPFmtpLine,
	}
}
