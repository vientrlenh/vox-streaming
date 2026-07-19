package recorder

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
)

// controls how often forwarding throughput is logged —
// gives a direct answer, next to any hang/exit log, to "did our side stop
// sending, or is ffmpeg receiving fine and stuck internally?" instead of
// having to infer it from ffmpeg's own stderr.
const forwardStatsInterval = 30 * time.Second

type Session struct {
	ports   allocatedPorts
	alloc   *PortAllocator
	sdpPath string
	cancel  context.CancelFunc
	done    chan struct{}

	videoConn *net.UDPConn
	audioConn *net.UDPConn
	logger    *zap.Logger

	// count forward attempts (incremented before
	// the UDP write), not confirmed deliveries — UDP has no delivery ack, so
	// "attempted" is the honest ceiling. videoWriteErrors/audioWriteErrors
	// track how many of those attempts actually failed at the local socket
	// write, so the periodic stats log can tell healthy-but-attempted-only
	// apart from actually-failing-to-send.
	videoPackets     atomic.Uint64
	audioPackets     atomic.Uint64
	videoWriteErrors atomic.Uint64
	audioWriteErrors atomic.Uint64
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
	if err := os.MkdirAll(tempDir, 0o700); err != nil {
		return nil, fmt.Errorf("create segment temp dir: %w", err)
	}
	
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
	wg.Add(3)
	go func() {
		defer wg.Done()
		logForward(logger, "video RTCP", forwardRTCP(sessionCtx, videoReceiver, ports.videoRTCP))
	}()
	go func() {
		defer wg.Done()
		logForward(logger, "audio RTCP", forwardRTCP(sessionCtx, audioReceiver, ports.audioRTCP))
	}()
	go func() {
		defer wg.Done()
		s.logForwardStats(sessionCtx)
	}()
	go func() {
		wg.Wait()
		close(s.done)
	}()

	logger.Info("ffmpeg ingest bridge started", zap.String("sdpPath", sdpPath))
	return s, nil
}


func (s *Session) ForwardVideoRTP(raw []byte) {
	s.videoPackets.Add(1)
	if _, err := s.videoConn.Write(raw); err != nil {
		s.videoWriteErrors.Add(1)
		s.logger.Debug("ffmpeg ingest video RTP forward failed", zap.Error(err))
	}
}

func (s *Session) ForwardAudioRTP(raw []byte) {
	s.audioPackets.Add(1)
	if _, err := s.audioConn.Write(raw); err != nil {
		s.audioWriteErrors.Add(1)
		s.logger.Debug("ffmpeg ingest audio RTP forward failed", zap.Error(err))
	}
}


func (s *Session) logForwardStats(ctx context.Context) {
	ticker := time.NewTicker(forwardStatsInterval)
	defer ticker.Stop()
	var lastVideo, lastAudio, lastVideoErr, lastAudioErr uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			video := s.videoPackets.Load()
			audio := s.audioPackets.Load()
			videoErr := s.videoWriteErrors.Load()
			audioErr := s.audioWriteErrors.Load()
			s.logger.Info("ffmpeg ingest forwarding stats",
				zap.Uint64("videoPacketsAttemptedTotal", video),
				zap.Uint64("audioPacketsAttemptedTotal", audio),
				zap.Uint64("videoPacketsAttemptedSinceLast", video-lastVideo),
				zap.Uint64("audioPacketsAttemptedSinceLast", audio-lastAudio),
				zap.Uint64("videoWriteErrorsTotal", videoErr),
				zap.Uint64("audioWriteErrorsTotal", audioErr),
				zap.Uint64("videoWriteErrorsSinceLast", videoErr-lastVideoErr),
				zap.Uint64("audioWriteErrorsSinceLast", audioErr-lastAudioErr),
			)
			lastVideo, lastAudio = video, audio
			lastVideoErr, lastAudioErr = videoErr, audioErr
		}
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
