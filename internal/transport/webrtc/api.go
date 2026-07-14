package webrtc

import (
	"fmt"
	"net"

	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
)

// ICEConfig controls how the shared WebRTC API gathers ICE candidates.
type ICEConfig struct {
	// UDPPort, when > 0, muxes all peer media onto this single UDP port instead
	// of a random ephemeral one — so only one port needs to be opened/forwarded
	// (essential for containers and firewalls). 0 keeps Pion's default behaviour.
	UDPPort int

	// NAT1To1IPs are public IPs advertised as host candidates, for a server
	// behind a 1:1 NAT (cloud VM / load balancer). Empty on a flat/local network.
	NAT1To1IPs []string
}

// NewWebRTCAPI builds the *webrtc.API shared by every Peer. The MediaEngine
// (H.264/Opus) and SettingEngine (UDP mux / NAT) are configured once here rather
// than rebuilt per peer — which also lets all peers share a single UDP port.
//
// The returned closer releases the muxed UDP socket; call it on shutdown. It is
// a no-op when the UDP mux is disabled.
func NewWebRTCAPI(cfg ICEConfig, logger *zap.Logger) (*webrtc.API, func() error, error) {
	me := &webrtc.MediaEngine{}
	if err := registerCodecs(me); err != nil {
		return nil, nil, err
	}

	settingEngine := webrtc.SettingEngine{}
	closer := func() error { return nil }

	if cfg.UDPPort > 0 {
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: cfg.UDPPort})
		if err != nil {
			return nil, nil, fmt.Errorf("listen udp mux :%d: %w", cfg.UDPPort, err)
		}
		// Absorb bursts while the RTP read path is momentarily blocked (e.g. a
		// synchronous fMP4 fragment write to disk). Too small a buffer drops
		// packets on high-bitrate screen share -> corrupt macroblocks in the
		// recording. The OS may clamp this; that's fine.
		if err := udpConn.SetReadBuffer(8 * 1024 * 1024); err != nil {
			logger.Warn("set udp read buffer failed (OS cap?)", zap.Error(err))
		}
		mux := webrtc.NewICEUDPMux(nil, udpConn)
		settingEngine.SetICEUDPMux(mux)
		closer = mux.Close
		logger.Info("webrtc ICE UDP mux enabled", zap.Int("port", cfg.UDPPort))
	} else {
		logger.Warn("webrtc UDP mux disabled — using ephemeral UDP ports (not container/NAT friendly); set WEBRTC_UDP_PORT to enable")
	}

	if len(cfg.NAT1To1IPs) > 0 {
		settingEngine.SetICEAddressRewriteRules(
			webrtc.ICEAddressRewriteRule{
				External: cfg.NAT1To1IPs, 
				AsCandidateType: webrtc.ICECandidateTypeHost, 
				Mode: webrtc.ICEAddressRewriteReplace,
			},
		)
		logger.Info("webrtc NAT 1:1 host IPs set", zap.Strings("ips", cfg.NAT1To1IPs))
	}

	api := webrtc.NewAPI(
		webrtc.WithMediaEngine(me),
		webrtc.WithSettingEngine(settingEngine),
	)
	return api, closer, nil
}

// registerCodecs registers exactly the codecs the recorder and AI relay expect:
// H.264 (baseline 42e01f + high 640028) and Opus. Kept in one place so the shared
// API and any other consumer negotiate an identical codec set.
func registerCodecs(me *webrtc.MediaEngine) error {
	// NACK (retransmit lost packets) + PLI/FIR (keyframe requests) must be
	// advertised on the codec or WebRTC won't negotiate them. Without NACK, a
	// single lost RTP packet corrupts the picture until the next keyframe — very
	// visible on screen share. Mirrors Pion's RegisterDefaultCodecs feedback set.
	videoFeedback := []webrtc.RTCPFeedback{
		{Type: "goog-remb"},
		{Type: "ccm", Parameter: "fir"},
		{Type: "nack"},
		{Type: "nack", Parameter: "pli"},
	}

	// H.264 baseline — Firefox, older Chrome.
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeH264,
			ClockRate:    90000,
			SDPFmtpLine:  "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
			RTCPFeedback: videoFeedback,
		},
		PayloadType: 102,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("register H.264 42e01f: %w", err)
	}

	// H.264 high profile — Chrome/Safari, better quality/bitrate.
	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeH264,
			ClockRate:    90000,
			SDPFmtpLine:  "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640028",
			RTCPFeedback: videoFeedback,
		},
		PayloadType: 104,
	}, webrtc.RTPCodecTypeVideo); err != nil {
		return fmt.Errorf("register H.264 640028: %w", err)
	}

	if err := me.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeOpus,
			ClockRate: 48000,
			Channels:  2,
		},
		PayloadType: 111,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return fmt.Errorf("register Opus: %w", err)
	}
	return nil
}
