package recorder

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sync"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"go.uber.org/zap"
)

const (
	nalTypeSPS = 7
	nalTypePPS = 8
	nalTypeIDR = 5

	rtpNALTypeSTAPA = 24
	rtpNALTypeFUA   = 28
)

var annexBStartCode = []byte{0x00, 0x00, 0x00, 0x01}

type fuaBuf struct {
	data []byte
}

// receive all NAL units each completed picture
// hasIDR = true if picture contains IDR frame
type NALSink func(nals [][]byte, hasIDR bool)

type FrameExtractor struct {
	track *webrtc.TrackRemote
	pc *webrtc.PeerConnection
	logger *zap.Logger

	mu sync.Mutex
	sps []byte	// latest sequence parameter set NAL
	pps []byte 	// latest picture parameter set NAL
	idrNALs [][]byte //NAL units of the latest complete IDR picture
	idrReady bool

	idrCh chan struct{} // được thông báo khi có một IDR frame được buffer

	fua *fuaBuf // FU-A reassembly 
	picBuf [][]byte // NALs accumulating for the current RTP picture
	sink NALSink // optional, called for every complete picture
}

func NewFrameExtractor(track *webrtc.TrackRemote, pc *webrtc.PeerConnection, logger *zap.Logger) *FrameExtractor {
	return &FrameExtractor{
		track: track, 
		pc: pc, 
		logger: logger, 
		idrCh: make(chan struct{}, 1),
	}
}

// return a channel that receives a value each time a new IDR frame is buffered
func (fe *FrameExtractor) IDRReady() <-chan struct{} {
	return fe.idrCh
}

// register sink to receive every picture for tier 2 recording
func (fe *FrameExtractor) SetNALSink(sink NALSink) {
	fe.sink = sink
}

func (fe *FrameExtractor) RequestKeyFrame() {
	if err := fe.pc.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{
			MediaSSRC: uint32(fe.track.SSRC()),
		},
	}); err != nil {
		fe.logger.Warn("PLI send failed", zap.Error(err))
	}
}

func (fe *FrameExtractor) ReadLoop(ctx context.Context) {
	buf := make([]byte, 1500) // MTU-sized
	for {
		select {
		case<-ctx.Done():
			return
		default:
		}
		n, _, err := fe.track.Read(buf)
		if err != nil {
			return
		}
		var pkt rtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		fe.ingest(&pkt)
	}
}

func (fe *FrameExtractor) ingest(pkt *rtp.Packet) {
	p := pkt.Payload
	if len(p) == 0 {
		return
	}
	nalType := p[0] & 0x1F
	switch {
	case nalType >= 1 && nalType <= 23:
		fe.addNAL(p, pkt.Marker)
	case nalType == rtpNALTypeSTAPA:
		fe.ingestSTAPA(p[1:], pkt.Marker)
	case nalType == rtpNALTypeFUA:
		fe.ingestFUA(p, pkt.Marker)
	}
}

func (fe *FrameExtractor) addNAL(raw []byte, marker bool) {
	nal := append([]byte(nil), raw...)
	fe.picBuf = append(fe.picBuf, nal)

	switch nal[0] & 0x1F {
	case nalTypeSPS:
		fe.mu.Lock()
		fe.sps = nal
		fe.mu.Unlock()
	case nalTypePPS:
		fe.mu.Lock()
		fe.pps = nal
		fe.mu.Unlock()
	}
	if marker {
		fe.commitPicture()
	}
}

// handle STAP-A aggregation packets (RFC 6184 5.7.1)
func (fe *FrameExtractor) ingestSTAPA(p []byte, marker bool) {
	for len(p) >= 2 {
		size := int(p[0])<<8 | int(p[1])
		p = p[2:]
		if len(p) < size {
			break
		}
		fe.addNAL(p[:size], false)
		p = p[size:]
	}
	if marker {
		fe.commitPicture()
	}
}

// handle FU-A fragmentation uints (RFC 6184 5.8)
func (fe *FrameExtractor) ingestFUA(p []byte, marker bool) {
	if len(p) < 2 {
		return
	}
	fuHeader := p[1]
	startBit := fuHeader&0x80 != 0
	fuNALType := fuHeader & 0x1F
	payload := p[2:]

	if startBit {
		reconstructed := (p[0] & 0xE0) | fuNALType
		fe.fua = &fuaBuf{data: []byte{reconstructed}}
	}
	if fe.fua == nil {
		return // missed start packet
	}
	fe.fua.data = append(fe.fua.data, payload...)

	if marker {
		complete := fe.fua.data
		fe.fua = nil
		fe.addNAL(complete, true)
	}
}


func (fe *FrameExtractor) commitPicture() {
	if len(fe.picBuf) == 0 {
		return
	}
	hasIDR := false
	for _, n := range fe.picBuf {
		if len(n) > 0 && n[0]&0x1F == nalTypeIDR {
			hasIDR = true
			break
		}
	}

	// tier 2: send all NAL units to recorder (IDR and non-IDR)
	if fe.sink != nil {
		fe.sink(fe.picBuf, hasIDR)
	}

	// tier 1: only buffer IDR to extract JPEG
	if hasIDR {
		fe.mu.Lock()
		fe.idrNALs = fe.picBuf
		fe.idrReady = true
		fe.mu.Unlock()
		select {
		case fe.idrCh <- struct{}{}:
		default:
		}
	}
	fe.picBuf = nil
}


// encode the latest buffered IDR frame as JPEG via ffmpeg
func (fe *FrameExtractor) CaptureJPEG(ctx context.Context) ([]byte, error) {
	fe.mu.Lock()
	if !fe.idrReady {
		fe.mu.Unlock()
		return nil, nil
	}
	sps := append([]byte(nil), fe.sps...)
	pps := append([]byte(nil), fe.pps...)
	nals := make([][]byte, len(fe.idrNALs))
	for i, n := range fe.idrNALs {
		nals[i] = append(nals[i], n...)
	}
	fe.mu.Unlock()

	var h264 bytes.Buffer
	if len(sps) > 0 {
		h264.Write(annexBStartCode)
		h264.Write(sps)
	}
	if len(pps) > 0 {
		h264.Write(annexBStartCode)
		h264.Write(pps)
	}
	for _, n := range nals {
		h264.Write(annexBStartCode)
		h264.Write(n)
	}
	return h264ToJPEG(ctx, h264.Bytes())
}

func h264ToJPEG(ctx context.Context, h264Data []byte) ([]byte, error) {
	cmd := exec.CommandContext(ctx, 
		"ffmpeg", 
		"-hide_banner", "-loglevel", "error", 
		"-f", "h264", "-i", "pipe:0",
		"-frames:v", "1", 
		"-f", "image2", "-vcodec", "mjpeg", 
		"-q:v", "3", // 1 - 31 (from best to worst)
		"pipe:1",
	)
	cmd.Stdin = bytes.NewReader(h264Data)
	var out, errBuf bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errBuf
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("ffmpeg h264 to jpeg: %w: %s", err, errBuf.String())
	}
	if out.Len() == 0 {
		return nil, fmt.Errorf("ffmpeg produced empty output")
	}
	return out.Bytes(), nil
}


