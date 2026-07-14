package recorder

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"sync"
	"time"

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

const minPLIInterval = 2 * time.Second

var annexBStartCode = []byte{0x00, 0x00, 0x00, 0x01}

type fuaBuf struct {
	data []byte
}

// reorderBuffer resequences RTP packets so downstream NAL processing always
// sees a monotonically increasing sequence number, absorbing the reordering
// that NACK-triggered retransmission naturally introduces (a repaired packet
// arrives later than packets that kept flowing while it was in flight).
// Genuinely lost packets (no retransmission within reorderMaxWait) still
// surface as a real gap to checkSeqGap, same as before.
type reorderBuffer struct {
	pending  map[uint16]*rtp.Packet
	nextSeq  uint16
	hasNext  bool
	gapSince time.Time
}

const (
	reorderMaxWait = 120 * time.Millisecond // budget for one NACK round-trip
	reorderMaxHold = 64                     // bound memory under pathological loss bursts
)

// push returns pkt (and any now-contiguous packets it unblocked) in sequence
// order. Returns nil if pkt is buffered pending an earlier gap, or discarded
// as a duplicate / already-past packet.
func (rb *reorderBuffer) push(pkt *rtp.Packet) []*rtp.Packet {
	if rb.pending == nil {
		rb.pending = make(map[uint16]*rtp.Packet)
	}
	if !rb.hasNext {
		rb.nextSeq = pkt.SequenceNumber
		rb.hasNext = true
	}

	delta := int32(int16(pkt.SequenceNumber - rb.nextSeq))
	if delta < 0 {
		return nil // duplicate, or arrived after we already gave up on this seq
	}
	if delta > 0 {
		if len(rb.pending) == 0 {
			rb.gapSince = time.Now()
		}
		rb.pending[pkt.SequenceNumber] = pkt
		if len(rb.pending) >= reorderMaxHold || time.Since(rb.gapSince) >= reorderMaxWait {
			return rb.giveUpAndDrain()
		}
		return nil
	}

	// delta == 0: exactly what we're waiting for
	out := []*rtp.Packet{pkt}
	rb.nextSeq++
	for {
		next, ok := rb.pending[rb.nextSeq]
		if !ok {
			break
		}
		delete(rb.pending, rb.nextSeq)
		out = append(out, next)
		rb.nextSeq++
	}
	return out
}

// giveUpAndDrain abandons the current gap (nextSeq is presumed permanently
// lost), advances past it, and drains any now-contiguous run from pending.
func (rb *reorderBuffer) giveUpAndDrain() []*rtp.Packet {
	rb.nextSeq++
	var out []*rtp.Packet
	for {
		next, ok := rb.pending[rb.nextSeq]
		if !ok {
			break
		}
		delete(rb.pending, rb.nextSeq)
		out = append(out, next)
		rb.nextSeq++
	}
	if len(rb.pending) > 0 {
		rb.gapSince = time.Now() // another gap remains, restart its wait budget
	}
	return out
}

// receive all NAL units each completed picture
// hasIDR = true if picture contains IDR frame
// dur is the picture duration in 90kHz ticks derived from RTP timestamps
type NALSink func(nals [][]byte, hasIDR bool, dur uint32)


// receive each parsed RTP packet to fan-out (relay to AI service)
// Notes: don't keep the reference of packet when the method return -> payload stay in buffer re-use from ReadLoop. If want to use it, copy/marshal
type RTPSink func(pkt *rtp.Packet)

// receive each raw RTP packet (pre-unmarshal bytes) to fan-out to the ffmpeg
// ingest bridge. Same aliasing rule as RTPSink: the slice is ReadLoop's reused
// read buffer, do not retain it past the call — copy if needed.
type RawSink func(raw []byte)

type FrameExtractor struct {
	track *webrtc.TrackRemote
	pc *webrtc.PeerConnection
	logger *zap.Logger

	mu sync.Mutex
	sps []byte	// latest sequence parameter set NAL
	pps []byte 	// latest picture parameter set NAL
	idrNALs [][]byte //NAL units of the latest complete IDR picture
	idrReady bool

	idrCh chan struct{}  // notify when an IDR frame buffered

	fua *fuaBuf // FU-A reassembly
	picBuf  [][]byte // NALs accumulating for the current RTP picture
	picTS   uint32   // RTP timestamp of the picture being assembled
	prevTS  uint32   // RTP timestamp of the previous committed picture
	hasFirstPic bool 

	prevSeq uint16 
	hasPrevSeq bool 
	lostPackets uint64 // number of lost packets

	pictureCorrupted bool
	lastRecoveryPLI time.Time
	reorder reorderBuffer

	sink NALSink // optional, called for every complete picture
	rtpSink RTPSink // optional, called for every RTP packet (fan-out relay)
	rawSink RawSink // optional, called with every raw RTP packet (ffmpeg ingest fan-out)
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


// register sink to receive every RTP packet (use for relaying fan-out to AI)
func (fe *FrameExtractor) SetRTPSink(sink RTPSink) {
	fe.rtpSink = sink
}

// register sink to receive every raw RTP packet (use for ffmpeg ingest
// fan-out — see peer.go's ffmpegIngestState). FrameExtractor is the sole
// reader of the underlying track; this lets a second consumer piggyback on
// that single read instead of racing it with its own track.Read/ReadRTP call.
func (fe *FrameExtractor) SetRawSink(sink RawSink) {
	fe.rawSink = sink
}

func (fe *FrameExtractor) RequestKeyFrame() {
	fe.mu.Lock()
	if time.Since(fe.lastRecoveryPLI) < minPLIInterval { 
		fe.mu.Unlock()
		return
	}
	fe.lastRecoveryPLI = time.Now()
	fe.mu.Unlock()

	if err := fe.pc.WriteRTCP([]rtcp.Packet{
		&rtcp.PictureLossIndication{
			MediaSSRC: uint32(fe.track.SSRC()),
		},
	}); err != nil {
		fe.logger.Warn("PLI send failed", zap.Error(err))
	}
}

func (fe *FrameExtractor) ReadLoop(ctx context.Context) {
	buf := make([]byte, 4096) // MTU-sized
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
		if fe.rawSink != nil {
			fe.rawSink(buf[:n]) // fan-out to ffmpeg ingest before unmarshal
		}
		var pkt rtp.Packet
		if err := pkt.Unmarshal(buf[:n]); err != nil {
			continue
		}
		if fe.rtpSink != nil {
			fe.rtpSink(&pkt) // fan-out relay: sink copy since the buf will be re-used
		}
		fe.ingest(&pkt)
	}
}

func (fe *FrameExtractor) checkSeqGap(pkt *rtp.Packet) bool {
	if !fe.hasPrevSeq {
		fe.prevSeq = pkt.SequenceNumber 
		fe.hasPrevSeq = true
		return false
	}

	delta := int32(int16(pkt.SequenceNumber - fe.prevSeq))

	switch {
	case delta == 1:
		fe.prevSeq = pkt.SequenceNumber
		return false
	case delta > 1: 
		lost := uint64(delta - 1)
		fe.lostPackets += lost
		fe.logger.Warn("RTP sequence gap detected (likely packet loss)", 
			zap.Uint16("seq", pkt.SequenceNumber), 
			zap.Int32("lostCount", delta-1),
			zap.Uint32("rtpTimestamp", pkt.Timestamp), 
			zap.Uint64("totalLost", fe.lostPackets),
		)
		fe.prevSeq = pkt.SequenceNumber
		return true
	default: 
		fe.logger.Warn("RTP packet out of order or duplicate", 
			zap.Uint16("seq", pkt.SequenceNumber), 
			zap.Int32("delta", delta), 
			zap.Uint32("rtpTimestamp", pkt.Timestamp),
		)
		return true
	}
}

// ingest is the ReadLoop entry point. The reorder buffer can retain pkt
// across multiple ReadLoop iterations while waiting for an earlier gap to
// fill (NACK retransmit), but pkt.Payload aliases ReadLoop's reused read
// buffer — clone it before it can be retained past this call.
func (fe *FrameExtractor) ingest(pkt *rtp.Packet) {
	cloned := *pkt
	cloned.Payload = append([]byte(nil), pkt.Payload...)
	for _, ordered := range fe.reorder.push(&cloned) {
		fe.ingestOrdered(ordered)
	}
}

func (fe *FrameExtractor) ingestOrdered(pkt *rtp.Packet) {
	if fe.checkSeqGap(pkt) && (len(fe.picBuf) > 0 || fe.fua != nil) {
		fe.pictureCorrupted = true
	}
	fe.picTS = pkt.Timestamp
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

// handle FU-A fragmentation units (RFC 6184 5.8)
func (fe *FrameExtractor) ingestFUA(p []byte, marker bool) {
	if len(p) < 2 {
		return
	}
	fuHeader := p[1]
	startBit := fuHeader&0x80 != 0
	endBit := fuHeader&0x40 != 0
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

	// A NAL is complete on the FU End bit — NOT the RTP marker (which ends the
	// whole frame). Using the marker drops every non-last slice of a multi-slice
	// picture. Commit the picture only when this is also the frame's last packet.
	if endBit {
		complete := fe.fua.data
		fe.fua = nil
		fe.addNAL(complete, marker)
	}
}


func (fe *FrameExtractor) commitPicture() {
	if len(fe.picBuf) == 0 {
		return
	}

	// derive duration from consecutive RTP timestamps (90kHz clock)
	// fallback to defaultFrameDur (30fps) when no prior timestamp or value looks wrong
	dur := defaultFrameDur
	if fe.hasFirstPic {
		if d := fe.picTS - fe.prevTS; d > 0 && d < 900000 { // sanity: < 10s at 90kHz
			dur = d
		}
	}
	fe.prevTS = fe.picTS
	fe.hasFirstPic = true

	if fe.pictureCorrupted {
		fe.pictureCorrupted = false
		fe.picBuf = nil 
		fe.logger.Warn("dropping picture reassembled from lost/out-of-order RTP packets", 
			zap.Uint32("rtpTimestamp", fe.picTS),
		)
		fe.RequestKeyFrame()
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
		fe.sink(fe.picBuf, hasIDR, dur)
	}

	// tier 1: only buffer IDR for keyframe capture
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


// use for displaying frame on browsers (browsers support JPEG rendering)
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

func H264ToJPEG(ctx context.Context, annexB []byte) ([]byte, error) {
	return h264ToJPEG(ctx, annexB)
}

// return Annex-B encoded H.264 keyframe (SPS + PPS + IDR)
// no need to spawn ffmpeg - decode the image using OpenCV
func (fe *FrameExtractor) CaptureKeyFrame() []byte {
	fe.mu.Lock()
	defer fe.mu.Unlock()

	if !fe.idrReady || len(fe.idrNALs) == 0 {
		return nil
	}

	var buf bytes.Buffer
	if len(fe.sps) > 0 {
		buf.Write(annexBStartCode)
		buf.Write(fe.sps)
	}
	if len(fe.pps) > 0 {
		buf.Write(annexBStartCode)
		buf.Write(fe.pps)
	}
	for _, nal := range fe.idrNALs {
		buf.Write(annexBStartCode)
		buf.Write(nal)
	}
	return buf.Bytes()
}
