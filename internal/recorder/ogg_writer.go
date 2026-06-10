package recorder

import (
	"encoding/binary"
	"math/rand"
	"os"
)

const (
	OpusPreSkip    = 312 // standard pre-skip for WebRTC Opus
	OpusSampleRate = 48000
	OpusFrameSize  = 960
)

type OGGWriter struct {
	f *os.File
	serialNo uint32
	pageSeq uint32
	granulePos uint64
	closed bool
}

func NewOGGWriter(f *os.File, channels uint8) (*OGGWriter, error) {
	ow := &OGGWriter{
		f: f, 
		serialNo: rand.Uint32(),
	}
	if err := ow.writeOpusHead(channels); err != nil {
		return nil, err
	}
	return ow, ow.writeOpusTags()
}

// write a single Opus RTP payload
// sampleCount is typically 960 (20ms at 48kHz)
func (ow *OGGWriter) WritePacket(payload []byte, sampleCount uint32) error {
	if ow.closed {
		return nil
	}
	ow.granulePos += uint64(sampleCount)
	return ow.writePage(payload, ow.granulePos, 0x00)
}

func (ow *OGGWriter) Close() error {
	if ow.closed {
		return nil
	}
	ow.closed = true
	return ow.writePage(nil, ow.granulePos, 0x04) // EOS
}

func (ow *OGGWriter) writeOpusHead(channels uint8) error {
	var buf [19]byte
	copy(buf[:], "OpusHead")
	buf[8] = 1
	buf[9] = channels
	binary.LittleEndian.PutUint16(buf[10:], OpusPreSkip)
	binary.LittleEndian.PutUint32(buf[12:], uint32(OpusSampleRate))

	// output_gain=0, channel_mapping_family=0 already zero
	return ow.writePage(buf[:], 0, 0x02) // BOS
}

func (ow *OGGWriter) writeOpusTags() error {
	const vendor = "vox-streaming"
	buf := make([]byte, 8+4+len(vendor)+4)
	copy(buf, "OpusTags")
	binary.LittleEndian.PutUint32(buf[8:], uint32(len(vendor)))
	copy(buf[12:], vendor)

	// user_comment_list_length = 0 already 0
	return ow.writePage(buf, 0, 0x00)
}

func (ow *OGGWriter) writePage(data []byte, granulePos uint64, headerType byte) error {
	segs := lacingValues(data)

	pageSize := 27 + len(segs)
	for _, s := range segs {
		pageSize += s
	}

	page := make([]byte, pageSize)
	copy(page[0:], "OggS")
	page[4] = 0
	page[5] = headerType
	binary.LittleEndian.PutUint64(page[6:], granulePos)
	binary.LittleEndian.PutUint32(page[14:], ow.serialNo)
	binary.LittleEndian.PutUint32(page[18:], ow.pageSeq)
	ow.pageSeq++

	// page[22:26] = CRC filled below
	page[26] = byte(len(segs))

	off := 27
	for _, s := range segs {
		page[off] = byte(s)
		off++
	}

	d := data
	for _, s := range segs {
		if s > 0 {
			copy(page[off:], d[:s])
			d = d[s:]
			off += s
		}
	}

	binary.LittleEndian.PutUint32(page[22:], oggCRC(page))
	_, err := ow.f.Write(page)
	return err
}

// build OGG segment sizes for one packet (RFC 3533 6.1)
func lacingValues(data []byte) []int {
	if len(data) == 0 {
		return []int{0}
	}
	var segs []int
	for len(data) > 0 {
		n := min(len(data), 255)
		segs = append(segs, n)
		data = data[n:]
	}
	
	// if last segment is exactly 255, append 0-byte segment to mark packet end
	if segs[len(segs)-1] == 255 {
		segs = append(segs, 0)
	}
	return segs
}

// OGG CRC32: polynomial 0x04c11db7, non-reflected but it is not the standard Go crc32.IEEE
var oggCRCTable = func() [256]uint32 {
	var t [256]uint32
	for i := range t {
		crc := uint32(i) << 24
		for j := 0; j < 8; j++ {
			if crc&0x80000000 != 0 {
				crc = (crc << 1) ^ 0x04c11db7
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return t
}()

func oggCRC(data []byte) uint32 {
	crc := uint32(0)
	for _, b := range data {
		crc = (crc << 8) ^ oggCRCTable[byte(crc>>24)^b]
	}
	return crc
}