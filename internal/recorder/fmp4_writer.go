package recorder

import (
	"bytes"
	"encoding/binary"
	"io"
)

type fMP4Writer struct {
	w io.Writer
	videoTrackID uint32
	audioTrackID uint32
	seqNr uint32
}

type videoFragment struct {
	videoSamples []fragSample
	audioSamples []fragSample
	videoDTSStart uint64
	audioDTSStart uint64
}

type fragSample struct {
	data []byte
	dur uint32
	isKey bool
}

const (
	videoTimescale = uint32(90000)
	audioTimescale = uint32(48000)
	defaultFrameDur = uint32(3000) // 90000/30fps = 33ms per frame
)

const OpusFrameSize = 960

func newFMP4Writer(w io.Writer) *fMP4Writer {
	return &fMP4Writer{
		w: w, 
		videoTrackID: 1, 
		audioTrackID: 2,
	}
}

func (fw *fMP4Writer) WriteInit(sps, pps []byte) error {
	out := append(fw.buildFTYP(), fw.buildMOOV(sps, pps)...)
	_, err := fw.w.Write(out)
	return err
}

func (fw *fMP4Writer) WriteFragment(vf videoFragment) error {
	fw.seqNr++
	var videoBytes, audioBytes int
	for _, s := range vf.videoSamples {
		videoBytes += len(s.data)
	}
	for _, s := range vf.audioSamples {
		audioBytes += len(s.data)
	}

	moofSize := fw.calcMoofSize(len(vf.videoSamples), len(vf.audioSamples))

	// data_offset is from start of moof to first byte of track data in mdat
	videoDataOffset := int32(moofSize + 8) // +8 = mdat box header
	audioDataOffset := int32(moofSize + 8 + videoBytes)

	moof := fw.buildMOOF(vf, videoDataOffset, audioDataOffset)

	var mdatBody []byte
	for _, s := range vf.videoSamples {
		mdatBody = append(mdatBody, s.data...)
	}
	for _, s := range vf.audioSamples {
		mdatBody = append(mdatBody, s.data...)
	}

	out := append(moof, buildBox("mdat", mdatBody)...)
	_, err := fw.w.Write(out)
	return err
}

// return the exact byte size of the moof box
//
// moof = 8(header) + 16(mfhd) + [8+16(tfhd)+16(tfdt)+(20+nx12)(trun)]
// = 8 + 16 + (60 + nx12) [+ (60 + mx12) if audio]
func (fw *fMP4Writer) calcMoofSize(nVideo, nAudio int) int {
	size := 84 + nVideo*12
	if nAudio > 0 {
		size += 60 + nAudio*12
	}
	return size
}

func (fw *fMP4Writer) buildFTYP() []byte {
	return buildBox("ftyp", concat(
		[]byte("isom"), be32(0), 
		[]byte("isom"), []byte("iso5"), []byte("dash"), []byte("avc1"),
	))
}

func (fw *fMP4Writer) buildMOOV(sps, pps []byte) []byte {
	return buildBox("moov", concat(
		fw.buildMVHD(), 
		fw.buildVideoTRAK(sps, pps), 
		fw.buildAudioTRAK(), 
		fw.buildMVEX(),
	))
}

func (fw *fMP4Writer) buildMVHD() []byte {
	return buildBox("mvhd", concat(
		be32(0), // version=0, flags=0
		be32(0), be32(0), // creation, modification
		be32(90000), // timescale
		be32(0), // duration = 0 (unknown)
		be32(0x00010000), // rate = 1.0
		be16(0x0100), // volume = 1.0
		make([]byte, 10), // reserved
		// unity matrix
		be32(0x00010000), be32(0), be32(0), 
		be32(0), be32(0x00010000), be32(0), 
		be32(0), be32(0), be32(0x40000000),
		make([]byte, 24), // pre_defined
		be32(fw.audioTrackID+1), // next_track_ID
	))
}

func (fw *fMP4Writer) buildVideoTRAK(sps, pps []byte) []byte {
	tkhd := buildBox("tkhd", concat(
		be32(0x00000003), // flags: enabled + in-movie
		be32(0), be32(0), // creation, modification
		be32(fw.videoTrackID), 
		be32(0), be32(0), make([]byte, 8),
		be16(0), be16(0), be16(0), be16(0), // layer, alt-group, volume=0, reserved
		be32(0x00010000), be32(0), be32(0), 
		be32(0), be32(0x00010000), be32(0),
		be32(0), be32(0), be32(0x40000000), 
		be32(0), be32(0), // width, height (from SPS)
	))
	mdhd := buildBox("mdhd", concat(
		be32(0), be32(0), be32(0), 
		be32(videoTimescale), be32(0), 
		be16(0x55C4), be16(0), // language='und'
	))
	hdlr := buildBox("hdlr", concat(
		be32(0), be32(0), []byte("vide"), make([]byte, 12), 
		[]byte("VideoHandler\x00"),
	))
	avcC := buildBox("avcC", buildAVCDecoderConfig(sps, pps))
	avc1 := buildBox("avc1", concat(
		make([]byte, 6), be16(1), // reserved + data_reference_index
		make([]byte, 16), // pre_defined + reserved
		be16(0), be16(0), // width, height
		be32(0x00480000), be32(0x00480000), // 72dpi
		be32(0), be16(1), // reserved, frame_count
		make([]byte, 32), // compressorname
		be16(0x0018), be16(0xFFFF), // depth, pre_defined
		avcC,
	))
	stsd := buildBox("stsd", concat(be32(0), be32(1), avc1))
	stbl := buildBox("stbl", concat(
		stsd, 
		buildBox("stts", concat(be32(0), be32(0))), 
		buildBox("stsc", concat(be32(0), be32(0))), 
		buildBox("stsz", concat(be32(0), be32(0), be32(0))),
		buildBox("stco", concat(be32(0), be32(0))),
	))
	vmhd := buildBox("vmhd", concat(be32(1), make([]byte, 8)))
	dref := buildBox("dref", concat(be32(0), be32(1), buildBox("url ", be32(1))))
	minf := buildBox("minf", concat(vmhd, buildBox("dinf", dref), stbl))
	return buildBox("trak", concat(tkhd, buildBox("mdia", concat(mdhd, hdlr, minf))))
}

func (fw *fMP4Writer) buildAudioTRAK() []byte {
	tkhd := buildBox("tkhd", concat(
		be32(0x00000003), 
		be32(0), be32(0), 
		be32(fw.audioTrackID), 
		be32(0), be32(0), make([]byte, 8), 
		be16(0), be16(0), be16(0x0100), be16(0), // volume = 1.0
		be32(0x00010000), be32(0), be32(0), 
		be32(0), be32(0x00010000), be32(0), 
		be32(0), be32(0), be32(0x40000000),
		be32(0), be32(0),
	))

	mdhd := buildBox("mdhd", concat(
		be32(0), be32(0), be32(0), 
		be32(audioTimescale), be32(0), 
		be16(0x55C4), be16(0),
	))

	hdlr := buildBox("hdlr", concat(
		be32(0), be32(0), []byte("soun"), make([]byte, 12), 
		[]byte("SoundHandler\x00"),
	))
	
	//Opus sample entry + dOps box (RFC 7845 / ISOBMFF Opus)
	dOps := buildBox("dOps", []byte{
		0, 2, // Version=0, OutputChannelCount=2
		0, 0, // PreSkip=0
		0, 0, 0xBB, 0x80, // InputSampleRate=48000
		0, 0, // OutputGain=0
		0, // ChannelMappingFamily=0 (stereo)
	}) 
	opusEntry := buildBox("Opus", concat(
		make([]byte, 6), be16(1),  // reserved + data_reference_index
		make([]byte, 8), // reserved
		be16(2), // channelcount = 2
		be16(16), // samplesize=16
		be16(0), be16(0), // pre_defined, reserved
		be32(0xBB800000), // samplerate=48000 in 16.16 fixed-point
		dOps,
	))
	stsd := buildBox("stsd", concat(be32(0), be32(1), opusEntry))
	stbl := buildBox("stbl", concat(
		stsd, 
		buildBox("stts", concat(be32(0), be32(0))), 
		buildBox("stsc", concat(be32(0), be32(0))), 
		buildBox("stsz", concat(be32(0), be32(0), be32(0))), 
		buildBox("stco", concat(be32(0), be32(0))),
	))
	smhd := buildBox("smhd", concat(be32(0), be16(0), be16(0)))
	dref := buildBox("dref", concat(be32(0), be32(1), buildBox("url ", be32(1))))
	minf := buildBox("minf", concat(smhd, buildBox("dinf", dref), stbl))
	return buildBox("trak", concat(tkhd, buildBox("mdia", concat(mdhd, hdlr, minf))))
}

func (fw *fMP4Writer) buildMVEX() []byte {
	trexVideo := buildBox("trex", concat(
		be32(0), be32(fw.videoTrackID), 
		be32(1), be32(0), be32(0), be32(0),
	))
	trexAudio := buildBox("trex", concat(
		be32(0), be32(fw.audioTrackID), 
		be32(1), be32(0), be32(0), be32(0),
	))
	return buildBox("mvex", concat(trexVideo, trexAudio))
}

func (fw *fMP4Writer) buildMOOF(vf videoFragment, videoDataOffset, audioDataOffset int32) []byte {
	mfhd := buildBox("mfhd", concat(be32(0), be32(fw.seqNr)))

	videoTRAF := buildBox("traf", concat(
		buildBox("tfhd", concat(be32(0x00020000), be32(fw.videoTrackID))),
		buildBox("tfdt", concat(be32(0), be32(uint32(vf.videoDTSStart)))),
		fw.buildTRUN(vf.videoSamples, videoDataOffset),
	))

	parts := [][]byte{mfhd, videoTRAF}

	if len(vf.audioSamples) > 0 {
		audioTRAF := buildBox("traf", concat(
			buildBox("tfhd", concat(be32(0x00020000), be32(fw.audioTrackID))), 
			buildBox("tfdt", concat(be32(0), be32(uint32(vf.audioDTSStart)))), 
			fw.buildTRUN(vf.audioSamples, audioDataOffset),
		))
		parts = append(parts, audioTRAF)
	}
	return buildBox("moof", concat(parts...))
}

func (fw *fMP4Writer) buildTRUN(samples []fragSample, dataOffset int32) []byte {
	// flags: data-offset(0x0001) + sample-duration(0x0100) + sample-size(0x0200) + sample-flag(0x0400)
	const trunFlags = uint32(0x00000701)
	body := concat(be32(trunFlags), be32(uint32(len(samples))))

	var dob [4]byte
	binary.BigEndian.PutUint32(dob[:], uint32(dataOffset))
	body = append(body, dob[:]...)

	for _, s := range samples {
		sampleFlags := uint32(0x00000000) // sync sample (keyframe)
		if !s.isKey {
			sampleFlags = 0x00010000
		}
		body = append(body, be32(s.dur)...)
		body = append(body, be32(uint32(len(s.data)))...)
		body = append(body, be32(sampleFlags)...)
	}
	return buildBox("trun", body)
}


// convert separated NAL units to AVCC format (4-byte length prefix per NAL)
// Filter out SPS (type 7) and PPS (type 8) - they live in avcC, not in samples
func nalsToAVCC(nals [][]byte) []byte {
	var buf bytes.Buffer
	for _, nal := range nals {
		if len(nal) == 0 {
			continue
		}
		nalType := nal[0] & 0x1F
		if nalType == 7 || nalType == 8 { // SPS, PPS already in avcC
			continue
		}
		var sz [4]byte
		binary.BigEndian.PutUint32(sz[:], uint32(len(nal)))
		buf.Write(sz[:])
		buf.Write(nal)
	}
	return buf.Bytes()
}


func buildAVCDecoderConfig(sps, pps []byte) []byte {
	if len(sps) < 4 {
		return nil
	}
	b := []byte{
		1, // configurationVersion
		sps[1], sps[2], sps[3], // profile, constraints, level
		0xFF, // lengthSizeMinusOne = 3 -> 4-byte AVCC lengths
		0xE1, // numSPS = 1
		byte(len(sps) >> 8), byte(len(sps)),
	}
	b = append(b, sps...)
	b = append(b, 1, byte(len(pps)>>8), byte(len(pps)))
	b = append(b, pps...)
	return b
}


func buildBox(boxType string, body []byte) []byte {
	out := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(out[0:], uint32(8+len(body)))
	copy(out[4:], boxType)
	copy(out[8:], body)
	return out
}

func be32(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func be16(v uint16) []byte {
	b := make([]byte, 2)
	binary.BigEndian.PutUint16(b, v)
	return b
}

func concat(parts ...[]byte) []byte {
	var n int
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}