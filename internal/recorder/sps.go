package recorder

func parseSPSDimensions(sps []byte) (width, height uint32, ok bool) {
	if len(sps) < 4 {
		return 0, 0, false
	}
	br := &bitReader{
		data: unescapeEPB(sps[1:]),
	}
	profileIdc := br.u(8)
	br.u(8) // constraint flags + reserved
	br.u(8) // level_idc
	br.ue() // seq_parameter_set_id

	switch profileIdc {
	case 100, 110, 122, 244, 44, 83, 86, 118, 128, 138, 139, 134: 
		chromaFormatIdc := br.ue()
		if chromaFormatIdc == 3 {
			br.u(1) // separate_colour_plane_flag
		}
		br.ue() // bit_depth_luma_minus8
		br.ue() // bit_depth_chroma_minus8
		br.u(1) // qpprime_y_zero_transform_bypass_flag
		if br.u(1) == 1 { // seq_scaling_matrix_present_flag
			count := 8
			if chromaFormatIdc == 3 {
				count = 12
			}
			for i := 0; i < count; i++ {
				if br.u(1) == 1 { // scaling_list_present_flag
					size := 16
					if i >= 6 {
						size = 64
					}
					br.skipScalingList(size)
				}
			}
		}
	}

	br.ue() // log2_max_frame_num_minus4
	picOrderCntType := br.ue()
	switch picOrderCntType {
	case 0:
		br.ue() // log2_max_pic_order_cnt_lsb_minus4
	case 1:
		br.u(1) // delta_pic_order_always_zero_flag
		br.se() // offset_for_non_ref_pic
		br.se() // offset_for_top_to_botton_field
		for n := br.ue(); n > 0; n-- {
			br.se() // offset_for_ref_frame[i]
		}
	}
	br.ue() // max_num_ref_frames
	br.u(1) // gaps_in_frame_num_value_allowed_flag
	picWidthInMbsMinus1 := br.ue()
	picHeightInMapUnitsMinus1 := br.ue()
	frameMbsOnlyFlag := br.u(1)
	if frameMbsOnlyFlag == 0 {
		br.u(1) // mb_adaptive_frame_field_flag
	}
	br.u(1) // direct_8x8_inference_flag

	var cropLeft, cropRight, cropTop, cropBottom uint32
	if br.u(1) == 1 { // frame_cropping_flag
		cropLeft = br.ue()
		cropRight = br.ue()
		cropTop = br.ue()
		cropBottom = br.ue()
	}	
	if br.err {
		return 0, 0, false
	}

	frameHeightInMbs := (2 - frameMbsOnlyFlag) * (picHeightInMapUnitsMinus1 + 1)
	width = (picWidthInMbsMinus1 + 1) * 16
	height = frameHeightInMbs * 16

	// 4:2:0 chroma crop units (only handles yuv420p)
	cropUnitX := uint32(2)
	cropUnitY := uint32(2) * (2 - frameMbsOnlyFlag)
	width -= (cropLeft + cropRight) * cropUnitX
	height -= (cropTop + cropBottom) * cropUnitY

	if width == 0 || height == 0 {
		return 0, 0, false
	}
	return width, height, true
}

type bitReader struct {
	data []byte 
	pos int
	err bool
}


func (r *bitReader) u(n int) uint32 {
	var v uint32 
	for range n {
		v <<= 1
		bytePos := r.pos / 8
		if bytePos >= len(r.data) {
			r.err = true
			return v
		}
		bit := (r.data[bytePos] >> (7 - uint(r.pos%8))) & 1
		v |= uint32(bit)
		r.pos++
	}
	return v
}

func (r *bitReader) ue() uint32 {
	leadingZeros := 0
	for r.u(1) == 0 {
		if r.err || leadingZeros >= 32 {
			r.err = true
			return 0
		}
		leadingZeros++
	}
	if leadingZeros == 0 {
		return 0
	}
	return (1 << uint(leadingZeros)) - 1 + r.u(leadingZeros)
}

func (r *bitReader) se() int32 {
	code := r.ue()
	if code%2 == 0 {
		return -int32(code / 2)
	}
	return int32((code + 1) / 2)
}

func (r *bitReader) skipScalingList(size int) {
	lastScale, nextScale := int32(8), int32(8)
	for j := 0; j < size; j++ {
		if nextScale != 0 {
			nextScale = (lastScale + r.se() + 256) % 256
		}
		if nextScale != 0 {
			lastScale = nextScale
		}
	}
}

// removes H.264 emulation-prevention bytes (the 0x03 inserted after any 0x00 in RBSP) so exp-golomb parsing lines up with the raw syntax elements
func unescapeEPB(data []byte) []byte {
	out := make([]byte, 0, len(data))
	zeroRun := 0
	for _, b := range data {
		if zeroRun >= 2 && b == 0x03 {
			zeroRun = 0
			continue
		}
		if b == 0 {
			zeroRun++
		} else {
			zeroRun = 0
		}
		out = append(out, b)
	}
	return out
}

