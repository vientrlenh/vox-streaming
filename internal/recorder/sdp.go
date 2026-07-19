package recorder

import "fmt"

type negotiatedCodec struct {
	payloadType uint8
	clockRate   uint32
	channels    uint16 // audio only
	fmtp        string // video only
}


func buildSDP(video, audio negotiatedCodec, ports allocatedPorts) string {
	videoFMTP := ""
	if video.fmtp != "" {
		videoFMTP = fmt.Sprintf("a=fmtp:%d %s\r\n", video.payloadType, video.fmtp)
	}
	channels := audio.channels
	if channels == 0 {
		channels = 2
	}
	return fmt.Sprintf(
		"v=0\r\n"+
			"o=- 0 0 IN IP4 127.0.0.1\r\n"+
			"s=vox-ffmpeg-ingest\r\n"+
			"c=IN IP4 127.0.0.1\r\n"+
			"t=0 0\r\n"+
			"m=video %d RTP/AVP %d\r\n"+
			"a=rtcp:%d IN IP4 127.0.0.1\r\n"+
			"a=rtpmap:%d H264/%d\r\n"+
			"%s"+
			"a=recvonly\r\n"+
			"m=audio %d RTP/AVP %d\r\n"+
			"a=rtcp:%d IN IP4 127.0.0.1\r\n"+
			"a=rtpmap:%d opus/%d/%d\r\n"+
			"a=recvonly\r\n",
		ports.videoRTP, video.payloadType,
		ports.videoRTCP,
		video.payloadType, video.clockRate,
		videoFMTP,
		ports.audioRTP, audio.payloadType,
		ports.audioRTCP,
		audio.payloadType, audio.clockRate, channels,
	)
}
