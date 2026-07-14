package ffmpegingest

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
)

// relays RTCP Sender Reports from receiver to a local UDP
// destination. Forwarding the SR is required for A/V sync — validated in
// scripts/rtcp-av-sync-test (without it, audio/video drift apart over time).
func forwardRTCP(ctx context.Context, receiver *webrtc.RTPReceiver, port int) error {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return err
	}
	defer conn.Close()
	dest := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port}

	for {
		if err := receiver.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
			return err
		}
		packets, _, err := receiver.ReadRTCP()
		if err != nil {
			if isTimeout(err) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					continue
				}
			}
			return err
		}
		hasSR := false
		for _, p := range packets {
			if _, ok := p.(*rtcp.SenderReport); ok {
				hasSR = true
				break
			}
		}
		if !hasSR {
			continue
		}
		raw, err := rtcp.Marshal(packets)
		if err != nil {
			continue
		}
		if _, err := conn.WriteToUDP(raw, dest); err != nil {
			return err
		}
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
