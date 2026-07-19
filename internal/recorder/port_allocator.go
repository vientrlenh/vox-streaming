package recorder

import (
	"fmt"
	"sync"
)

// portsPerStream is video RTP, video RTCP, audio RTP, audio RTCP.
const portsPerStream = 4

type allocatedPorts struct {
	videoRTP  int
	videoRTCP int
	audioRTP  int
	audioRTCP int
}


type PortAllocator struct {
	mu   sync.Mutex
	free []int // free group start ports, each group is portsPerStream wide
}

func NewPortAllocator(rangeStart, rangeEnd int) (*PortAllocator, error) {
	if rangeEnd <= rangeStart {
		return nil, fmt.Errorf("invalid ffmpeg ingest port range %d-%d", rangeStart, rangeEnd)
	}
	a := &PortAllocator{}
	for p := rangeStart; p+portsPerStream-1 <= rangeEnd; p += portsPerStream {
		a.free = append(a.free, p)
	}
	if len(a.free) == 0 {
		return nil, fmt.Errorf("ffmpeg ingest port range %d-%d too small for %d ports/stream", rangeStart, rangeEnd, portsPerStream)
	}
	return a, nil
}

func (a *PortAllocator) Allocate() (allocatedPorts, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.free) == 0 {
		return allocatedPorts{}, fmt.Errorf("no free ffmpeg ingest ports available")
	}
	base := a.free[len(a.free)-1]
	a.free = a.free[:len(a.free)-1]
	return allocatedPorts{
		videoRTP:  base,
		videoRTCP: base + 1,
		audioRTP:  base + 2,
		audioRTCP: base + 3,
	}, nil
}

func (a *PortAllocator) Release(p allocatedPorts) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.free = append(a.free, p.videoRTP)
}
