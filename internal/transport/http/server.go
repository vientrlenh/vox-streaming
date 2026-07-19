package http

import (
	"net/http"

	"github.com/vientrlenh/vox-streaming/internal/transport/segment"
	"github.com/vientrlenh/vox-streaming/internal/transport/webrtc"
)


func Register(mux *http.ServeMux, webrtcHandler *webrtc.Handler, segmentHandler *segment.SegmentHandler) {
	mux.HandleFunc("/ws/stream", webrtcHandler.ServeStream)
	mux.HandleFunc("/ws/monitor", webrtcHandler.ServeMonitor)
	mux.HandleFunc("/rooms/active", webrtcHandler.GetActiveRooms)

	mux.HandleFunc("POST /stream/segment", segmentHandler.Upload)
	mux.HandleFunc("POST /stream/segment/complete", segmentHandler.Complete)
}

