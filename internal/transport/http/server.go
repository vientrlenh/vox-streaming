package http

import (
	"net/http"

	"github.com/vientrlenh/vox-streaming/internal/transport/segment"
	"github.com/vientrlenh/vox-streaming/internal/transport/webrtc"
)

func Register(mux *http.ServeMux, webrtcHandler *webrtc.Handler, segmentHandler *segment.SegmentHandler) {
	mux.HandleFunc("/ws/stream", webrtcHandler.ServeStream)
	mux.HandleFunc("/ws/monitor", webrtcHandler.ServeMonitor)
	mux.HandleFunc("/schedules/active", webrtcHandler.GetActiveSchedules)

	mux.HandleFunc("POST /stream/sessions", segmentHandler.CreateSession)
	mux.HandleFunc("PUT /stream/sessions/{streamId}/segments/{seq}", segmentHandler.Upload)
	mux.HandleFunc("POST /stream/sessions/{streamId}/complete", segmentHandler.Complete)
}
