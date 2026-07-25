package http

import (
	"github.com/vientrlenh/vox-streaming/internal/handler/recording"
	"net/http"

	"github.com/vientrlenh/vox-streaming/internal/handler/segment"
	"github.com/vientrlenh/vox-streaming/internal/transport/webrtc"
)

func Register(mux *http.ServeMux, webrtcHandler *webrtc.Handler, segmentHandler *segment.SegmentHandler, recordHandler *recording.RecordHandler) {
	mux.HandleFunc("/ws/stream", webrtcHandler.ServeStream)
	mux.HandleFunc("/ws/monitor", webrtcHandler.ServeMonitor)
	mux.HandleFunc("/schedules/active", webrtcHandler.GetActiveSchedules)

	mux.HandleFunc("POST /stream/sessions", segmentHandler.CreateSession)
	mux.HandleFunc("PUT /stream/sessions/{streamId}/segments/{seq}", segmentHandler.Upload)
	mux.HandleFunc("POST /stream/sessions/{streamId}/complete", segmentHandler.Complete)
	mux.HandleFunc("POST /internal/recordings/playback", recordHandler.CreatePlaybackURL)
}
