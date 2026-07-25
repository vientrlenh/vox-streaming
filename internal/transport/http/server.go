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
	// The literal "playlist.m3u8" segment is more specific than the {asset}
	// wildcard below, so ServeMux routes the manifest to its own handler even
	// though both patterns match it (Go 1.22+ precedence rules).
	mux.HandleFunc("GET /live/{scheduleId}/{streamId}/playlist.m3u8", webrtcHandler.GetLiveManifest)
	mux.HandleFunc("GET /live/{scheduleId}/{streamId}/{asset}", webrtcHandler.GetLiveAsset)

	mux.HandleFunc("POST /stream/sessions", segmentHandler.CreateSession)
	mux.HandleFunc("PUT /stream/sessions/{streamId}/segments/{seq}", segmentHandler.Upload)
	mux.HandleFunc("POST /stream/sessions/{streamId}/complete", segmentHandler.Complete)
	mux.HandleFunc("GET /stream/sessions/{streamId}/audit", segmentHandler.Audit)
	mux.HandleFunc("POST /internal/recordings/playback", recordHandler.CreatePlaybackURL)
}
