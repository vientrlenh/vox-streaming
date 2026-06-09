package http

import (
	"net/http"

	"github.com/vientrlenh/vox-streaming/internal/transport/webrtc"
)


func Register(mux *http.ServeMux, webrtcHandler *webrtc.Handler) {
	mux.HandleFunc("/ws/stream", webrtcHandler.ServeStream)
	mux.HandleFunc("/ws/monitor", webrtcHandler.ServeMonitor)
	mux.HandleFunc("/rooms/active", webrtcHandler.GetActiveRooms)
}

