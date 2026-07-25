// Backend endpoints for the demo client. Loaded (as a classic script) before the
// page modules, so window.VOX is set before any request is made.
//
// ── Proxy mode (DEFAULT) ────────────────────────────────────────────────────
// Served behind the nginx `web` container at http://localhost:5173, which
// reverse-proxies /token, /ws/*, /schedules/*, /live/* to the backends. Leave the bases empty
// to use same-origin relative URLs. Nothing to change.
//
// ── Direct / standalone mode ────────────────────────────────────────────────
// Serving web/ yourself with a plain static server (no nginx)? Point the bases
// straight at the host-run services and uncomment:
//
//   window.VOX = {
//     tokenBase:      "http://localhost:8090", // devserver HTTP (mint JWT)
//     serverHttpBase: "http://localhost:8082", // streaming server HTTP
//     serverWsBase:   "ws://localhost:8082",   // streaming server WebSocket
//   };
//
// IMPORTANT (direct mode): serve the pages from http://localhost:5173 so the
// browser's Origin matches the server's ALLOWED_ORIGIN. If you use a different
// port, set ALLOWED_ORIGIN to that exact origin when you start the service.

// Direct / standalone mode — serving web/ with a plain static server (python,
// npx serve, ...) instead of the nginx proxy. Point straight at the host-run
// services. (These absolute localhost URLs also work behind the nginx setup,
// since devserver:8090 and the host server:8082 are reachable from the browser.)
// For pure proxy mode, set all three back to "".
window.VOX = {
  tokenBase: "http://localhost:8090",   // devserver HTTP (mint JWT)
  serverHttpBase: "http://localhost:8082", // streaming server HTTP
  serverWsBase: "ws://localhost:8082",  // streaming server WebSocket
};
