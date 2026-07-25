// Shared helpers for the demo pages.
//
// Endpoint bases come from window.VOX (see config.js). Two modes:
//  - Proxy mode (bases empty): served behind nginx at http://localhost:5173,
//    which reverse-proxies /token, /ws/*, /schedules/*, /live/* to the right
//    backends. All requests are same-origin — no CORS, and the WebSocket
//    Origin check passes.
//  - Direct/standalone mode: web/ served by any static server, bases point
//    straight at the host-run services (see config.js for values).
const CFG = (typeof window !== "undefined" && window.VOX) || {};

// Fetch a freshly-minted JWT from the dev token endpoint.
export async function mintToken({ role, scheduleId, userId, sessionId, streamTypes }) {
  const params = new URLSearchParams({ role, scheduleId });
  if (userId) params.set("userId", userId);
  if (sessionId) params.set("sessionId", sessionId);
  if (streamTypes) params.set("streamTypes", streamTypes);
  const base = CFG.tokenBase || ""; // "" -> same-origin (proxy mode)
  const res = await fetch(`${base}/token?${params.toString()}`);
  if (!res.ok) throw new Error(`token endpoint ${res.status}: ${await res.text()}`);
  return res.json();
}

// Build a ws:// (or wss://) URL for a server path.
export function wsURL(path, query) {
  const qs = new URLSearchParams(query).toString();
  if (CFG.serverWsBase) return `${CFG.serverWsBase}${path}?${qs}`; // direct mode
  const scheme = location.protocol === "https:" ? "wss" : "ws";     // proxy mode
  return `${scheme}://${location.host}${path}?${qs}`;
}

// Build an http:// (or https://) URL for a plain HTTP server path (e.g. the
// /live/*/playlist.m3u8 manifest — everything else about it, incl. the
// presigned segment URLs it embeds, is plain HTTP fetches, not a WebSocket).
export function httpURL(path, query) {
  const qs = new URLSearchParams(query).toString();
  const base = CFG.serverHttpBase || ""; // "" -> same-origin (proxy mode)
  return `${base}${path}${qs ? `?${qs}` : ""}`;
}

// Tiny append-only logger bound to a <div class="log"> element.
export function makeLogger(el) {
  const line = (cls, msg) => {
    const t = new Date().toLocaleTimeString();
    const row = document.createElement("div");
    row.innerHTML = `<span class="t">${t}</span> <span class="${cls}">${escapeHtml(msg)}</span>`;
    el.appendChild(row);
    el.scrollTop = el.scrollHeight;
  };
  return {
    info: (m) => line("", m),
    ok: (m) => line("ok", m),
    warn: (m) => line("warn", m),
    err: (m) => line("err", m),
  };
}

export function setStatus(dotEl, textEl, state, text) {
  dotEl.className = `dot ${state}`; // "", ok, warn, err
  if (textEl) textEl.textContent = text;
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
