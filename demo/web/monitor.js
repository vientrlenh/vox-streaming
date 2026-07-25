import { mintToken, wsURL, httpURL, makeLogger, setStatus } from "./common.js";

const $ = (id) => document.getElementById(id);
const dot = $("dot");
const stateText = $("stateText");
const framesEl = $("frames");
const eventsEl = $("events");
const alertsEl = $("alerts");
const studentListEl = $("studentList");
const playerStatusEl = $("playerStatus");
const videoEl = $("player");
const seekBarEl = $("seekBar");
const seekRangeEl = $("seekRange");
const seekInfoEl = $("seekInfo");
const liveBtnEl = $("liveBtn");

// Safety margin (seconds) kept off the reported DVR end when clamping a seek target. `end` is
// the position immediately AFTER the last known sample, not itself a safely decodable point --
// seeking exactly there (or past it) can make hls.js reload the last fragment, snap back to the
// keyframe before it, or jump to the live sync position, which looks from the outside like the
// last few seconds repeating.
const SEEK_END_MARGIN_SECS = 0.25;

// Monitor JWT TTL is 5 min server-side (IssueMonitorTokenUseCase.MONITOR_TOKEN_TTL) -- fine for
// /ws/monitor (validated once at handshake, then the socket just stays open), but the live-rewind
// manifest is polled repeatedly over HTTP for as long as a stream is selected, and every poll
// re-validates the token. Refresh with margin well before the 5-minute TTL so watching a stream
// longer than that doesn't silently start 401ing.
const TOKEN_REFRESH_INTERVAL_MS = 4 * 60 * 1000;

let ws = null;
let hls = null; // current hls.js instance, if any (native Safari playback uses videoEl.src directly instead)
let currentScheduleId = null;
let currentUserId = null;
let currentToken = null;
let selectedStreamId = null;
let usingNativeHls = false;
let tokenRefreshTimer = null;
let seekBarTimer = null;
let seekDragging = false;
// {start, end} in videoEl.currentTime's coordinate space, from hls.js's own parsed playlist
// (Hls.Events.LEVEL_UPDATED) -- NOT from videoEl.buffered, which only reflects what the browser
// has actually downloaded so far. A teacher opening the monitor minutes into a stream would only
// have the last few seconds buffered even though the manifest (and S3) still has every older
// fragment, so buffered alone underreports the real DVR window and can't be dragged into. Native
// Safari (no hls.js) has no such event, so that path falls back to videoEl.seekable, which the
// platform's own AVFoundation engine manages.
let dvrRange = null;
// Maps videoEl.currentTime onto absolute wall-clock time: {mediaTime, dateMs} means "media
// position mediaTime was captured at dateMs". Built from #EXT-X-PROGRAM-DATE-TIME (hls.js exposes
// it as fragment.programDateTime; Safari as videoEl.getStartDate()).
//
// This is what makes the scrub bar survive the player being torn down. currentTime alone is
// meaningless across player instances: with no absolute anchor, hls.js maps whichever fragment
// happens to be first when it attaches to media time 0, so stopping and resuming silently
// re-based the whole timeline and the stream appeared to restart from 0:00. With the anchor, the
// bar is always drawn over [stream start .. now] in real time and only the *window inside it*
// that is actually seekable changes.
let wallAnchor = null;
// The absolute time domain, in ms, the range input is currently drawn over: {startMs, endMs}
// (stream start .. now) plus the seekable sub-range inside it. Frozen for the duration of a drag
// so the thumb's meaning can't shift under the user's finger while the live edge keeps advancing.
let seekDomain = null;
// false the moment the user drags the scrub bar; true again only when they press the Live button.
// Gates two things: whether the periodic Safari token-refresh is allowed to reset videoEl.src
// (which jumps to the live edge -- fine while following live, a jarring surprise mid-rewind), and
// the seek-info readout ("đang xem trực tiếp" vs "đang xem lại").
let isFollowingLiveEdge = true;

const tiles = new Map(); // streamId -> frame-tile DOM refs
const activeStreams = new Map(); // streamId -> { sessionId, participantId, streamType, startedAt }

$("connectBtn").addEventListener("click", connect);
$("disconnectBtn").addEventListener("click", () => disconnect("stopped by user"));
$("stopWatchBtn").addEventListener("click", stopWatching);
seekRangeEl.addEventListener("input", () => {
  seekDragging = true;
  isFollowingLiveEdge = false;
});
seekRangeEl.addEventListener("change", () => {
  seekToOffset(parseFloat(seekRangeEl.value));
  seekDragging = false;
});
liveBtnEl.addEventListener("click", () => {
  isFollowingLiveEdge = true;
  if (dvrRange) {
    videoEl.currentTime = Math.max(dvrRange.start, dvrRange.end - SEEK_END_MARGIN_SECS);
  }
});

// Seeks to `offsetSecs` seconds after the stream started (the slider's own unit). Goes through
// wall-clock time rather than treating the offset as a media position: the two only coincide when
// the player attached at the very start of the stream.
function seekToOffset(offsetSecs) {
  if (!dvrRange || !seekDomain) return;
  // Keyed off the domain the bar is actually drawn over, not off wallAnchor alone: the absolute
  // domain also needs a known stream start, and reading the offset in the wrong unit would seek
  // somewhere unrelated.
  let target;
  if (seekDomain.absolute) {
    target = dateToMedia(seekDomain.startMs + offsetSecs * 1000);
  } else {
    // Raw media time (no PROGRAM-DATE-TIME, or the stream's start is unknown), so the slider
    // offset already is a media position.
    target = offsetSecs;
  }
  // Clamp inside the seekable window rather than trusting the raw value: the slider spans the
  // whole elapsed stream, of which only dvrRange is actually fetchable, and its far end is the
  // position immediately AFTER the last known sample -- see SEEK_END_MARGIN_SECS.
  const safeEnd = Math.max(dvrRange.start, dvrRange.end - SEEK_END_MARGIN_SECS);
  videoEl.currentTime = Math.min(safeEnd, Math.max(dvrRange.start + 0.05, target));
}

function mediaToDate(mediaTime) {
  return wallAnchor.dateMs + (mediaTime - wallAnchor.mediaTime) * 1000;
}

function dateToMedia(dateMs) {
  return wallAnchor.mediaTime + (dateMs - wallAnchor.dateMs) / 1000;
}

async function connect() {
  const scheduleId = $("scheduleId").value.trim();
  const userId = $("userId").value.trim();
  if (!scheduleId) return;

  $("connectBtn").disabled = true;
  setStatus(dot, stateText, "warn", "connecting…");

  try {
    // Teacher/monitor token: role TEACHER, scheduleIds=[scheduleId].
    const { token } = await mintToken({ role: "teacher", scheduleId, userId });
    currentScheduleId = scheduleId;
    currentUserId = userId;
    currentToken = token;

    ws = new WebSocket(wsURL("/ws/monitor", { scheduleId, token }));

    ws.onopen = () => setStatus(dot, stateText, "ok", `watching ${scheduleId}`);
    ws.onclose = (e) => { setStatus(dot, stateText, "err", `closed (${e.code})`); teardown(); };
    ws.onerror = () => setStatus(dot, stateText, "err", "socket error");

    ws.onmessage = (evt) => {
      const msg = JSON.parse(evt.data);
      switch (msg.type) {
        case "snapshot": renderSnapshot(msg.streams || []); break;
        case "frame":     renderFrame(msg.frame); break;
        case "participant": renderParticipant(msg.event); break;
        case "alert":     renderAlert(msg.alert); break;
      }
    };

    $("disconnectBtn").disabled = false;
  } catch (err) {
    setStatus(dot, stateText, "err", "error");
    prepend(eventsEl, `<span class="badge alert">error</span> ${err.message || err}`);
    $("connectBtn").disabled = false;
  }
}

function renderSnapshot(streams) {
  activeStreams.clear();
  for (const s of streams) {
    activeStreams.set(s.streamId, {
      sessionId: s.sessionId, participantId: s.participantId,
      streamType: s.streamType, startedAt: s.startedAt,
    });
  }
  renderStudentList();

  eventsEl.querySelectorAll('[data-snap]').forEach((n) => n.remove());
  if (streams.length === 0) {
    prepend(eventsEl, `<span class="muted" data-snap>— chưa có ai online —</span>`);
    return;
  }
  for (const s of streams) {
    prepend(eventsEl, `<span class="badge join" data-snap>online</span> <b>${s.participantId}</b> · ${s.streamType} · <span class="muted">${s.streamId.slice(0, 8)}</span>`);
  }
}

function renderFrame(frame) {
  if (!frame || !frame.frameUrl) return;
  $("framesEmpty").style.display = "none";

  let tile = tiles.get(frame.streamId);
  if (!tile) {
    const el = document.createElement("div");
    el.className = "frame-tile";
    el.innerHTML = `<img alt="frame" /><div class="meta"><span class="label"></span><span class="seq"></span></div>`;
    framesEl.appendChild(el);
    tile = { root: el, img: el.querySelector("img"), label: el.querySelector(".label"), seq: el.querySelector(".seq") };
    tiles.set(frame.streamId, tile);
  }
  tile.img.src = frame.frameUrl; // presigned MinIO/S3 JPEG URL
  tile.label.textContent = `${frame.streamType} · ${frame.streamId.slice(0, 8)}`;
  tile.seq.textContent = `#${frame.sequenceNo}`;
}

function renderParticipant(ev) {
  if (!ev) return;
  const cls = ev.type === "joined" ? "join" : "leave";
  prepend(eventsEl, `<span class="badge ${cls}">${ev.type}</span> <b>${ev.participantId}</b> · ${ev.streamType} <span class="muted">${fmtTime(ev.at)}</span>`);

  if (ev.type === "joined") {
    activeStreams.set(ev.streamId, {
      sessionId: null, participantId: ev.participantId,
      streamType: ev.streamType, startedAt: ev.at,
    });
  } else if (ev.type === "left") {
    activeStreams.delete(ev.streamId);
    const tile = tiles.get(ev.streamId);
    if (tile) { tile.root.remove(); tiles.delete(ev.streamId); }
    if (selectedStreamId === ev.streamId) {
      stopPlayer("Học viên này đã ngừng stream.");
    }
  }
  renderStudentList();
}

function renderAlert(a) {
  if (!a) return;
  const level = (a.level || "INFO").toUpperCase();
  prepend(alertsEl,
    `<span class="badge alert">${level}</span> <b>${a.alertType}</b> · ${a.participantId || "?"} ` +
    `<span class="muted">conf=${(a.confidence ?? 0).toFixed(2)} · ${a.source} · ${fmtTime(a.capturedAt)}</span>` +
    (a.detail ? `<br><span class="muted">${a.detail}</span>` : ""));
}

// ── Student list + live (HLS) playback ──────────────────────────────────────

function renderStudentList() {
  studentListEl.innerHTML = "";
  const entries = [...activeStreams.entries()];
  $("studentListEmpty").style.display = entries.length ? "none" : "";

  for (const [streamId, s] of entries) {
    const div = document.createElement("div");
    div.className = "item clickable" + (streamId === selectedStreamId ? " selected" : "");
    div.innerHTML = `<b>${s.participantId}</b><br><span class="stream-type">${s.streamType} · bắt đầu ${fmtTime(s.startedAt)} · ${streamId.slice(0, 8)}</span>`;
    div.addEventListener("click", () => selectStream(streamId, s));
    studentListEl.appendChild(div);
  }
}

function selectStream(streamId, info) {
  selectedStreamId = streamId;
  renderStudentList();
  playStream(streamId, info);
}

function manifestUrl(streamId, token) {
  return httpURL(`/live/${currentScheduleId}/${streamId}/playlist.m3u8`, { token });
}

function playStream(streamId, info) {
  teardownPlayer();
  setPlayerStatus(`Đang tải stream của ${info.participantId}…`, "muted");

  const url = manifestUrl(streamId, currentToken);

  // hls.js is tried FIRST and native HLS is only the fallback -- the order hls.js's own docs
  // recommend, and getting it backwards is not a subtle preference but a hard break on Chromium:
  // Chrome AND Edge both answer canPlayType("application/vnd.apple.mpegurl") with "maybe", a
  // TRUTHY string, so probing that first sent every Chromium browser down the native path. There
  // the manifest load just sits at networkState=LOADING forever -- no "error" event to surface, no
  // loadedmetadata, videoEl.seekable permanently empty, and getStartDate() undefined (it is a
  // Safari-only API). readDvrWindow() therefore always returned null, which pinned the scrub bar on
  // "Chưa có đoạn nào để tua." and left its min/max/value unwritten -- a thumb frozen at 0 -- with
  // no way to ever build the wall-clock anchor.
  if (typeof Hls === "undefined" || !Hls.isSupported()) {
    if (videoEl.canPlayType("application/vnd.apple.mpegurl")) {
      // Safari/iOS: no MSE for hls.js to use, but the platform plays the manifest itself.
      usingNativeHls = true;
      dvrRange = null;
      wallAnchor = null;
      videoEl.src = url;
      videoEl.onloadedmetadata = () => setPlayerStatus(`Đang xem: ${info.participantId} (${info.streamType})`, "");
      videoEl.onerror = () => setPlayerStatus(
        "Không tải được stream (live rewind chưa bật ở server, hoặc chưa có đoạn nào sẵn sàng — xem frame JPEG bên dưới thay thế).",
        "err");
      startTokenRefresh(streamId);
      startSeekBarUpdates(streamId);
      return;
    }
    setPlayerStatus("Trình duyệt này không hỗ trợ phát HLS (cần Chrome/Firefox/Edge/Safari bản mới).", "err");
    return;
  }

  usingNativeHls = false;
  dvrRange = null;
  wallAnchor = null;
  let mediaErrorRecoveries = 0;
  hls = new Hls({
    liveSyncDurationCount: 3,
    // Every request to this service's own /live/ routes -- the manifest hls.js re-polls on its
    // own as a live playlist, AND the init/fragment URLs the manifest points at, which are now
    // served by us rather than being presigned S3 URLs -- is re-targeted to carry the latest
    // currentToken. That makes the periodic background refresh (see startTokenRefresh) enough to
    // keep playing past the monitor token's TTL without ever reloading the source. Fragment URLs
    // already arrive with a fresh token from the manifest that listed them; rewriting them too
    // covers the case where hls.js re-fetches one it has been holding for a while (rewinding).
    xhrSetup: (xhr, requestUrl) => {
      if (!requestUrl.includes("/live/")) return;
      const withToken = new URL(requestUrl, window.location.href);
      withToken.searchParams.set("token", currentToken);
      xhr.open("GET", withToken.toString(), true);
    },
  });
  hls.on(Hls.Events.MANIFEST_PARSED, () => {
    mediaErrorRecoveries = 0;
    setPlayerStatus(`Đang xem: ${info.participantId} (${info.streamType})`, "");
  });
  // Fires on every manifest reload (live playlist), not just the first parse -- this is the one
  // place that reflects the FULL current server-side DVR window, independent of how much of it
  // this particular browser tab has actually downloaded.
  hls.on(Hls.Events.LEVEL_UPDATED, (_evt, data) => {
    const frags = data.details && data.details.fragments;
    if (!frags || !frags.length) return;
    const first = frags[0];
    const last = frags[frags.length - 1];
    dvrRange = { start: first.start, end: last.start + last.duration };
    // hls.js resolves #EXT-X-PROGRAM-DATE-TIME per fragment (accumulating #EXTINF from each
    // anchor), so this pins the media timeline to real time. Re-read on every reload rather than
    // cached from the first one: after a discontinuity the server re-anchors, and the first
    // fragment in the window changes as it slides.
    if (typeof first.programDateTime === "number") {
      wallAnchor = { mediaTime: first.start, dateMs: first.programDateTime };
    }
  });
  hls.on(Hls.Events.ERROR, (_evt, data) => {
    // Surfaced to console (not just the fatal branch below) because non-fatal buffer/stall
    // errors are exactly what a silent playback freeze looks like from the outside -- this is
    // the only place that ever sees hls.js's own diagnosis of *why* it stalled.
    console.warn("hls.js error", data.type, data.details, "fatal=" + data.fatal, data);
    if (!data.fatal) return;

    // Standard hls.js-recommended fatal-error recovery (see hls.js README "Error handling"):
    // NETWORK_ERROR retries the load, MEDIA_ERROR attempts a buffer/decoder recovery -- without
    // this, ANY fatal error (even a single transient one hls.js could have recovered from)
    // permanently killed playback with a static message and no way to resume without re-clicking
    // the student in the list.
    switch (data.type) {
      case Hls.ErrorTypes.NETWORK_ERROR:
        hls.startLoad();
        return;
      case Hls.ErrorTypes.MEDIA_ERROR:
        if (mediaErrorRecoveries < 3) {
          mediaErrorRecoveries++;
          hls.recoverMediaError();
          return;
        }
        break;
    }
    setPlayerStatus(
      "Không tải được stream (live rewind chưa bật ở server — FFMPEG_INGEST_HLS_ENABLED, hoặc chưa có đoạn nào sẵn sàng — xem frame JPEG bên dưới thay thế).",
      "err");
  });
  hls.loadSource(url);
  hls.attachMedia(videoEl);
  startTokenRefresh(streamId);
  startSeekBarUpdates(streamId);
}

// Custom scrub bar, refreshed on a timer rather than just on "progress"/"timeupdate" so its
// numbers keep advancing even if playback itself stalls -- exactly the signal needed to tell a
// genuine DVR bug (the seekable range not growing to match real elapsed time) apart from ordinary
// playback pausing.
function startSeekBarUpdates(streamId) {
  stopSeekBarUpdates();
  isFollowingLiveEdge = true;
  seekDomain = null;
  seekBarEl.style.display = "";
  updateSeekBar(streamId);
  seekBarTimer = setInterval(() => updateSeekBar(streamId), 500);
}

function stopSeekBarUpdates() {
  if (seekBarTimer) {
    clearInterval(seekBarTimer);
    seekBarTimer = null;
  }
  seekBarEl.style.display = "none";
  seekDragging = false;
  seekDomain = null;
}

// Reads the current DVR window in media time. Deliberately NOT videoEl.buffered: that only
// reflects what this browser tab has downloaded (a teacher opening the monitor an hour into an
// exam would see the last few seconds only, even though the server still lists every fragment),
// and seekable's live-duration fallback isn't populated reliably by hls.js/Chrome in practice.
function readDvrWindow() {
  if (!usingNativeHls) {
    return dvrRange; // from hls.js's own parsed playlist, see LEVEL_UPDATED in playStream
  }
  // Safari's native AVFoundation engine manages seekable directly against the whole playlist,
  // so there it IS the authoritative window -- and getStartDate() is the platform's own
  // #EXT-X-PROGRAM-DATE-TIME readout, the native equivalent of the hls.js anchor.
  const seekable = videoEl.seekable;
  if (!seekable || seekable.length === 0) return null;
  if (!wallAnchor && typeof videoEl.getStartDate === "function") {
    const startDate = videoEl.getStartDate();
    if (startDate && !isNaN(startDate.getTime())) {
      wallAnchor = { mediaTime: 0, dateMs: startDate.getTime() };
    }
  }
  return { start: seekable.start(0), end: seekable.end(seekable.length - 1) };
}

function updateSeekBar(streamId) {
  const dvr = readDvrWindow();
  if (!dvr) {
    seekInfoEl.textContent = "Chưa có đoạn nào để tua.";
    return;
  }
  dvrRange = dvr;

  const info = activeStreams.get(streamId);
  const streamStartMs = info && info.startedAt ? new Date(info.startedAt).getTime() : NaN;

  // The slider spans the stream's whole real elapsed time -- [start request .. now] -- not just
  // the part that happens to be seekable. That is what keeps the bar's length equal to the
  // stream's own duration no matter when this player attached or how many times it was torn
  // down. Needs both an absolute anchor for the media timeline and a known stream start; without
  // either, fall back to plotting raw media time (the old behaviour).
  const absolute = wallAnchor !== null && !isNaN(streamStartMs);
  if (!seekDragging || !seekDomain) {
    seekDomain = absolute
      ? { startMs: streamStartMs, endMs: Date.now(), absolute: true }
      : { startMs: 0, endMs: 0, absolute: false };
  }

  let dvrStartOffset, dvrEndOffset, domainEndOffset, playheadOffset;
  if (seekDomain.absolute) {
    const toOffset = (ms) => (ms - seekDomain.startMs) / 1000;
    dvrStartOffset = toOffset(mediaToDate(dvrRange.start));
    dvrEndOffset = toOffset(mediaToDate(dvrRange.end));
    domainEndOffset = toOffset(seekDomain.endMs);
    playheadOffset = toOffset(mediaToDate(videoEl.currentTime));
  } else {
    dvrStartOffset = dvrRange.start;
    dvrEndOffset = dvrRange.end;
    domainEndOffset = dvrRange.end;
    playheadOffset = videoEl.currentTime;
  }

  if (!seekDragging) {
    // Frozen while dragging: the live edge advances twice a second, and moving max out from
    // under the thumb mid-drag silently changes where the user thinks they are aiming.
    seekRangeEl.min = "0";
    seekRangeEl.max = String(Math.max(domainEndOffset, dvrEndOffset));
    seekRangeEl.value = String(playheadOffset);
  }

  liveBtnEl.classList.toggle("live-active", isFollowingLiveEdge);

  const parts = [];
  if (seekDomain.absolute) {
    parts.push(`đã live: ${fmtDuration(domainEndOffset)}`);
    parts.push(`tua được: ${fmtDuration(dvrStartOffset)}–${fmtDuration(dvrEndOffset)}`);
  } else {
    parts.push(`tua được: ${fmtDuration(dvrStartOffset)}–${fmtDuration(dvrEndOffset)} (mốc tương đối — server chưa gửi PROGRAM-DATE-TIME)`);
  }
  parts.push(isFollowingLiveEdge
    ? "đang xem trực tiếp"
    : `đang xem lại (cách live ${fmtDuration(Math.max(0, dvrEndOffset - playheadOffset))})`);
  seekInfoEl.textContent = parts.join(" · ");
}

function fmtDuration(seconds) {
  if (!isFinite(seconds) || seconds < 0) seconds = 0;
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  const mm = h > 0 ? String(m).padStart(2, "0") : String(m);
  return `${h > 0 ? h + ":" : ""}${mm}:${String(s).padStart(2, "0")}`;
}

// Keeps currentToken valid for as long as a stream stays selected. hls.js picks up the refreshed
// token on its own next manifest poll via xhrSetup above; native Safari has no such hook, so the
// only way to keep it authenticated is to re-point videoEl.src, which jumps back to the live edge
// (a brief, visible reset -- not seamless, but the only option the platform allows here). Skipped
// entirely while the user is deliberately rewound: forcing them back to live mid-review would be
// exactly the unwanted auto-snap this whole isFollowingLiveEdge flag exists to avoid. Delaying the
// refresh until they return to live (or reselect the stream) is the acceptable tradeoff -- the
// token still has margin left before its 5-minute TTL by the time this would fire again.
function startTokenRefresh(streamId) {
  stopTokenRefresh();
  tokenRefreshTimer = setInterval(async () => {
    try {
      const { token } = await mintToken({ role: "teacher", scheduleId: currentScheduleId, userId: currentUserId });
      currentToken = token;
      if (usingNativeHls && selectedStreamId === streamId && isFollowingLiveEdge) {
        videoEl.src = manifestUrl(streamId, currentToken);
      }
    } catch (err) {
      console.warn("monitor token refresh failed", err);
    }
  }, TOKEN_REFRESH_INTERVAL_MS);
}

function stopTokenRefresh() {
  if (tokenRefreshTimer) {
    clearInterval(tokenRefreshTimer);
    tokenRefreshTimer = null;
  }
}

function teardownPlayer() {
  stopTokenRefresh();
  stopSeekBarUpdates();
  usingNativeHls = false;
  dvrRange = null;
  wallAnchor = null;
  if (hls) { hls.destroy(); hls = null; }
  videoEl.onloadedmetadata = null;
  videoEl.onerror = null;
  videoEl.removeAttribute("src");
  videoEl.load();
}

// "Ngừng xem": stops playback and frees the player, but deliberately keeps the WebSocket, the
// student list and each stream's startedAt. Watching again re-attaches to the same absolute
// timeline (see wallAnchor), so the stream's own elapsed duration keeps running underneath and
// picking it back up does not look like the stream restarted. Contrast Disconnect/teardown, which
// tears down the whole monitor session.
function stopWatching() {
  teardownPlayer();
  setPlayerStatus(
    selectedStreamId
      ? "Đã ngừng xem. Bấm lại học viên trong danh sách để xem tiếp — thời lượng stream vẫn tiếp tục chạy."
      : "Chọn một học viên ở danh sách bên trái để xem stream.",
    "muted");
}

function stopPlayer(reason) {
  teardownPlayer();
  selectedStreamId = null;
  renderStudentList();
  setPlayerStatus(reason || "Chọn một học viên ở danh sách bên trái để xem stream.", "muted");
}

function setPlayerStatus(text, cls) {
  playerStatusEl.textContent = text;
  playerStatusEl.className = cls || "";
}

// ── misc ─────────────────────────────────────────────────────────────────

function prepend(container, html) {
  const div = document.createElement("div");
  div.className = "item";
  div.innerHTML = html;
  container.prepend(div);
  while (container.children.length > 100) container.lastChild.remove();
}

function fmtTime(t) {
  if (!t) return "";
  const d = new Date(t);
  return isNaN(d) ? "" : d.toLocaleTimeString();
}

function disconnect(_reason) {
  try { ws && ws.readyState === WebSocket.OPEN && ws.close(1000, "client disconnect"); } catch {}
  teardown();
}

function teardown() {
  ws = null;
  stopPlayer();
  activeStreams.clear();
  renderStudentList();
  $("connectBtn").disabled = false;
  $("disconnectBtn").disabled = true;
  setStatus(dot, stateText, "", "idle");
}
