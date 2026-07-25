import { mintToken, wsURL, makeLogger, setStatus } from "./common.js";

const $ = (id) => document.getElementById(id);
const log = makeLogger($("log"));
const dot = $("dot");
const stateText = $("stateText");

let pc = null;
let ws = null;
let localStream = null;

$("startBtn").addEventListener("click", start);
$("stopBtn").addEventListener("click", () => stop("stopped by user"));

async function start() {
  const scheduleId = $("scheduleId").value.trim();
  const userId = $("userId").value.trim();
  const sessionId = $("sessionId").value.trim();
  const streamType = $("streamType").value;
  if (!scheduleId) return log.err("scheduleId is required");

  $("startBtn").disabled = true;
  setStatus(dot, stateText, "warn", "starting…");

  try {
    // 1. Capture media (camera+mic, or screen).
    if (streamType === "screen") {
      localStream = await navigator.mediaDevices.getDisplayMedia({ video: true, audio: true });
      log.info("captured screen");
    } else {
      localStream = await navigator.mediaDevices.getUserMedia({ video: true, audio: true });
      log.info("captured camera + mic");
    }
    $("preview").srcObject = localStream;

    // 2. Mint a student JWT (role STUDENT, scheduleId, streamTypes include this one).
    const { token, sessionId: mintedSessionId } = await mintToken({ role: "student", scheduleId, userId, sessionId, streamTypes: "camera,screen" });
    log.ok(`token minted (sessionId=${mintedSessionId})`);

    // 3. RTCPeerConnection. STUN default is fine; on localhost host candidates connect directly.
    pc = new RTCPeerConnection({ iceServers: [{ urls: "stun:stun.l.google.com:19302" }] });

    pc.oniceconnectionstatechange = () => log.info(`ICE: ${pc.iceConnectionState}`);
    pc.onconnectionstatechange = () => {
      const s = pc.connectionState;
      log.info(`PC: ${s}`);
      if (s === "connected") setStatus(dot, stateText, "ok", "streaming");
      else if (s === "failed" || s === "disconnected") setStatus(dot, stateText, "err", s);
    };

    // Add tracks and force H.264 so the recorder accepts the video.
    for (const track of localStream.getTracks()) {
      const sender = pc.addTrack(track, localStream);
      if (track.kind === "video") preferH264(sender);
    }

    // 4. Open signaling WebSocket (same-origin, proxied by nginx to the host server).
    ws = new WebSocket(wsURL("/ws/stream", { scheduleId, streamType, token }));

    ws.onopen = async () => {
      log.ok("signaling connected — sending offer");
      // Browser is the offerer; server answers.
      const offer = await pc.createOffer();
      await pc.setLocalDescription(offer);
      ws.send(JSON.stringify({ type: "offer", sdp: offer.sdp }));
    };

    pc.onicecandidate = (e) => {
      if (e.candidate && ws?.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ type: "ice-candidate", candidate: e.candidate.toJSON() }));
      }
    };

    ws.onmessage = async (evt) => {
      const msg = JSON.parse(evt.data);
      switch (msg.type) {
        case "answer":
          await pc.setRemoteDescription({ type: "answer", sdp: msg.sdp });
          log.ok("answer received — negotiation done");
          break;
        case "ice-candidate":
          if (msg.candidate) {
            try { await pc.addIceCandidate(msg.candidate); }
            catch (err) { log.warn(`addIceCandidate: ${err}`); }
          }
          break;
        case "error":
          log.err(`server error: ${msg.message || "unknown"}`);
          break;
      }
    };

    ws.onclose = (e) => { log.warn(`signaling closed (${e.code})`); stop("ws closed"); };
    ws.onerror = () => log.err("signaling socket error");

    $("stopBtn").disabled = false;
  } catch (err) {
    log.err(`start failed: ${err.message || err}`);
    setStatus(dot, stateText, "err", "error");
    stop("start failed");
  }
}

// Reorder codecs so H.264 is first in the offer (server only registers H.264/Opus).
function preferH264(sender) {
  try {
    const caps = RTCRtpSender.getCapabilities("video");
    if (!caps) return;
    const h264 = caps.codecs.filter((c) => c.mimeType.toLowerCase() === "video/h264");
    const others = caps.codecs.filter((c) => c.mimeType.toLowerCase() !== "video/h264");
    if (h264.length === 0) { log.warn("browser reports no H.264 encoder"); return; }
    const tx = pc.getTransceivers().find((t) => t.sender === sender);
    if (tx && tx.setCodecPreferences) {
      tx.setCodecPreferences([...h264, ...others]);
      log.info("codec preference set: H.264 first");
    }
  } catch (err) {
    log.warn(`setCodecPreferences skipped: ${err}`);
  }
}

function stop(reason) {
  if (!pc && !ws && !localStream) return;
  log.info(`stopping: ${reason}`);
  try { ws && ws.readyState === WebSocket.OPEN && ws.close(1000, "client stop"); } catch {}
  ws = null;
  try { pc && pc.close(); } catch {}
  pc = null;
  if (localStream) { localStream.getTracks().forEach((t) => t.stop()); localStream = null; }
  $("preview").srcObject = null;
  $("startBtn").disabled = false;
  $("stopBtn").disabled = true;
  setStatus(dot, stateText, "", "idle");
}
