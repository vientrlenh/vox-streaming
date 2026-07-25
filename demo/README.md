# vox-streaming — demo harness

A local, browser-based test harness for the streaming/recording service. It lets
you exercise the real flows end-to-end without AWS or the Spring/exam backend:

- **Student** page — push webcam or screen to the server over WebRTC (H.264/Opus).
  The server records fMP4 segments to storage and extracts periodic keyframes.
- **Monitor** page — a clickable list of who's currently streaming (click one to watch
  its live HLS feed via `hls.js`, with a DVR scrub bar covering the stream's whole
  elapsed duration), plus periodic JPEG frames for every stream, join/leave events,
  and alerts.
- **devserver** — a dev-only exam gRPC stub (`ValidateAccess`/`UpdateRecording`
  always succeed) plus a JWT minting endpoint (the browser can't hold the secret).
- Local infra: **Redis + Kafka + MinIO** (MinIO stands in for AWS S3).

> ⚠️ Everything here is for local testing only. `devserver` grants access to
> everyone and hands out tokens freely — never deploy it.

## Architecture

```
browser (http://localhost:5173)
   │   /token                          -> nginx -> devserver:8090         (mint JWT)
   │   /ws/*  /schedules/*  /live/*    -> nginx -> host.docker.internal:8082
   ▼
docker compose:  redis · kafka · minio · devserver · web(nginx)
host process:    vox-streaming  (go run ./cmd/server)  <-- WebRTC media, ffmpeg
```

The streaming service runs on the **host** (not in Docker) on purpose: WebRTC media
uses ephemeral UDP ports with no fixed range in the code, so keeping the server on
the host lets media flow over `localhost` without Docker NAT. nginx reverse-proxies
everything to a single origin (`localhost:5173`) so there are no CORS issues and the
server's WebSocket Origin check passes.

## Prerequisites

- Docker + Docker Compose
- Go (to run the service on the host)
- **ffmpeg on PATH** — needed for monitor JPEG frames and final MP4 assembly.
  Streaming and fMP4 segment recording work without it.
  - Windows: `winget install Gyan.FFmpeg`
  - macOS: `brew install ffmpeg` · Debian/Ubuntu: `sudo apt install ffmpeg`

## Run

From the repository root:

```bash
# 1. Start local infra (redis, kafka, minio, devserver, web)
docker compose -f demo/docker-compose.yml up -d --build

# 2. Start the streaming service on the host
#    Windows:
./demo/run-service.ps1
#    macOS / Linux:
./demo/run-service.sh
```

Then open **http://localhost:5173**.

### Order matters

1. Open **Monitor**, set the schedule ID (default `00000000-0000-0000-0000-000000000001`),
   click **Connect**. The server only converts frames to JPEG while a monitor is
   subscribed (it checks Redis `PUBSUB NUMSUB`), so connect the monitor first.
2. Open **Student** in another tab, same schedule ID, pick *camera* or *screen*, **Start**.
3. Back on Monitor: the student appears in **Học viên đang stream** — click their entry
   to watch the live HLS feed (only if the server was started with
   `FFMPEG_INGEST_HLS_ENABLED=true`; otherwise you'll see a fallback message and can
   still watch the periodic JPEG frame below). Join/leave events and alerts stream in too.
   Stop the student (or close the tab) to trigger recording finalize.

### DVR / live rewind

The scrub bar under the player spans the stream's **whole elapsed time**, from when the
student pressed Start to now — not just what this browser tab has downloaded. It stays
that way across **⏹ Ngừng xem** / re-watch, because the manifest carries
`#EXT-X-PROGRAM-DATE-TIME` and the bar is plotted in absolute time rather than in the
player's own media timeline. The readout shows `đã live` (real elapsed), `tua được` (the
part actually fetchable right now), and how far behind live you are.

`LIVE_REWIND_WINDOW_MINUTES=0` (the default in the run scripts) means "keep the whole
stream". Set it to a positive number of minutes to trim the playlist to that trailing
window instead — then the bar still spans the full stream, but only the tail is seekable.

Note the gap between the two numbers: media time runs a little behind wall clock (a few
percent, from encoder frame drops on the sending side), plus one fragment of segmenting
latency and its upload. A `tua được` end a handful of seconds behind `đã live` is normal.

## Serving the client standalone (no nginx)

The pages default to **proxy mode** (relative URLs behind the `web` nginx
container). To serve `web/` yourself instead — e.g. while iterating on the
service with `go run` — switch the client to **direct mode**:

1. Edit `web/config.js` and set the absolute backend bases:

   ```js
   window.VOX = {
     tokenBase:      "http://localhost:8090", // devserver
     serverHttpBase: "http://localhost:8082", // streaming server
     serverWsBase:   "ws://localhost:8082",
   };
   ```

2. Serve `web/` on **port 5173** (must match `ALLOWED_ORIGIN`), e.g.:

   ```bash
   python -m http.server 5173 --directory demo/web
   # or:  npx serve -l 5173 demo/web
   ```

You still need the backend pieces running — direct mode only changes how the
static files are served, not the dependencies:

- **devserver** (token + exam stub) — `docker compose ... up devserver`, or
  `go run ./demo/devserver` with `JWT_STREAM_SECRET` set to the same value.
- **Redis + Kafka + MinIO** — the service won't start without them.

No CORS headaches: the client only calls `/token` (devserver sends
`Access-Control-Allow-Origin: *`) and `/ws/*` (no CORS — an Origin check the
`ALLOWED_ORIGIN=http://localhost:5173` setting satisfies). If you serve on a
different port, start the service with `ALLOWED_ORIGIN` set to that exact origin.

## What each feature exercises

| You do | Server path exercised |
| --- | --- |
| Student *Start* | JWT auth → exam `ValidateAccess` → WebRTC offer/answer → track ingest |
| …while streaming | fMP4 segmenter → MinIO; keyframe capture → `FrameReady` (Kafka) |
| Monitor connected | frame-convert consumer → ffmpeg H.264→JPEG → Redis pub/sub → monitor |
| Monitor clicks a student | `GET /live/{scheduleId}/{streamId}/playlist.m3u8` → `hls.js`/native playback, fragments via `GET /live/.../seg-NNNNNN.m4s` (needs `FFMPEG_INGEST_HLS_ENABLED=true`) |
| Student *Stop* | recording finalize → `StreamEnded` → assembler concat (ffmpeg) → MinIO → exam `UpdateRecording` |

Inspect results in the MinIO console at **http://localhost:9001**
(`minioadmin` / `minioadmin`): buckets `vox-frames` and `vox-recordings`.

## Ports

| Port | Service |
| --- | --- |
| 5173 | demo web (nginx) — open this |
| 8082 | streaming server: HTTP/WebSocket (host) |
| 9090 | streaming server: metrics/health (host) |
| 9096 | streaming server: gRPC alert ingest (host) |
| 9000 / 9001 | MinIO API / console |
| 9094 | Kafka (external listener) |
| 6379 | Redis |
| 9095 / 8090 | devserver gRPC / token HTTP |

## Troubleshooting

- **WS closes immediately / 403** — page not opened at `http://localhost:5173`
  (Origin check), or the schedule/role in the token doesn't match.
- **Student connects but Monitor shows no frames** — connect the Monitor *before*
  streaming; confirm ffmpeg is on PATH (frame conversion needs it).
- **Clicking a student in Monitor shows the fallback message, no video** — the server
  needs `FFMPEG_INGEST_HLS_ENABLED=true` (off by default) to produce the parallel HLS
  output; the periodic JPEG frames below still work either way. Give it a few seconds
  after Start for the first HLS fragment to upload.
- **Scrub bar says "mốc tương đối — server chưa gửi PROGRAM-DATE-TIME"** — the page is
  talking to a server build from before absolute DVR anchoring. Playback and rewind still
  work, but the bar falls back to the player's own media timeline, so it re-bases to 0:00
  every time the player is recreated. Restart the streaming service from this repo.
- **Playback dies after ~5 minutes with a "stream is not live" 404** — was the old
  behaviour when the periodic-JPEG loop (the only thing that used to refresh the Redis
  session key) stalled. The peer now heartbeats that key on its own; if you still see it,
  the peer connection itself dropped — check the service log for `peer disconnected`.
- **`create bucket` / storage errors on startup** — MinIO not up yet; wait for its
  healthcheck (`docker compose ps`) and restart the host service.
- **Kafka connection refused** — the service reads `localhost:9094`; make sure the
  `kafka` container is healthy before starting the host service.
- **No video / recording disabled warning** — the browser didn't send H.264. Use a
  recent Chrome/Edge/Firefox; the Student page already pins H.264 codec preference.

## Notes

- JWT secret is shared in two places and must match: `docker-compose.yml`
  (`devserver` env) and the `run-service` scripts. Change both together.
- To reset storage/state: `docker compose -f demo/docker-compose.yml down -v`.
