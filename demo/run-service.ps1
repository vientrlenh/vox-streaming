# Run the vox-streaming service on the HOST, wired to the dockerized demo infra.
#
# Why on the host? WebRTC media uses ephemeral UDP ports with no fixed range in
# the code. On the host, browser <-> server media flows over localhost directly,
# no Docker NAT to fight. (Frame JPEG conversion + final MP4 assembly shell out
# to ffmpeg, so ffmpeg must be on PATH.)
#
# Prereqs:
#   docker compose -f demo/docker-compose.yml up -d --build   # infra first
#   winget install Gyan.FFmpeg                                 # ffmpeg on PATH
#
# Usage (from anywhere):  ./demo/run-service.ps1

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

# cmd/server calls godotenv.Load() which FATALS if ./.env is missing. The values
# below are exported to the process env first, and godotenv does NOT override
# already-set variables — so these win over whatever is in your real .env.
if (-not (Test-Path ".env")) { New-Item -ItemType File ".env" | Out-Null }

$env:JWT_STREAM_SECRET        = "demo-super-secret-change-me-1234567890"  # MUST match docker-compose devserver
$env:ALLOWED_ORIGIN           = "http://localhost:5173"

$env:WEBRTC_ADDR              = ":8082"
$env:GRPC_ADDR                = ":9096"
$env:METRIC_ADDR              = ":9090"
$env:FRAME_INTERVAL_SECS      = "5"
$env:WEBRTC_UDP_PORT          = "50000"  # mux all WebRTC media onto one UDP port
# $env:WEBRTC_NAT_1TO1_IP     = ""        # set to the public IP when behind a 1:1 NAT

$env:REDIS_ADDR               = "localhost:6379"
$env:REDIS_PASSWORD           = ""

$env:KAFKA_BROKERS            = "localhost:9094"
$env:KAFKA_CONSUMER_GROUP     = "vox-streaming"
$env:KAFKA_TLS_ENABLED        = "false"
$env:KAFKA_USERNAME           = ""
$env:KAFKA_PASSWORD           = ""

$env:STORAGE_ENDPOINT         = "localhost:9000"
$env:STORAGE_ACCESS_KEY       = "minioadmin"
$env:STORAGE_SECRET_KEY       = "minioadmin"
$env:STORAGE_USE_SSL          = "false"
$env:STORAGE_FRAME_BUCKET     = "vox-frames"
$env:STORAGE_RECORDING_BUCKET = "vox-recordings"

$env:EXAM_SERVICE_GRPC_ADDR   = "localhost:9095"
$env:GRPC_SERVICE_TOKEN       = ""

# Keep recording-assembly temp files under the Windows temp dir (default is the
# Unix-style /var/tmp/vox-assembly, which lands at the drive root on Windows).
$env:ASSEMBLER_WORK_DIR       = Join-Path $env:TEMP "vox-assembly"

$env:TURN_URL                 = ""        # local only, no TURN needed
$env:AI_RELAY_ENABLED         = "false"
$env:SLACK_WEBHOOK_URL        = ""

# Live-rewind HLS output for the Monitor page's "click a student to watch" feature.
# Off by default in production (main.go); on here so the demo works out of the box.
$env:FFMPEG_INGEST_HLS_ENABLED = "true"
$env:FFMPEG_INGEST_HLS_SEGMENT_SECONDS = "4"
$env:FFMPEG_INGEST_HLS_AUDIO_BITRATE_K = "128"
$env:LIVE_REWIND_WINDOW_MINUTES        = "0"   # 0 = full DVR from stream start; >0 trims to that trailing window

# ffprobe is checked separately from ffmpeg: it ships alongside it in every normal install, but the
# assembler's recording quality checks (missing track, unplayable file, silent audio) shell out to
# ffprobe specifically, and a PATH with only one of the two fails those silently -- they are
# non-fatal by design, so the recording still uploads with the checks quietly skipped.
foreach ($bin in @("ffmpeg", "ffprobe")) {
  if (-not (Get-Command $bin -ErrorAction SilentlyContinue)) {
    Write-Warning "$bin not found on PATH. Streaming + fMP4 recording still work, but monitor JPEG frames, final MP4 assembly and recording quality checks need it. Install: winget install Gyan.FFmpeg"
  }
}

Write-Host "vox-streaming -> ws :8082 | metrics :9090 | grpc :9096  (Ctrl+C to stop)" -ForegroundColor Cyan
go run ./cmd/server
