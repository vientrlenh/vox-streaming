#!/usr/bin/env bash
# Run the vox-streaming service on the HOST, wired to the dockerized demo infra.
# See run-service.ps1 for the rationale (WebRTC UDP over localhost + host ffmpeg).
#
# Prereqs:
#   docker compose -f demo/docker-compose.yml up -d --build
#   ffmpeg on PATH  (apt install ffmpeg / brew install ffmpeg)
#
# Usage:  ./demo/run-service.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# godotenv.Load() fatals if ./.env is missing. Values exported here win because
# godotenv does not override variables already present in the environment.
[ -f .env ] || touch .env

export JWT_STREAM_SECRET="demo-super-secret-change-me-1234567890"  # MUST match docker-compose devserver
export ALLOWED_ORIGIN="http://localhost:5173"

export WEBRTC_ADDR=":8082"
export GRPC_ADDR=":9096"
export METRIC_ADDR=":9090"
export FRAME_INTERVAL_SECS="5"
export WEBRTC_UDP_PORT="50000"   # mux all WebRTC media onto one UDP port
# export WEBRTC_NAT_1TO1_IP=""    # set to the public IP when behind a 1:1 NAT

export REDIS_ADDR="localhost:6379"
export REDIS_PASSWORD=""

export KAFKA_BROKERS="localhost:9094"
export KAFKA_CONSUMER_GROUP="vox-streaming"
export KAFKA_TLS_ENABLED="false"
export KAFKA_USERNAME=""
export KAFKA_PASSWORD=""

export STORAGE_ENDPOINT="localhost:9000"
export STORAGE_ACCESS_KEY="minioadmin"
export STORAGE_SECRET_KEY="minioadmin"
export STORAGE_USE_SSL="false"
export STORAGE_FRAME_BUCKET="vox-frames"
export STORAGE_RECORDING_BUCKET="vox-recordings"

export EXAM_SERVICE_GRPC_ADDR="localhost:9095"
export GRPC_SERVICE_TOKEN=""

export TURN_URL=""
export AI_RELAY_ENABLED="false"
export SLACK_WEBHOOK_URL=""

# Live-rewind HLS output for the Monitor page's "click a student to watch" feature.
# Off by default in production (main.go); on here so the demo works out of the box.
export FFMPEG_INGEST_HLS_ENABLED="true"
export FFMPEG_INGEST_HLS_SEGMENT_SECONDS="4"
export FFMPEG_INGEST_HLS_AUDIO_BITRATE_K="128"
export LIVE_REWIND_WINDOW_MINUTES="0"   # 0 = full DVR from stream start; >0 trims to that trailing window

command -v ffmpeg >/dev/null 2>&1 || echo "WARNING: ffmpeg not on PATH — monitor JPEG frames and final MP4 assembly will fail (streaming + fMP4 recording still work)."

echo "vox-streaming -> ws :8082 | metrics :9090 | grpc :9096  (Ctrl+C to stop)"
exec go run ./cmd/server
