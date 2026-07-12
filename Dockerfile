# syntax=docker/dockerfile:1

############################################
# Stage 1 — build a static Go binary
############################################
FROM golang:1.26-alpine AS builder

# git: some Go modules are fetched via VCS. ca-certificates: HTTPS to the module proxy.
RUN apk add --no-cache git ca-certificates

WORKDIR /src

# Download dependencies first so this layer is cached until go.mod/go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the rest of the source and build.
COPY . .

# CGO_ENABLED=0 -> fully static binary (all deps are pure Go: pion, aws-sdk, grpc, redis, kafka-go).
# -trimpath + -ldflags "-s -w" strip paths and debug info for a smaller binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/vox-streaming ./cmd/server

############################################
# Stage 2 — minimal runtime image
############################################
FROM alpine:3.21

# ffmpeg      : required at runtime for H.264->JPEG frame conversion
#               (internal/recorder) and recording assembly / concat+remux
#               (internal/usecase/assembler_usecase.go).
# ca-certificates : outbound TLS (S3/MinIO, Slack, gRPC-TLS, Kafka-TLS).
# tzdata      : correct timezone handling for timestamps.
RUN apk add --no-cache ffmpeg ca-certificates tzdata \
    && addgroup -S vox \
    && adduser -S -G vox -H -s /sbin/nologin vox

WORKDIR /app

COPY --from=builder /out/vox-streaming /app/vox-streaming

# Scratch space for in-progress fMP4 segments and recording-assembly jobs.
# Mount a volume/tmpfs here (see docker-compose.yml) so it never fills the container layer.
ENV SEGMENT_TEMP_DIR=/app/tmp
RUN mkdir -p /app/tmp && chown -R vox:vox /app

USER vox

# --- Documented ports (values match .env; overridable via env) ---
# HTTP + WebSocket signaling (WEBRTC_ADDR)
EXPOSE 8082
# gRPC alert ingest server (GRPC_ADDR)
EXPOSE 9096
# Prometheus metrics + health probes: /metrics /healthz /readyz (METRIC_ADDR)
EXPOSE 9090
# WebRTC media travels over UDP on ephemeral ports (Pion default, no fixed range
# configured in code) -> run with host networking, or a TURN relay. See docker-compose.yml.

# Liveness probe against the metrics server (/healthz never touches upstreams).
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD wget -qO- http://127.0.0.1:9090/healthz >/dev/null 2>&1 || exit 1

ENTRYPOINT ["/app/vox-streaming"]
