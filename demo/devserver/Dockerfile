# syntax=docker/dockerfile:1
#
# DEV-ONLY exam stub + JWT token minter for the demo.
# Build context MUST be the repository root (it imports pkg/auth and pkg/pb):
#   docker build -f demo/devserver/Dockerfile -t vox-devserver .
# docker-compose in this folder already sets `context: ..` accordingly.

FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w" -o /out/devserver ./demo/devserver

FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /out/devserver /devserver
# gRPC exam stub / HTTP token minter
EXPOSE 9095 8090
ENTRYPOINT ["/devserver"]
