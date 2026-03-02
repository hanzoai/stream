# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

RUN apk add --no-cache git

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /hanzo-stream .

FROM alpine:3.20

LABEL org.opencontainers.image.source="https://github.com/hanzoai/stream"
LABEL org.opencontainers.image.description="Hanzo Stream - Kafka-compatible streaming over NATS"
LABEL org.opencontainers.image.licenses="MIT"

RUN apk add --no-cache ca-certificates
COPY --from=builder /hanzo-stream /usr/local/bin/hanzo-stream

EXPOSE 9092 9093

ENTRYPOINT ["hanzo-stream"]
CMD ["--pubsub-url", "nats://pubsub:4222", "--host", "0.0.0.0"]
