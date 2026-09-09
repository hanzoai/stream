# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26.5-alpine AS builder
# go.mod pins the toolchain. The golang base image sets GOTOOLCHAIN=local,
# which turns a `go` directive newer than the image into a hard build
# failure instead of a download.
ENV GOTOOLCHAIN=auto

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
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /hanzo-kafka .

# One CGO_ENABLED=0 binary that talks to NATS over TLS. This base carries the CA
# bundle and points SSL_CERT_FILE at it, so the certs need no package manager.
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.source="https://github.com/hanzoai/kafka"
LABEL org.opencontainers.image.description="Hanzo Kafka - Kafka-compatible streaming over NATS"
LABEL org.opencontainers.image.licenses="MIT"

COPY --from=builder /hanzo-kafka /usr/local/bin/hanzo-kafka

EXPOSE 9092 9093

ENTRYPOINT ["hanzo-kafka"]
CMD ["--pubsub-url", "nats://pubsub:4222", "--host", "0.0.0.0"]
