# Hanzo Kafka — LLM.md

## Overview
Hanzo Kafka is a stateless Kafka wire protocol gateway that translates Kafka client requests to Hanzo Kafka (JetStream) operations against Hanzo PubSub. Standard Kafka clients connect on `:9092`; all storage and replication is delegated to Hanzo PubSub.

## Architecture
```
Kafka Client → TCP :9092 → Hanzo Kafka (protocol translation) → Hanzo PubSub
```

**Stateless gateway**: All state lives in Hanzo PubSub. Multiple instances can share the same PubSub cluster.

## Kafka-to-PubSub Mapping
| Kafka Concept | PubSub Equivalent |
|---|---|
| Topic `foo`, Partition N | Stream `kafka-foo-N`, Subject `kafka.foo.N` |
| Produce | `Publish("kafka.foo.0", recordBatchBytes)` → seq = offset+1 |
| Fetch at offset | `GetMsg(streamName, offset+1)` (PubSub 1-based, Kafka 0-based) |
| Consumer group offsets | KV bucket `kafka-consumer-offsets`, key `{group}.{topic}.{partition}` |
| Create topic (N parts) | N calls to `AddStream()` |
| Metadata | `StreamInfo()` per partition stream |

## Critical: Offset Model
Kafka offsets are STAMPED into each stored RecordBatch header at produce time
(baseOffset, outside the CRC'd region) and read back from those headers —
never derived from PubSub sequences. Hanzo PubSub sequences are only
monotonic, not dense (the production store allocates from a sparse e18 space;
deletes leave holes), so sequence arithmetic is meaningless.

- Produce: next offset = last valid batch's baseOffset + record count; the
  incoming chain is validated (`walkBatches`) and refused with INVALID_RECORD
  if malformed — foreign bytes on a partition subject can otherwise poison
  every consumer.
- Fetch: `findRecordSet` binary-searches the sequence space using
  by-start-sequence probes, skipping stored messages that are not valid
  batches; out-of-range offsets answer OFFSET_OUT_OF_RANGE so clients reset.
- Watermarks: `partitionBounds` walks the stream edges for the first/last
  valid batch.
- Consumer groups: committed offsets live in KV bucket
  `kafka-consumer-offsets` (`group.topic.partition` keys) — the durable state;
  the gateway itself is stateless and restartable. Any non-negative int64 is a
  valid offset. OffsetFetch with a null topics array enumerates the bucket.

## Tests
`go test ./...` — `test/e2e` runs a REAL Kafka client (franz-go) against the
broker over an in-process `hanzoai/pubsub/embed`, covering compression
round-trips, sequence holes, poison messages, e18 offsets, out-of-range
reset, broker-restart group resume, >1MiB records, and two gateways sharing
one store.

## Module Structure
```
github.com/hanzoai/kafka
├── main.go              # CLI entry point (cobra)
├── pubsub/              # Hanzo PubSub client wrapper
│   ├── client.go        # Connection + stream context
│   ├── streams.go       # Stream CRUD, publish, get message, list topics
│   └── consumer.go      # KV-based consumer offset management
├── protocol/            # Kafka wire protocol handlers
│   ├── broker.go        # TCP server, connection handling
│   ├── dispatcher.go    # API key → handler routing
│   ├── produce.go       # Produce (API key 0)
│   ├── fetch.go         # Fetch (API key 1)
│   ├── metadata.go      # Metadata (API key 3)
│   ├── create_topic.go  # CreateTopics (API key 19)
│   ├── responses.go     # ListOffsets, OffsetCommit/Fetch, JoinGroup, etc.
│   ├── find_coordinator.go
│   ├── describe_configs.go
│   ├── api_versions.go
│   ├── types.go         # Request/response struct definitions
│   └── error.go         # Kafka error codes
├── serde/               # Kafka protocol serialization (reflection-based)
├── compress/            # GZIP, Snappy, LZ4, ZSTD codecs
├── logging/             # Simple log levels
├── utils/               # Time utilities
└── types/               # Shared types (Config, Request, Record, RecordBatch)
```

## Key Design Decisions
- **One stream per partition** ensures clean 1:1 offset-sequence mapping
- **Produce/Fetch use hand-written decoders** (not reflection) for performance
- **All other handlers use reflection-based serde** via tagged structs
- **KV bucket for consumer offsets** instead of __consumer-offsets topic
- **No local storage, no Raft, no Serf** — pure protocol translation

## Dependencies
- `github.com/nats-io/nats.go` — Hanzo PubSub client
- `github.com/spf13/cobra` — CLI
- Compression: klauspost/compress (zstd), pierrec/lz4, eapache/go-xerial-snappy
- Zero hashicorp dependencies

## Running
```bash
# Start Hanzo PubSub
nats-server --jetstream

# Start Hanzo Kafka
go run main.go --pubsub-url nats://localhost:4222 --port 9092

# Use standard Kafka CLI tools
kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

## Admin HTTP API (port 9093)

All management endpoints under `/v1/stream/`:
```
GET /v1/stream/status  — service status (JSON)
GET /v1/stream/topics  — list topics with partition details
GET /v1/stream/groups  — list consumer group offsets
GET /healthz           — health check (K8s probes)
```

Root `/` redirects to `/v1/stream/`.

## Deployment (hanzo-k8s, do-sfo3)
```
Namespace: hanzo
PubSub:    pubsub.hanzo.svc:4222   (nats:2.10-alpine, 1 replica, 20Gi PVC)
Stream:    stream.hanzo.svc:9092   (ghcr.io/hanzoai/kafka:latest, 2 replicas)
Admin:     :9093 (HTTP, /healthz for K8s probes)
```
Dockerfile builds linux/amd64 via `GOARCH=amd64`. CI pushes to GHCR on every main push.

## Tests
- `test/e2e/` — E2E tests using Kafka CLI binaries (requires `KAFKA_BIN_DIR`)
- `test/cluster/` — Multi-instance tests (two gateways sharing same PubSub)
