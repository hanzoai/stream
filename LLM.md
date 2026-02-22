# Hanzo Kafka — LLM.md

## Overview
Hanzo Kafka is a thin, stateless Kafka wire protocol gateway that translates Kafka client requests to Hanzo Stream (JetStream) operations against Hanzo PubSub. Standard Kafka clients connect on `:9092`; all storage and replication is delegated to Hanzo Stream.

## Architecture
```
Kafka Client → TCP :9092 → Hanzo Kafka (protocol translation) → Hanzo PubSub (Hanzo Stream)
```

**Stateless broker**: All state lives in Hanzo Stream. Multiple Hanzo Kafka instances can point at the same PubSub cluster.

## Kafka-to-Hanzo Stream Mapping
| Kafka Concept | Hanzo Stream Equivalent |
|---|---|
| Topic `foo`, Partition N | Stream `kafka-foo-N`, Subject `kafka.foo.N` |
| Produce | `Publish("kafka.foo.0", recordBatchBytes)` → seq = offset+1 |
| Fetch at offset | `GetMsg(streamName, offset+1)` (PubSub 1-based, Kafka 0-based) |
| Consumer group offsets | KV bucket `kafka-consumer-offsets`, key `{group}.{topic}.{partition}` |
| Create topic (N parts) | N calls to `AddStream()` |
| Metadata | `StreamInfo()` per partition stream |

## Critical: Offset Translation
```
Kafka offset 0  ↔  PubSub sequence 1
Kafka offset N  ↔  PubSub sequence N+1
Produce: seq = Publish(); return seq - 1
Fetch:   msg = GetMsg(offset + 1)
```

## Module Structure
```
github.com/hanzoai/kafka
├── main.go              # CLI entry point (cobra)
├── pubsub/              # Hanzo PubSub client wrapper
│   ├── client.go        # Connection + Hanzo Stream context
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
- **KV bucket for consumer offsets** instead of __consumer-offsets topic + custom serialization
- **No local storage, no Raft, no Serf** — pure protocol translation

## Dependencies
- `github.com/nats-io/nats.go` — Hanzo PubSub client + Hanzo Stream
- `github.com/spf13/cobra` — CLI
- Compression: klauspost/compress (zstd), pierrec/lz4, eapache/go-xerial-snappy
- Zero hashicorp dependencies

## Running
```bash
# Start Hanzo PubSub with Hanzo Stream
nats-server --jetstream

# Start Hanzo Kafka
go run main.go --pubsub-url nats://localhost:4222 --port 9092

# Use standard Kafka CLI tools
kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning
```

## Deployment (hanzo-k8s)
```
Namespace: hanzo
PubSub:    pubsub.hanzo.svc:4222  (NATS with Hanzo Stream enabled)
Kafka:     kafka.hanzo.svc:9092   (ghcr.io/hanzoai/kafka)
```

## Tests
- `test/e2e/` — E2E tests using Kafka CLI binaries (requires `KAFKA_BIN_DIR`)
- `test/cluster/` — Multi-instance tests (two brokers sharing same PubSub)
