# Hanzo Stream

Kafka wire protocol adapter for Hanzo PubSub (NATS JetStream). Accepts standard Kafka clients and translates requests to NATS underneath, allowing any Kafka producer/consumer to work against Hanzo infrastructure without code changes.

## Features

- Topic creation, produce, and consume via the Kafka binary protocol
- Cluster mode with Raft consensus and Serf membership
- Compression support
- Multiple log segments per partition
- Compatible with standard Kafka CLI tools and client libraries

## Quick Start

Start a single-node broker on port 9092:

```bash
go run main.go --bootstrap --node-id 1
```

Test with any Kafka client:

```bash
# Create topic
kafka-topics.sh --create --topic events --bootstrap-server localhost:9092

# Produce
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic events

# Consume
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic events --from-beginning
```

## Cluster Mode

Cluster mode uses Raft for distributed state and HashiCorp Serf for membership management.

```bash
# Bootstrap first node
go run main.go --bootstrap --node-id 1 --serf-addr 127.0.0.1:3331

# Join additional nodes
go run main.go --node-id 2 --broker-port 9093 --raft-addr localhost:2222 --serf-addr 127.0.0.1:3332 --serf-join "127.0.0.1:3331"
go run main.go --node-id 3 --broker-port 9094 --raft-addr localhost:2223 --serf-addr 127.0.0.1:3333 --serf-join "127.0.0.1:3331"
```

## Kubernetes

In Hanzo infrastructure, Stream runs as a deployment in the `hanzo` namespace and connects to NATS via `--pubsub-url nats://pubsub.hanzo.svc:4222`.

Two instances are typically deployed:

| Deployment | Service | Purpose |
|-----------|---------|---------|
| `insights-kafka` | `insights-kafka:9092` | Dedicated to Insights pipeline |
| `stream` | `stream:9092` | General purpose |

## Running Tests

Tests use Kafka CLI tools for end-to-end verification:

```bash
export KAFKA_BIN_DIR=/path/to/kafka_2.13-3.9.0/bin
go test -v ./...
```

## Credits

Based on [MonKafka](https://github.com/cefboud/monkafka) by cefboud. Cluster mode inspired by [Jocko](https://github.com/travisjeffery/jocko).

## License

MIT
