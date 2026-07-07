package protocol

import (
	"testing"

	"github.com/hanzoai/stream/types"
)

// TestServeReturnsErrorOnUnreachablePubSub proves the embed-safety property: on
// a startup failure Serve RETURNS an error rather than calling log.Panic /
// os.Exit, so a host binary (hanzoai/cloud) can run the adaptor in-process
// without the whole process dying. pubsub.NewClient wraps nats.Connect, which
// fails fast against an unreachable URL, so Serve returns before it ever binds
// the Kafka port or starts the admin server (AdminPort 0 disables it anyway).
func TestServeReturnsErrorOnUnreachablePubSub(t *testing.T) {
	b := NewBroker(&types.Configuration{
		PubSubUrl:  "nats://127.0.0.1:1", // nothing listens on port 1
		BrokerPort: 0,
		AdminPort:  0,
	})
	if err := b.Serve(); err == nil {
		t.Fatal("Serve returned nil for an unreachable PubSub; want an error")
	}
}
