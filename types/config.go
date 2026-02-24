package types

// Configuration represents the Hanzo Kafka broker configuration
type Configuration struct {
	// Hanzo PubSub connection
	PubSubUrl      string // PubSub server URL (e.g. "nats://localhost:4222")
	PubSubCredFile string // Optional PubSub credentials file

	// Kafka listener
	BrokerHost string `kafka:"CompactString"`
	BrokerPort int

	// Hanzo Stream defaults
	StreamReplicas int    // Number of replicas for Hanzo Stream (default 1)
	StorageType    string // "file" or "memory" (default "file")

	// Admin HTTP server
	AdminPort int // HTTP admin/monitoring port (default 9093, 0 to disable)

	// Broker identity
	NodeID int
}
