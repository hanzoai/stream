package pubsub

import (
	"fmt"
	"strings"
	"time"

	log "github.com/hanzoai/kafka/logging"
	"github.com/nats-io/nats.go"
)

// StreamName returns the Hanzo Kafka name for a topic+partition
func StreamName(topic string, partition uint32) string {
	return fmt.Sprintf("kafka-%s-%d", topic, partition)
}

// SubjectName returns the PubSub subject for a topic+partition.
// Must match the subject pattern configured on the JetStream stream:
// StreamName(topic, partition) + ".data"
func SubjectName(topic string, partition uint32) string {
	return fmt.Sprintf("%s.data", StreamName(topic, partition))
}

// ParseStreamName extracts topic and partition from a stream name
func ParseStreamName(name string) (topic string, partition uint32, ok bool) {
	if !strings.HasPrefix(name, "kafka-") {
		return "", 0, false
	}
	rest := name[6:]
	lastDash := strings.LastIndex(rest, "-")
	if lastDash == -1 {
		return "", 0, false
	}
	topic = rest[:lastDash]
	_, err := fmt.Sscanf(rest[lastDash+1:], "%d", &partition)
	return topic, partition, err == nil
}

// CreateTopicStreams creates N Hanzo Kafka streams for a topic (one per partition)
func (c *Client) CreateTopicStreams(topic string, numPartitions uint32, replicas int, storage nats.StorageType) error {
	if replicas < 1 {
		replicas = 1
	}
	for i := uint32(0); i < numPartitions; i++ {
		cfg := &nats.StreamConfig{
			Name:     StreamName(topic, i),
			Subjects: []string{SubjectName(topic, i)},
			Replicas: replicas,
			Storage:  storage,
		}
		_, err := c.JS.AddStream(cfg)
		if err != nil {
			return fmt.Errorf("failed to create stream for %s partition %d: %w", topic, i, err)
		}
		log.Info("Created stream %s", cfg.Name)
	}
	return nil
}

// TopicExists checks if at least partition 0 stream exists for this topic
func (c *Client) TopicExists(topic string) bool {
	_, err := c.JS.StreamInfo(StreamName(topic, 0))
	return err == nil
}

// GetTopicPartitionCount counts partition streams for a topic
func (c *Client) GetTopicPartitionCount(topic string) (uint32, error) {
	var count uint32
	for {
		_, err := c.JS.StreamInfo(StreamName(topic, count))
		if err != nil {
			break
		}
		count++
	}
	return count, nil
}

// GetStreamInfo returns Hanzo Kafka info for a topic+partition
func (c *Client) GetStreamInfo(topic string, partition uint32) (*nats.StreamInfo, error) {
	return c.JS.StreamInfo(StreamName(topic, partition))
}

// Publish publishes record batch bytes to a partition, returns sequence (= Kafka offset + 1)
func (c *Client) Publish(topic string, partition uint32, data []byte) (uint64, error) {
	ack, err := c.JS.Publish(SubjectName(topic, partition), data)
	if err != nil {
		return 0, err
	}
	return ack.Sequence, nil
}

// GetMessage retrieves a message by stream sequence number
func (c *Client) GetMessage(topic string, partition uint32, sequence uint64) (*nats.RawStreamMsg, error) {
	return c.JS.GetMsg(StreamName(topic, partition), sequence)
}

// StoredMsg is a stored partition message with its stream sequence.
type StoredMsg struct {
	Sequence uint64
	Data     []byte
}

// NextMessage returns the first stored message with sequence >= seq, or nil if
// the partition holds none at or past it. This — not sequence arithmetic — is
// the primitive for reading a partition: Hanzo PubSub assigns sequences that
// are only guaranteed monotonic, not dense (deletes leave holes and the
// production store allocates from a sparse space), so "seq+1" addresses
// nothing. An ephemeral by-start-sequence subscription asks the stream itself
// for the next real message.
func (c *Client) NextMessage(topic string, partition uint32, seq uint64) (*StoredMsg, error) {
	if seq < 1 {
		seq = 1
	}
	sub, err := c.JS.SubscribeSync(SubjectName(topic, partition),
		nats.StartSequence(seq), nats.AckNone(), nats.MaxDeliver(1))
	if err != nil {
		return nil, err
	}
	defer sub.Unsubscribe()
	msg, err := sub.NextMsg(500 * time.Millisecond)
	if err != nil {
		return nil, nil // nothing at or past seq
	}
	meta, err := msg.Metadata()
	if err != nil {
		return nil, err
	}
	return &StoredMsg{Sequence: meta.Sequence.Stream, Data: msg.Data}, nil
}

// ListTopics returns all unique topic names from kafka-* streams
func (c *Client) ListTopics() ([]string, error) {
	topicSet := make(map[string]bool)
	for name := range c.JS.StreamNames() {
		topic, _, ok := ParseStreamName(name)
		if ok {
			topicSet[topic] = true
		}
	}
	var topics []string
	for t := range topicSet {
		topics = append(topics, t)
	}
	return topics, nil
}
