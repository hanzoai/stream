package pubsub

import (
	"fmt"
	"strconv"

	log "github.com/hanzoai/kafka/logging"
	"github.com/nats-io/nats.go"
)

const offsetBucketName = "kafka-consumer-offsets"

// EnsureOffsetBucket creates the KV bucket for consumer group offsets if it doesn't exist
func (c *Client) EnsureOffsetBucket() error {
	_, err := c.JS.KeyValue(offsetBucketName)
	if err != nil {
		_, err = c.JS.CreateKeyValue(&nats.KeyValueConfig{
			Bucket:  offsetBucketName,
			Storage: nats.FileStorage,
		})
		if err != nil {
			return fmt.Errorf("failed to create offset KV bucket: %w", err)
		}
		log.Info("Created KV bucket %s", offsetBucketName)
	}
	return nil
}

func offsetKey(group, topic string, partition uint32) string {
	return fmt.Sprintf("%s.%s.%d", group, topic, partition)
}

// CommitOffset durably stores the committed offset for a consumer group.
// Any non-negative int64 is a valid offset: offsets are stamped from the
// partition's own log positions, which on Hanzo PubSub live in a sparse space
// that reaches e18. A magnitude ceiling ("reject absurdly large") once lived
// here and rejected every real commit — offsets are opaque, never plausible.
func (c *Client) CommitOffset(group, topic string, partition uint32, offset int64) error {
	if offset < 0 {
		return fmt.Errorf("negative offset: %d", offset)
	}
	kv, err := c.JS.KeyValue(offsetBucketName)
	if err != nil {
		return err
	}
	_, err = kv.PutString(offsetKey(group, topic, partition), strconv.FormatInt(offset, 10))
	return err
}

// GetCommittedOffset returns the committed offset, or -1 if none.
func (c *Client) GetCommittedOffset(group, topic string, partition uint32) (int64, error) {
	kv, err := c.JS.KeyValue(offsetBucketName)
	if err != nil {
		return -1, nil
	}
	entry, err := kv.Get(offsetKey(group, topic, partition))
	if err != nil {
		return -1, nil
	}
	offset, err := strconv.ParseInt(string(entry.Value()), 10, 64)
	if err != nil || offset < 0 {
		return -1, nil
	}
	return offset, nil
}

// PurgeAllOffsets deletes all committed offsets from the KV store
func (c *Client) PurgeAllOffsets() error {
	kv, err := c.JS.KeyValue(offsetBucketName)
	if err != nil {
		return err
	}
	keys, err := kv.Keys()
	if err != nil {
		return err
	}
	for _, key := range keys {
		log.Info("Purging offset key: %s", key)
		_ = kv.Delete(key)
	}
	return nil
}
