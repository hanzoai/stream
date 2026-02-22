package pubsub

import (
	"fmt"

	log "github.com/hanzoai/stream/logging"
	"github.com/nats-io/nats.go"
)

// Client wraps Hanzo PubSub connection and Hanzo Stream context
type Client struct {
	NC *nats.Conn
	JS nats.JetStreamContext
}

// NewClient connects to Hanzo PubSub and obtains Hanzo Stream context
func NewClient(url string, opts ...nats.Option) (*Client, error) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Hanzo PubSub at %s: %w", url, err)
	}
	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("failed to get Hanzo Stream context: %w", err)
	}
	log.Info("Connected to Hanzo PubSub at %s", url)
	return &Client{NC: nc, JS: js}, nil
}

// Close closes the PubSub connection
func (c *Client) Close() {
	if c.NC != nil {
		c.NC.Close()
		log.Info("Hanzo PubSub connection closed")
	}
}
