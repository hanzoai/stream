package protocol

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"time"

	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/pubsub"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
)

// Broker represents a Hanzo Kafka broker instance
type Broker struct {
	Config         *types.Configuration
	PubSub         *pubsub.Client
	ShutDownSignal chan bool
}

// NewBroker creates a new Broker instance with the provided configuration
func NewBroker(config *types.Configuration) *Broker {
	return &Broker{
		Config:         config,
		ShutDownSignal: make(chan bool),
	}
}

// Startup initializes the broker, connects to NATS, and listens for incoming Kafka client connections
func (b *Broker) Startup() {
	var err error

	b.PubSub, err = pubsub.NewClient(b.Config.PubSubUrl)
	if err != nil {
		log.Panic("Failed to connect to Hanzo PubSub: %v", err)
	}

	err = b.PubSub.EnsureOffsetBucket()
	if err != nil {
		log.Panic("Failed to ensure offset bucket: %v", err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Config.BrokerPort))
	if err != nil {
		log.Error("Error starting server: %v", err)
		os.Exit(1)
	}
	defer listener.Close()

	log.Info("Hanzo Kafka listening on port %d (PubSub: %s)", b.Config.BrokerPort, b.Config.PubSubUrl)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Error("Error accepting connection: %v", err)
			continue
		}
		go b.HandleConnection(conn)
	}
}

// HandleConnection processes incoming requests from a client connection
func (b *Broker) HandleConnection(conn net.Conn) {
	defer conn.Close()
	connectionAddr := conn.RemoteAddr().String()
	log.Info("Connection established with %s", connectionAddr)

	for {
		startTime := time.Now()
		lengthBuffer := make([]byte, 4)
		_, err := io.ReadFull(conn, lengthBuffer)
		if err != nil {
			log.Info("failed to read request's length. Error: %v ", err)
			return
		}
		length := serde.Encoding.Uint32(lengthBuffer)
		buffer := make([]byte, length+4)
		copy(buffer, lengthBuffer)
		_, err = io.ReadFull(conn, buffer[4:])
		if err != nil {
			if err.Error() != "EOF" {
				log.Error("Error reading from connection: %v", err)
			}
			break
		}
		req := serde.ParseHeader(buffer, connectionAddr)
		apiKeyHandler := b.APIDispatcher(req.RequestAPIKey)
		log.Info("Received RequestAPIKey: %v | RequestAPIVersion: %v | CorrelationID: %v | Length: %v | BodyLen: %v", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, length, len(req.Body))

		response, handlerErr := b.safeHandle(apiKeyHandler, req)
		if handlerErr != nil {
			log.Error("Panic in handler %v (apiKey=%d, version=%d): %v", apiKeyHandler.Name, req.RequestAPIKey, req.RequestAPIVersion, handlerErr)
			break
		}

		_, err = conn.Write(response)
		if err != nil {
			log.Error("Error writing to connection: %v", err)
			break
		}
		d := time.Since(startTime)
		log.Trace("handleConnection Iteration took %v", d)
	}
	log.Debug("Connection with %s closed.", connectionAddr)
}

// safeHandle calls the API handler with panic recovery so a single bad request doesn't crash the process.
func (b *Broker) safeHandle(h APIKeyHandler, req types.Request) (response []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v\n%s", r, debug.Stack())
		}
	}()
	return h.Handler(req), nil
}

// Shutdown gracefully shuts down the broker
func (b *Broker) Shutdown() {
	close(b.ShutDownSignal)
	if b.PubSub != nil {
		b.PubSub.Close()
	}
	log.Info("Hanzo Kafka shut down")
}
