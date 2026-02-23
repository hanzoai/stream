package protocol

import (
	"fmt"
	"io"
	"net"
	"os"
	"runtime/debug"
	"sync"
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
	partitionMu    sync.Map // map[string]*sync.Mutex keyed by "topic-partition"
}

// partitionLock returns a mutex for a topic+partition, ensuring safe concurrent offset assignment.
func (b *Broker) partitionLock(topic string, partition uint32) *sync.Mutex {
	key := fmt.Sprintf("%s-%d", topic, partition)
	v, _ := b.partitionMu.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// nextKafkaOffset computes the next Kafka record offset for a partition by reading
// the last stored RecordBatch header. Must be called while holding the partition lock.
func (b *Broker) nextKafkaOffset(topic string, partition uint32) (int64, error) {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil {
		return 0, err
	}
	if info.State.Msgs == 0 {
		return 0, nil
	}
	lastMsg, err := b.PubSub.GetMessage(topic, partition, info.State.LastSeq)
	if err != nil {
		return 0, fmt.Errorf("read last message seq %d: %w", info.State.LastSeq, err)
	}
	header, err := ParseRecordBatchHeader(lastMsg.Data)
	if err != nil {
		return 0, fmt.Errorf("parse last record batch: %w", err)
	}
	return header.BaseOffset + header.OffsetCount(), nil
}

// kafkaHighWatermark returns the next Kafka offset after the last stored record.
func (b *Broker) kafkaHighWatermark(topic string, partition uint32) uint64 {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil || info.State.Msgs == 0 {
		return 0
	}
	lastMsg, err := b.PubSub.GetMessage(topic, partition, info.State.LastSeq)
	if err != nil {
		return 0
	}
	header, err := ParseRecordBatchHeader(lastMsg.Data)
	if err != nil {
		return 0
	}
	return uint64(header.BaseOffset + header.OffsetCount())
}

// kafkaLogStartOffset returns the Kafka offset of the first stored record.
func (b *Broker) kafkaLogStartOffset(topic string, partition uint32) uint64 {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil || info.State.Msgs == 0 {
		return 0
	}
	firstMsg, err := b.PubSub.GetMessage(topic, partition, info.State.FirstSeq)
	if err != nil {
		return 0
	}
	header, err := ParseRecordBatchHeader(firstMsg.Data)
	if err != nil {
		return 0
	}
	return uint64(header.BaseOffset)
}

// findSequenceForOffset uses binary search to find the NATS sequence
// containing the given Kafka offset. Returns 0 if not found.
func (b *Broker) findSequenceForOffset(topic string, partition uint32, offset uint64) uint64 {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil || info.State.Msgs == 0 {
		return 0
	}

	lo := info.State.FirstSeq
	hi := info.State.LastSeq

	for lo <= hi {
		mid := lo + (hi-lo)/2
		msg, err := b.PubSub.GetMessage(topic, partition, mid)
		if err != nil {
			return 0
		}
		header, err := ParseRecordBatchHeader(msg.Data)
		if err != nil {
			return 0
		}

		batchStart := uint64(header.BaseOffset)
		batchEnd := batchStart + uint64(header.LastOffsetDelta)

		if offset < batchStart {
			hi = mid - 1
		} else if offset > batchEnd {
			lo = mid + 1
		} else {
			return mid
		}
	}

	return 0
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
