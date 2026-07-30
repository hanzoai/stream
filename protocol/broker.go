package protocol

import (
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"time"

	log "github.com/hanzoai/kafka/logging"
	"github.com/hanzoai/kafka/pubsub"
	"github.com/hanzoai/kafka/serde"
	"github.com/hanzoai/kafka/types"
)

// maxRequestSize bounds a single Kafka request frame (128 MiB).
const maxRequestSize = 128 << 20

// Broker represents a Hanzo Kafka broker instance
type Broker struct {
	Config         *types.Configuration
	PubSub         *pubsub.Client
	ShutDownSignal chan bool
	listener       net.Listener
	partitionMu    sync.Map // map[string]*sync.Mutex keyed by "topic-partition"
	readHints      sync.Map // map[string]readHint keyed by "topic-partition"
	shutdownOnce   sync.Once
}

// partitionLock returns a mutex for a topic+partition, ensuring safe concurrent offset assignment.
func (b *Broker) partitionLock(topic string, partition uint32) *sync.Mutex {
	key := fmt.Sprintf("%s-%d", topic, partition)
	v, _ := b.partitionMu.LoadOrStore(key, &sync.Mutex{})
	return v.(*sync.Mutex)
}

// Log positions on a partition are read from the stored RecordBatch headers,
// never derived from PubSub sequences: Hanzo PubSub sequences are only
// monotonic, not dense (the production store allocates from a sparse e18
// space, and deletes leave holes), so sequence arithmetic addresses nothing.
// Messages that do not walk as valid batch chains are skipped everywhere —
// the stream subject is reachable by any PubSub client, and one raw publish
// served verbatim poisons every consumer that fetches it (the 2026-07
// insights outage: unfetchable records until the stream was purged).

// boundsScanLimit caps how many stored messages a poison-recovery scan will
// read before treating the partition as empty. Valid batches are found in one
// read; only a partition whose edges are wall-to-wall foreign messages pays
// more, and unbounded reads on the fetch path would let one bad stream stall
// the broker.
const boundsScanLimit = 10000

// partitionBounds derives the Kafka log bounds: logStart is the first offset
// of the first valid record set, next is one past the last offset of the last
// valid one (the high watermark, and the offset produce stamps next).
func (b *Broker) partitionBounds(topic string, partition uint32) (logStart, next int64, err error) {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil {
		return 0, 0, err
	}
	if info.State.Msgs == 0 {
		return 0, 0, nil
	}

	// Head: first valid record set at or after FirstSeq.
	seq := info.State.FirstSeq
	var first *int64
	for i := 0; i < boundsScanLimit; i++ {
		msg, err := b.PubSub.NextMessage(topic, partition, seq)
		if err != nil || msg == nil {
			break
		}
		if f, _, ok := batchSpan(msg.Data); ok {
			first = &f
			break
		}
		seq = msg.Sequence + 1
	}
	if first == nil {
		return 0, 0, nil // nothing but foreign messages: an empty log
	}

	// Tail: the last stored message is addressable directly. If it is foreign,
	// fall back to a bounded forward scan for the last valid record set.
	if msg, err := b.PubSub.GetMessage(topic, partition, info.State.LastSeq); err == nil {
		if _, last, ok := batchSpan(msg.Data); ok {
			return *first, last + 1, nil
		}
	}
	log.Warn("partition %s/%d tail is not a record batch; scanning", topic, partition)
	next = *first
	seq = info.State.FirstSeq
	for i := 0; i < boundsScanLimit; i++ {
		msg, err := b.PubSub.NextMessage(topic, partition, seq)
		if err != nil || msg == nil {
			break
		}
		if _, last, ok := batchSpan(msg.Data); ok {
			next = last + 1
		}
		seq = msg.Sequence + 1
	}
	return *first, next, nil
}

// readHint remembers, per partition, where the last served fetch left off, so
// a sequential consumer costs one addressed read per fetch instead of a
// search.
type readHint struct {
	offset int64  // next Kafka offset the consumer will ask for
	seq    uint64 // first sequence that can hold it
}

// findRecordSet returns the stored record set whose span contains offset, or
// the first valid one past it (sequence holes and skipped foreign messages
// leave gaps in the offset space). Returns nil when nothing at or past offset
// exists. Callers have already bounds-checked offset, so nil means the data
// the bounds promised could not be read — serve empty and let the client
// retry, never serve bytes that did not walk.
func (b *Broker) findRecordSet(topic string, partition uint32, offset int64) *pubsub.StoredMsg {
	info, err := b.PubSub.GetStreamInfo(topic, partition)
	if err != nil || info.State.Msgs == 0 {
		return nil
	}

	key := fmt.Sprintf("%s-%d", topic, partition)
	if v, ok := b.readHints.Load(key); ok {
		h := v.(readHint)
		if h.offset == offset {
			if msg := b.probeFrom(topic, partition, h.seq, offset); msg != nil {
				b.rememberHint(key, offset, msg)
				return msg
			}
		}
	}

	// Binary search over the sequence space. A probe at mid returns the first
	// stored valid record set at or after mid together with its real sequence,
	// so sparse sequences and holes still halve the interval each round.
	lo, hi := info.State.FirstSeq, info.State.LastSeq
	for lo <= hi {
		mid := lo + (hi-lo)/2
		msg := b.probeFrom(topic, partition, mid, 0)
		if msg == nil {
			// Nothing valid at or after mid: the target, if any, is below.
			if mid == 0 {
				break
			}
			hi = mid - 1
			continue
		}
		f, l, _ := batchSpan(msg.Data)
		switch {
		case offset >= f && offset <= l:
			b.rememberHint(key, offset, msg)
			return msg
		case offset < f:
			if msg.Sequence <= lo {
				// Everything from lo on starts past offset: this is the first
				// record set after the gap that swallowed it.
				b.rememberHint(key, offset, msg)
				return msg
			}
			hi = min(mid-1, msg.Sequence-1)
		default: // offset > l
			lo = msg.Sequence + 1
		}
	}
	return nil
}

// probeFrom returns the first valid record set at or after seq, skipping up to
// boundsScanLimit foreign messages. wantOffset is a fast-path hint: when the
// exact sequence holds the batch (the dense steady state), one direct read
// answers without a subscription.
func (b *Broker) probeFrom(topic string, partition uint32, seq uint64, wantOffset int64) *pubsub.StoredMsg {
	if raw, err := b.PubSub.GetMessage(topic, partition, seq); err == nil {
		if f, l, ok := batchSpan(raw.Data); ok && (wantOffset == 0 || (wantOffset >= f && wantOffset <= l)) {
			return &pubsub.StoredMsg{Sequence: seq, Data: raw.Data}
		}
	}
	for i := 0; i < boundsScanLimit; i++ {
		msg, err := b.PubSub.NextMessage(topic, partition, seq)
		if err != nil || msg == nil {
			return nil
		}
		if _, _, ok := batchSpan(msg.Data); ok {
			return msg
		}
		seq = msg.Sequence + 1
	}
	return nil
}

func (b *Broker) rememberHint(key string, offset int64, msg *pubsub.StoredMsg) {
	_, last, ok := batchSpan(msg.Data)
	if !ok {
		return
	}
	b.readHints.Store(key, readHint{offset: last + 1, seq: msg.Sequence + 1})
}

// NewBroker creates a new Broker instance with the provided configuration
func NewBroker(config *types.Configuration) *Broker {
	return &Broker{
		Config:         config,
		ShutDownSignal: make(chan bool),
	}
}

// Startup initializes the broker and blocks serving Kafka clients. It is the
// standalone entrypoint (main.go): any startup failure is fatal.
func (b *Broker) Startup() {
	if err := b.Serve(); err != nil {
		log.Panic("Hanzo Kafka failed: %v", err)
	}
}

// Serve is the embed-safe form of Startup: it connects to PubSub, starts the
// admin server, and runs the Kafka accept loop, RETURNING errors instead of
// exiting the process so a host binary (hanzoai/cloud) can run the adaptor
// in-process. It stores the listener so Shutdown can stop it; a clean Shutdown
// closes ShutDownSignal + the listener, so Accept fails and Serve returns nil.
// Run it in a goroutine when embedding.
func (b *Broker) Serve() error {
	var err error

	b.PubSub, err = pubsub.NewClient(b.Config.PubSubUrl)
	if err != nil {
		return fmt.Errorf("connect pubsub: %w", err)
	}

	if err = b.PubSub.EnsureOffsetBucket(); err != nil {
		return fmt.Errorf("ensure offset bucket: %w", err)
	}

	b.StartAdmin()

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", b.Config.BrokerPort))
	if err != nil {
		return fmt.Errorf("listen :%d: %w", b.Config.BrokerPort, err)
	}
	b.listener = ln
	// Port 0 asked the kernel to pick: publish the real port so Metadata and
	// FindCoordinator advertise an address clients can actually dial.
	if b.Config.BrokerPort == 0 {
		b.Config.BrokerPort = ln.Addr().(*net.TCPAddr).Port
	}

	log.Info("Hanzo Kafka listening on port %d (PubSub: %s)", b.Config.BrokerPort, b.Config.PubSubUrl)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.ShutDownSignal:
				return nil
			default:
				log.Error("Error accepting connection: %v", err)
				continue
			}
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
		if length > maxRequestSize {
			// A framed length this large is not Kafka — it is a stray protocol
			// (an HTTP probe reads as a ~1GB frame) or garbage. Allocating it
			// would let any port scan OOM the broker.
			log.Error("request frame of %d bytes from %s exceeds %d; closing", length, connectionAddr, maxRequestSize)
			return
		}
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
		switch req.RequestAPIKey {
		case listOffsetsKey, fetchKey, heartbeatKey:
			log.Debug("Received %v v%d corr=%d from %s len=%d body=%d", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, connectionAddr, length, len(req.Body))
		default:
			log.Info("Received %v v%d corr=%d from %s len=%d body=%d", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, connectionAddr, length, len(req.Body))
		}

		response, handlerErr := b.safeHandle(apiKeyHandler, req)
		if handlerErr != nil {
			log.Error("Panic in handler %v (apiKey=%d, version=%d): %v", apiKeyHandler.Name, req.RequestAPIKey, req.RequestAPIVersion, handlerErr)
			break
		}

		log.Debug("Response %v v%d corr=%d len=%d", apiKeyHandler.Name, req.RequestAPIVersion, req.CorrelationID, len(response))
		_, err = conn.Write(response)
		if err != nil {
			log.Error("Error writing to connection: %v", err)
			break
		}
		d := time.Since(startTime)
		log.Trace("handleConnection Iteration took %v", d)
	}
	log.Info("Connection with %s closed.", connectionAddr)
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

// Addr returns the listener address once Serve has bound it, else nil.
func (b *Broker) Addr() net.Addr {
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// Shutdown gracefully shuts down the broker. Idempotent: hosts and tests may
// tear down on overlapping paths, and a second call must be a no-op, not a
// closed-channel panic.
func (b *Broker) Shutdown() {
	b.shutdownOnce.Do(func() {
		close(b.ShutDownSignal)
		if b.listener != nil {
			b.listener.Close()
		}
		if b.PubSub != nil {
			b.PubSub.Close()
		}
		log.Info("Hanzo Kafka shut down")
	})
}
