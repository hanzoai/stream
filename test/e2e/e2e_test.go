// Package e2e proves the Kafka-wire adaptor against a REAL Kafka client
// (franz-go) and the REAL native store (hanzoai/pubsub embedded in-process) —
// no external binaries, no fixtures, one `go test`. Every regression from the
// 2026-07 insights outage has a test here: sparse sequences, poison messages,
// huge offsets, out-of-range recovery, restart durability, big batches.
package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	broker "github.com/hanzoai/kafka/protocol"
	"github.com/hanzoai/kafka/pubsub"
	"github.com/hanzoai/kafka/types"
	psembed "github.com/hanzoai/pubsub/embed"
	natsio "github.com/nats-io/nats.go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// stack is one embedded pubsub + one broker over it + a raw verify client.
type stack struct {
	ps   *psembed.Server
	b    *broker.Broker
	addr string
	nc   *natsio.Conn
	js   natsio.JetStreamContext
}

func newStack(t *testing.T) *stack {
	t.Helper()
	ps, err := psembed.Open(psembed.Options{
		Host: "127.0.0.1", Port: -1, ServerName: "kafka-e2e", StoreDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("pubsub open: %v", err)
	}
	t.Cleanup(ps.Shutdown)

	s := &stack{ps: ps}
	s.startBroker(t)

	s.nc, err = natsio.Connect(ps.ClientURL())
	if err != nil {
		t.Fatalf("verify connect: %v", err)
	}
	t.Cleanup(s.nc.Close)
	s.js, err = s.nc.JetStream()
	if err != nil {
		t.Fatalf("verify jetstream: %v", err)
	}
	return s
}

// startBroker (re)starts the Kafka adaptor over the same pubsub — the restart
// tests use it to prove no broker-local state matters.
func (s *stack) startBroker(t *testing.T) {
	t.Helper()
	s.b = broker.NewBroker(&types.Configuration{
		PubSubUrl:      s.ps.ClientURL(),
		BrokerHost:     "127.0.0.1",
		BrokerPort:     0,
		NodeID:         1,
		StreamReplicas: 1,
		StorageType:    "file",
	})
	errc := make(chan error, 1)
	go func() { errc <- s.b.Serve() }()
	deadline := time.Now().Add(5 * time.Second)
	for s.b.Addr() == nil {
		select {
		case err := <-errc:
			t.Fatalf("broker serve: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("broker never bound")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.addr = s.b.Addr().String()
	t.Cleanup(s.b.Shutdown)
}

func (s *stack) createTopic(t *testing.T, topic string) {
	t.Helper()
	if err := s.b.PubSub.CreateTopicStreams(topic, 1, 1, natsio.FileStorage); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

func (s *stack) client(t *testing.T, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(s.addr),
		kgo.ProducerBatchMaxBytes(8 << 20),
		kgo.FetchMaxBytes(64 << 20),
		kgo.FetchMaxPartitionBytes(64 << 20),
	}, opts...)...)
	if err != nil {
		t.Fatalf("kgo client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produceN(t *testing.T, cl *kgo.Client, topic string, from, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := from; i < from+n; i++ {
		r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("record-%04d", i))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
}

// consumeN drains exactly n records, failing on timeout.
func consumeN(t *testing.T, cl *kgo.Client, n int) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			t.Fatalf("consumed %d/%d before timeout", len(out), n)
		}
		fetches.EachError(func(topic string, p int32, err error) {
			t.Fatalf("fetch error %s/%d: %v", topic, p, err)
		})
		out = append(out, fetches.Records()...)
	}
	return out
}

// TestProduceFetchRoundTrip: records survive the wire byte-for-byte, across
// compression codecs (the batch is stored and served verbatim; only its
// baseOffset is stamped, which the CRC does not cover).
func TestProduceFetchRoundTrip(t *testing.T) {
	s := newStack(t)
	for _, c := range []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
	} {
		t.Run(c.name, func(t *testing.T) {
			topic := "roundtrip-" + c.name
			s.createTopic(t, topic)
			prod := s.client(t, kgo.ProducerBatchCompression(c.codec))
			produceN(t, prod, topic, 0, 25)

			cons := s.client(t,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
			got := consumeN(t, cons, 25)
			seen := map[string]bool{}
			for _, r := range got {
				seen[string(r.Value)] = true
			}
			for i := 0; i < 25; i++ {
				if !seen[fmt.Sprintf("record-%04d", i)] {
					t.Fatalf("missing record %d", i)
				}
			}
		})
	}
}

// TestConsumeAcrossSequenceHoles: deleting stored messages leaves sequence
// holes (the production store's sequence space is sparse by design); the
// consumer must read straight across them.
func TestConsumeAcrossSequenceHoles(t *testing.T) {
	s := newStack(t)
	topic := "holes"
	s.createTopic(t, topic)
	prod := s.client(t)
	produceN(t, prod, topic, 0, 10)

	stream := pubsub.StreamName(topic, 0)
	info, err := s.js.StreamInfo(stream)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	// Punch holes in the middle third of the sequence range.
	first, last := info.State.FirstSeq, info.State.LastSeq
	for seq := first + 3; seq <= first+5 && seq < last; seq++ {
		if err := s.js.DeleteMsg(stream, seq); err != nil {
			t.Fatalf("delete seq %d: %v", seq, err)
		}
	}

	cons := s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	got := consumeN(t, cons, 7) // 10 - 3 deleted
	if len(got) != 7 {
		t.Fatalf("got %d records, want 7", len(got))
	}
}

// TestPoisonMessageSkipped: a raw publish onto the partition subject (any
// PubSub client can do this) must not wedge the partition — this exact shape
// took insights down: the broker served the garbage verbatim, librdkafka read
// its bytes as a giant batch length, and every record became unfetchable.
func TestPoisonMessageSkipped(t *testing.T) {
	s := newStack(t)
	topic := "poison"
	s.createTopic(t, topic)
	prod := s.client(t)
	produceN(t, prod, topic, 0, 5)

	// Foreign junk lands mid-stream: short, and long-but-not-a-batch.
	subj := pubsub.SubjectName(topic, 0)
	if _, err := s.js.Publish(subj, []byte("not a kafka batch")); err != nil {
		t.Fatalf("poison publish: %v", err)
	}
	junk := make([]byte, 512)
	for i := range junk {
		junk[i] = 0xFF
	}
	if _, err := s.js.Publish(subj, junk); err != nil {
		t.Fatalf("poison publish: %v", err)
	}

	// Produce must still work (bounds walk skips the junk)…
	produceN(t, prod, topic, 5, 5)

	// …and a fresh consumer gets all 10 real records, junk invisible.
	cons := s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	got := consumeN(t, cons, 10)
	for _, r := range got {
		if len(r.Value) < 7 || string(r.Value[:7]) != "record-" {
			t.Fatalf("consumer surfaced poison bytes: %q", r.Value)
		}
	}
}

// TestHugeCommittedOffsetAccepted: production offsets live in a sparse e18
// space. A "plausibility" ceiling on commits once rejected all of them; this
// pins the wire path (OffsetCommit with no generation) accepting the full
// int64 range and reading it back.
func TestHugeCommittedOffsetAccepted(t *testing.T) {
	s := newStack(t)
	topic := "e18"
	s.createTopic(t, topic)
	adm := kadm.NewClient(s.client(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	huge := int64(8872783363889192000)
	var os kadm.Offsets
	os.AddOffset(topic, 0, huge, -1)
	resp, err := adm.CommitOffsets(ctx, "e18-group", os)
	if err != nil {
		t.Fatalf("commit huge offset: %v", err)
	}
	if err := resp.Error(); err != nil {
		t.Fatalf("commit huge offset (partition error): %v", err)
	}
	fetched, err := adm.FetchOffsets(ctx, "e18-group")
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	got, ok := fetched.Lookup(topic, 0)
	if !ok || got.At != huge {
		t.Fatalf("committed offset round-trip: got %+v want %d", got, huge)
	}
}

// TestOutOfRangeResets: a committed offset past the log (purge, reset,
// retention) must answer OFFSET_OUT_OF_RANGE so the client's reset policy
// engages — the old empty-response stall left consumers polling a dead
// position forever.
func TestOutOfRangeResets(t *testing.T) {
	s := newStack(t)
	topic := "oor"
	s.createTopic(t, topic)
	prod := s.client(t)
	produceN(t, prod, topic, 0, 5)

	group := "oor-group"
	adm := kadm.NewClient(s.client(t))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var os kadm.Offsets
	os.AddOffset(topic, 0, 4_000_000, -1) // far past the high watermark
	if _, err := adm.CommitOffsets(ctx, group, os); err != nil {
		t.Fatalf("seed bogus commit: %v", err)
	}

	cons := s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	got := consumeN(t, cons, 5)
	if len(got) != 5 {
		t.Fatalf("reset consumer got %d records, want 5", len(got))
	}
}

// TestGroupResumeAcrossBrokerRestart: committed offsets are the durable state
// (KV bucket on the pubsub), the gateway itself is stateless — a new broker
// over the same store resumes exactly where the group left off.
func TestGroupResumeAcrossBrokerRestart(t *testing.T) {
	s := newStack(t)
	topic := "resume"
	s.createTopic(t, topic)
	prod := s.client(t)
	produceN(t, prod, topic, 0, 6)

	group := "resume-group"
	c1 := s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit())
	first := consumeN(t, c1, 6)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := c1.CommitRecords(ctx, first...); err != nil {
		t.Fatalf("commit: %v", err)
	}
	c1.Close()

	// Kill the broker, produce nothing, start a fresh one on the same store.
	s.b.Shutdown()
	s.startBroker(t)
	prod2 := s.client(t)
	produceN(t, prod2, topic, 6, 3)

	c2 := s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	rest := consumeN(t, c2, 3)
	for _, r := range rest {
		for _, f := range first {
			if string(r.Value) == string(f.Value) {
				t.Fatalf("record %q reconsumed: committed offsets did not survive restart", r.Value)
			}
		}
	}
}

// TestLargeRecordRoundTrip: a >1MiB record crosses both encoders intact (the
// response buffer once grew by a fixed 64KiB step and silently truncated
// anything bigger).
func TestLargeRecordRoundTrip(t *testing.T) {
	s := newStack(t)
	topic := "big"
	s.createTopic(t, topic)

	big := make([]byte, 1<<20+1<<19) // 1.5 MiB
	for i := range big {
		big[i] = byte(i * 31)
	}
	prod := s.client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := prod.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: big}).FirstErr(); err != nil {
		t.Fatalf("produce big: %v", err)
	}

	cons := s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	got := consumeN(t, cons, 1)
	if len(got[0].Value) != len(big) {
		t.Fatalf("size mismatch: got %d want %d", len(got[0].Value), len(big))
	}
	for i := range big {
		if got[0].Value[i] != big[i] {
			t.Fatalf("byte %d differs", i)
		}
	}
}

// TestTwoBrokersOnePubSub: two gateway instances over one store see one log —
// the stateless-gateway claim, on the wire.
func TestTwoBrokersOnePubSub(t *testing.T) {
	s := newStack(t)
	topic := "shared"
	s.createTopic(t, topic)

	// Second broker over the same pubsub.
	s2 := &stack{ps: s.ps}
	s2.startBroker(t)

	prod := s.client(t) // produce through broker 1
	produceN(t, prod, topic, 0, 8)

	cons := s2.client(t, // consume through broker 2
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if got := consumeN(t, cons, 8); len(got) != 8 {
		t.Fatalf("broker 2 served %d of 8 records produced via broker 1", len(got))
	}
}
