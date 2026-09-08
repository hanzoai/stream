package e2e

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/hanzoai/kafka/logging"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestAReceivedFrameIsNotNews holds the broker to one line of restraint: a frame
// that arrived is a trace, not news.
//
// The broker used to name three APIs as the chatty ones and announce every other
// frame at Info. That is a guess about which calls a client repeats, and the
// guess was wrong about the one that matters. Measured in production: Metadata
// was 824 of 1200 lines — 69% of everything the process said — and eight minutes
// of it rotated the boot error explaining an outage out of the pod's history
// before anyone could read it.
//
// The test drives a real client through a real round trip and reads back what
// the broker said. Two assertions, and the second is why the first can be
// believed: at Info nothing may announce a frame, and at Debug those same frames
// must appear — otherwise a broker that logged nothing at all would pass.
func TestAReceivedFrameIsNotNews(t *testing.T) {
	atInfo := said(t, logging.INFO)
	atDebug := said(t, logging.DEBUG)

	if n := announced(atInfo, "[INFO]"); n != 0 {
		t.Fatalf("broker announced %d frame(s) at Info; a frame is a trace:\n%s",
			n, firstFew(atInfo, "[INFO]"))
	}
	if n := announced(atDebug, "[DEBUG]"); n == 0 {
		t.Fatal("no frame was traced even at Debug — the test is blind and its " +
			"Info assertion proves nothing")
	}
}

// said runs one produce/consume round trip at the given level and returns
// everything the broker wrote. The logger's sink is a process global written by
// every connection goroutine, so the buffer is guarded and both globals are put
// back the way they were found.
func said(t *testing.T, level string) string {
	t.Helper()

	buf := &guarded{}
	priorLevel, priorFlags := logging.LogLevel(), log.Flags()
	logging.SetLogLevel(level)
	log.SetOutput(buf)
	log.SetFlags(0)
	t.Cleanup(func() {
		log.SetOutput(discardTo(t))
		log.SetFlags(priorFlags)
		logging.SetLogLevel(priorLevel)
	})

	s := newStack(t)
	topic := "quiet-" + strings.ToLower(level)
	s.createTopic(t, topic)
	produceN(t, s.client(t), topic, 0, 5)
	consumeN(t, s.client(t,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart())), 5)

	return buf.String()
}

// discardTo restores logging to somewhere harmless for whatever runs next. The
// broker outlives the round trip by however long its goroutines take to notice
// the shutdown, and those writes must not land in the next test's buffer.
func discardTo(t *testing.T) *guarded {
	t.Helper()
	return &guarded{}
}

func announced(out, level string) int {
	n := 0
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, level) && strings.Contains(line, "Received ") {
			n++
		}
	}
	return n
}

func firstFew(out, level string) string {
	var keep []string
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, level) && strings.Contains(line, "Received ") {
			if keep = append(keep, line); len(keep) == 5 {
				break
			}
		}
	}
	return strings.Join(keep, "\n")
}

// guarded is a bytes.Buffer every connection goroutine may write at once.
type guarded struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (g *guarded) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.b.Write(p)
}

func (g *guarded) String() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.b.String()
}
