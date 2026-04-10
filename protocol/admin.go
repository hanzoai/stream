package protocol

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"

	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/pubsub"
)

// AdminStatus is the response for GET /status
type AdminStatus struct {
	Service    string `json:"service"`
	KafkaPort  int    `json:"kafka_port"`
	AdminPort  int    `json:"admin_port"`
	PubSubURL  string `json:"pubsub_url"`
	PubSubConn string `json:"pubsub_connected"`
	NodeID     int    `json:"node_id"`
}

// TopicInfo is the response for topic listing
type TopicInfo struct {
	Name       string          `json:"name"`
	Partitions []PartitionInfo `json:"partitions"`
}

// PartitionInfo holds per-partition state
type PartitionInfo struct {
	Partition uint32 `json:"partition"`
	Messages  uint64 `json:"messages"`
	FirstSeq  uint64 `json:"first_seq"`
	LastSeq   uint64 `json:"last_seq"`
	Bytes     uint64 `json:"bytes"`
	Stream    string `json:"stream"`
	Subject   string `json:"subject"`
}

// GroupOffset holds a committed offset for one group/topic/partition
type GroupOffset struct {
	Group     string `json:"group"`
	Topic     string `json:"topic"`
	Partition uint32 `json:"partition"`
	Offset    int64  `json:"offset"`
}

// StartAdmin starts the admin HTTP server on the configured port.
// All management endpoints are served under /v1/stream/.
// /healthz is served at root for K8s probes.
func (b *Broker) StartAdmin() {
	if b.Config.AdminPort == 0 {
		return
	}

	const prefix = "/v1/stream"

	mux := http.NewServeMux()
	mux.HandleFunc(prefix+"/status", b.handleStatus)
	mux.HandleFunc(prefix+"/topics", b.handleTopics)
	mux.HandleFunc(prefix+"/groups", b.handleGroups)
	mux.HandleFunc("/healthz", b.handleHealthz)
	mux.HandleFunc(prefix+"/", b.handleIndex)
	mux.HandleFunc("/", b.handleRoot)

	addr := fmt.Sprintf(":%d", b.Config.AdminPort)
	log.Info("Admin HTTP server listening on %s", addr)
	go func() {
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Error("Admin HTTP server error: %v", err)
		}
	}()
}

func (b *Broker) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.Redirect(w, r, "/v1/stream/", http.StatusMovedPermanently)
}

func (b *Broker) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	fmt.Fprintf(w, "Hanzo Stream Admin\n\n")
	fmt.Fprintf(w, "GET /v1/stream/status  — service status\n")
	fmt.Fprintf(w, "GET /v1/stream/topics  — list topics with partition details\n")
	fmt.Fprintf(w, "GET /v1/stream/groups  — list consumer group offsets\n")
	fmt.Fprintf(w, "GET /healthz           — health check\n")
}

func (b *Broker) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if b.PubSub != nil && b.PubSub.NC != nil && b.PubSub.NC.IsConnected() {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "ok\n")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprintf(w, "pubsub disconnected\n")
}

func (b *Broker) handleStatus(w http.ResponseWriter, r *http.Request) {
	connStatus := "disconnected"
	if b.PubSub != nil && b.PubSub.NC != nil && b.PubSub.NC.IsConnected() {
		connStatus = "connected"
	}
	writeJSON(w, AdminStatus{
		Service:    "hanzo-stream",
		KafkaPort:  b.Config.BrokerPort,
		AdminPort:  b.Config.AdminPort,
		PubSubURL:  b.Config.PubSubUrl,
		PubSubConn: connStatus,
		NodeID:     b.Config.NodeID,
	})
}

func (b *Broker) handleTopics(w http.ResponseWriter, r *http.Request) {
	if b.PubSub == nil {
		http.Error(w, "pubsub not connected", http.StatusServiceUnavailable)
		return
	}

	// Collect all streams, group by topic
	topicMap := make(map[string][]PartitionInfo)
	for name := range b.PubSub.JS.StreamNames() {
		topic, partition, ok := pubsub.ParseStreamName(name)
		if !ok {
			continue
		}
		info, err := b.PubSub.JS.StreamInfo(name)
		if err != nil {
			continue
		}
		pi := PartitionInfo{
			Partition: partition,
			Messages:  info.State.Msgs,
			FirstSeq:  info.State.FirstSeq,
			LastSeq:   info.State.LastSeq,
			Bytes:     info.State.Bytes,
			Stream:    name,
			Subject:   pubsub.SubjectName(topic, partition),
		}
		topicMap[topic] = append(topicMap[topic], pi)
	}

	// Filter by ?topic= if provided
	filterTopic := r.URL.Query().Get("topic")

	var topics []TopicInfo
	for name, partitions := range topicMap {
		if filterTopic != "" && name != filterTopic {
			continue
		}
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].Partition < partitions[j].Partition
		})
		topics = append(topics, TopicInfo{Name: name, Partitions: partitions})
	}
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
	writeJSON(w, topics)
}

func (b *Broker) handleGroups(w http.ResponseWriter, r *http.Request) {
	if b.PubSub == nil {
		http.Error(w, "pubsub not connected", http.StatusServiceUnavailable)
		return
	}

	kv, err := b.PubSub.JS.KeyValue("kafka-consumer-offsets")
	if err != nil {
		writeJSON(w, []GroupOffset{})
		return
	}

	keys, err := kv.Keys()
	if err != nil {
		writeJSON(w, []GroupOffset{})
		return
	}

	filterGroup := r.URL.Query().Get("group")
	filterTopic := r.URL.Query().Get("topic")

	var offsets []GroupOffset
	for _, key := range keys {
		// Key format: group.topic.partition
		parts := strings.SplitN(key, ".", 3)
		if len(parts) != 3 {
			continue
		}
		group, topic := parts[0], parts[1]
		var partition uint32
		fmt.Sscanf(parts[2], "%d", &partition)

		if filterGroup != "" && group != filterGroup {
			continue
		}
		if filterTopic != "" && topic != filterTopic {
			continue
		}

		entry, err := kv.Get(key)
		if err != nil {
			continue
		}
		var offset int64
		fmt.Sscanf(string(entry.Value()), "%d", &offset)
		offsets = append(offsets, GroupOffset{
			Group:     group,
			Topic:     topic,
			Partition: partition,
			Offset:    offset,
		})
	}

	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].Group != offsets[j].Group {
			return offsets[i].Group < offsets[j].Group
		}
		if offsets[i].Topic != offsets[j].Topic {
			return offsets[i].Topic < offsets[j].Topic
		}
		return offsets[i].Partition < offsets[j].Partition
	})
	writeJSON(w, offsets)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
