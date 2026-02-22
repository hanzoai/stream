package protocol

import (
	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
	"github.com/nats-io/nats.go"
)

// Metadata (Api key = 3)

// MetadataResponseBroker represents a broker in a metadata response.
type MetadataResponseBroker struct {
	NodeID uint32
	Host   string `kafka:"CompactString"`
	Port   uint32
	Rack   string `kafka:"CompactString"`
}

// MetadataResponsePartition represents partition information in a metadata response.
type MetadataResponsePartition struct {
	ErrorCode       uint16
	PartitionIndex  uint32
	LeaderID        uint32
	LeaderEpoch     uint32
	ReplicaNodes    []uint32
	IsrNodes        []uint32
	OfflineReplicas []uint32
}

// MetadataRequest represents a metadata request.
type MetadataRequest struct {
	Topics                           []MetadataRequestTopic
	AllowAutoTopicCreation           bool
	IncludeTopicAuthorizedOperations bool
}

// MetadataRequestTopic represents a topic in the metadata request.
type MetadataRequestTopic struct {
	TopicID [16]byte
	Name    string `kafka:"CompactString"`
}

// MetadataResponseTopic represents a topic in the metadata response.
type MetadataResponseTopic struct {
	ErrorCode                 uint16
	Name                      string `kafka:"CompactString"`
	TopicID                   [16]byte
	IsInternal                bool
	Partitions                []MetadataResponsePartition
	TopicAuthorizedOperations uint32
}

// MetadataResponse represents a metadata response with brokers, topics, and more.
type MetadataResponse struct {
	ThrottleTimeMs uint32
	Brokers        []MetadataResponseBroker
	ClusterID      string `kafka:"CompactString"` // nullable
	ControllerID   uint32
	Topics         []MetadataResponseTopic
}

func (b *Broker) getMetadataResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	metadataRequest := decoder.Decode(&MetadataRequest{}).(*MetadataRequest)
	log.Debug("metadataRequest %+v", metadataRequest)

	nodeID := uint32(b.Config.NodeID)
	brokers := []MetadataResponseBroker{{
		NodeID: nodeID,
		Host:   b.Config.BrokerHost,
		Port:   uint32(b.Config.BrokerPort),
	}}

	// If no topics requested, list all
	if len(metadataRequest.Topics) == 0 {
		topicNames, _ := b.PubSub.ListTopics()
		for _, t := range topicNames {
			metadataRequest.Topics = append(metadataRequest.Topics, MetadataRequestTopic{Name: t})
		}
	}

	var topics []MetadataResponseTopic
	for _, reqTopic := range metadataRequest.Topics {
		topic := MetadataResponseTopic{Name: reqTopic.Name, TopicID: reqTopic.TopicID, IsInternal: false}

		if b.PubSub.TopicExists(reqTopic.Name) {
			numPartitions, _ := b.PubSub.GetTopicPartitionCount(reqTopic.Name)
			for i := uint32(0); i < numPartitions; i++ {
				topic.Partitions = append(topic.Partitions, MetadataResponsePartition{
					PartitionIndex: i,
					LeaderID:       nodeID,
					ReplicaNodes:   []uint32{nodeID},
					IsrNodes:       []uint32{nodeID},
				})
			}
		} else if metadataRequest.AllowAutoTopicCreation {
			replicas := b.Config.StreamReplicas
			if replicas < 1 {
				replicas = 1
			}
			b.PubSub.CreateTopicStreams(reqTopic.Name, 1, replicas, nats.FileStorage)
			topic.Partitions = append(topic.Partitions, MetadataResponsePartition{
				PartitionIndex: 0,
				LeaderID:       nodeID,
				ReplicaNodes:   []uint32{nodeID},
				IsrNodes:       []uint32{nodeID},
			})
		} else {
			topic.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
		}

		topics = append(topics, topic)
	}

	response := MetadataResponse{
		Brokers:      brokers,
		ClusterID:    ClusterID,
		ControllerID: nodeID,
		Topics:       topics,
	}
	log.Debug("MetadataResponse %+v", response)
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}
