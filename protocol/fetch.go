package protocol

import (
	"time"

	log "github.com/hanzoai/kafka/logging"
	"github.com/hanzoai/kafka/serde"
	"github.com/hanzoai/kafka/types"
)

// FetchRequest represents the details of a FetchRequest (Version: 12).
type FetchRequest struct {
	ReplicaID           uint32
	MaxWaitMs           uint32
	MinBytes            uint32
	MaxBytes            uint32
	IsolationLevel      uint8
	SessionID           uint32
	SessionEpoch        uint32
	Topics              []FetchRequestTopic
	ForgottenTopicsData []FetchRequestForgottenTopic
	RackID              string `kafka:"CompactString"`
}

// FetchRequestTopic represents the topic-level data in a FetchRequest.
type FetchRequestTopic struct {
	Name       string `kafka:"CompactString"`
	Partitions []FetchRequestPartitionData
}

// FetchRequestPartitionData represents the partition-level data in a FetchRequest.
type FetchRequestPartitionData struct {
	PartitionIndex     uint32
	CurrentLeaderEpoch uint32
	FetchOffset        uint64
	LastFetchedEpoch   uint32
	LogStartOffset     uint64
	PartitionMaxBytes  uint32
}

// FetchRequestForgottenTopic represents the forgotten topic data in a FetchRequest.
type FetchRequestForgottenTopic struct {
	Topic      string `kafka:"CompactString"`
	Partitions []uint32
}

// FetchResponse represents the response to a fetch request.
type FetchResponse struct {
	ThrottleTimeMs uint32
	ErrorCode      uint16
	SessionID      uint32
	Responses      []FetchTopicResponse
}

// FetchTopicResponse represents the response for a topic in a fetch request.
type FetchTopicResponse struct {
	TopicName  string `kafka:"CompactString"`
	Partitions []FetchPartitionResponse
}

// FetchPartitionResponse represents the response for a partition in a fetch request.
type FetchPartitionResponse struct {
	PartitionIndex       uint32
	ErrorCode            uint16
	HighWatermark        uint64
	LastStableOffset     uint64
	LogStartOffset       uint64
	AbortedTransactions  []AbortedTransaction
	PreferredReadReplica uint32
	Records              []byte
}

// AbortedTransaction represents an aborted transaction in the fetch response.
type AbortedTransaction struct {
	ProducerID  uint64
	FirstOffset uint64
}

func decodeFetchRequest(d serde.Decoder, fetchRequest *FetchRequest) {
	fetchRequest.ReplicaID = d.UInt32()
	fetchRequest.MaxWaitMs = d.UInt32()
	fetchRequest.MinBytes = d.UInt32()
	fetchRequest.MaxBytes = d.UInt32()
	fetchRequest.IsolationLevel = d.UInt8()
	fetchRequest.SessionID = d.UInt32()
	fetchRequest.SessionEpoch = d.UInt32()

	lenTopic := int(d.CompactArrayLen())

	for i := 0; i < lenTopic; i++ {
		topic := FetchRequestTopic{Name: d.CompactString()}
		lenPartitions := int(d.CompactArrayLen())
		for j := 0; j < lenPartitions; j++ {
			topic.Partitions = append(topic.Partitions, FetchRequestPartitionData{
				PartitionIndex:     d.UInt32(),
				CurrentLeaderEpoch: d.UInt32(),
				FetchOffset:        d.UInt64(),
				LastFetchedEpoch:   d.UInt32(),
				LogStartOffset:     d.UInt64(),
				PartitionMaxBytes:  d.UInt32(),
			})
			d.EndStruct()
		}
		fetchRequest.Topics = append(fetchRequest.Topics, topic)
		d.EndStruct()
	}
	return
}

func (b *Broker) getFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	fetchRequest := &FetchRequest{}
	decodeFetchRequest(decoder, fetchRequest)
	log.Debug("fetchRequest %+v", fetchRequest)

	numTotalRecordBytes := 0
	response := FetchResponse{}
	for _, tp := range fetchRequest.Topics {
		fetchTopicResponse := FetchTopicResponse{TopicName: tp.Name}
		for _, p := range tp.Partitions {
			info, err := b.PubSub.GetStreamInfo(tp.Name, p.PartitionIndex)
			if err != nil {
				response.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
				goto FINISH
			}

			var recordBytes []byte
			// Convert Kafka offset (0-based) to NATS sequence (1-based)
			pubsubSeq := p.FetchOffset + 1
			if pubsubSeq <= info.State.LastSeq && info.State.Msgs > 0 {
				msg, err := b.PubSub.GetMessage(tp.Name, p.PartitionIndex, pubsubSeq)
				if err != nil {
					log.Error("Error fetching from PubSub seq %d: %v", pubsubSeq, err)
				} else {
					recordBytes = msg.Data
					numTotalRecordBytes += len(recordBytes)
				}
			}

			// Map NATS sequences to Kafka offsets
			highWatermark := uint64(0)
			logStartOffset := uint64(0)
			if info.State.Msgs > 0 {
				highWatermark = info.State.LastSeq // next offset after last
				if info.State.FirstSeq > 0 {
					logStartOffset = info.State.FirstSeq - 1
				}
			}

			fetchTopicResponse.Partitions = append(fetchTopicResponse.Partitions,
				FetchPartitionResponse{
					PartitionIndex:   p.PartitionIndex,
					HighWatermark:    highWatermark,
					LastStableOffset: uint64(MinusOne),
					LogStartOffset:   logStartOffset,
					Records:          recordBytes,
				})
		}
		response.Responses = append(response.Responses, fetchTopicResponse)
	}
	if numTotalRecordBytes == 0 {
		log.Info("No data available for this fetch, waiting briefly")
		time.Sleep(300 * time.Millisecond)
	}
FINISH:
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}
