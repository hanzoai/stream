package protocol

import (
	"time"

	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
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

func decodeFetchRequest(d serde.Decoder, fetchRequest *FetchRequest, apiVersion uint16) {
	fetchRequest.ReplicaID = d.UInt32()
	fetchRequest.MaxWaitMs = d.UInt32()
	fetchRequest.MinBytes = d.UInt32()
	if apiVersion >= 3 {
		fetchRequest.MaxBytes = d.UInt32()
	}
	if apiVersion >= 4 {
		fetchRequest.IsolationLevel = d.UInt8()
	}
	if apiVersion >= 7 {
		fetchRequest.SessionID = d.UInt32()
		fetchRequest.SessionEpoch = d.UInt32()
	}

	if apiVersion >= 12 {
		// Flexible version: compact arrays/strings + tagged fields
		lenTopic := int(d.CompactArrayLen())
		for i := 0; i < lenTopic; i++ {
			topic := FetchRequestTopic{Name: d.CompactString()}
			lenPartitions := int(d.CompactArrayLen())
			for j := 0; j < lenPartitions; j++ {
				p := FetchRequestPartitionData{
					PartitionIndex:     d.UInt32(),
					CurrentLeaderEpoch: d.UInt32(),
					FetchOffset:        d.UInt64(),
					LastFetchedEpoch:   d.UInt32(),
					LogStartOffset:     d.UInt64(),
					PartitionMaxBytes:  d.UInt32(),
				}
				topic.Partitions = append(topic.Partitions, p)
				d.EndStruct()
			}
			fetchRequest.Topics = append(fetchRequest.Topics, topic)
			d.EndStruct()
		}
		// forgotten topics (compact array)
		lenForgotten := int(d.CompactArrayLen())
		for i := 0; i < lenForgotten; i++ {
			ft := FetchRequestForgottenTopic{Topic: d.CompactString()}
			lenParts := int(d.CompactArrayLen())
			for j := 0; j < lenParts; j++ {
				ft.Partitions = append(ft.Partitions, d.UInt32())
			}
			fetchRequest.ForgottenTopicsData = append(fetchRequest.ForgottenTopicsData, ft)
			d.EndStruct()
		}
		fetchRequest.RackID = d.CompactString()
	} else {
		// Non-flexible: int32 array counts, int16 strings
		lenTopic := int(int32(d.UInt32()))
		for i := 0; i < lenTopic; i++ {
			topic := FetchRequestTopic{Name: d.NullableString()}
			lenPartitions := int(int32(d.UInt32()))
			for j := 0; j < lenPartitions; j++ {
				p := FetchRequestPartitionData{
					PartitionIndex: d.UInt32(),
				}
				if apiVersion >= 9 {
					p.CurrentLeaderEpoch = d.UInt32()
				}
				p.FetchOffset = d.UInt64()
				if apiVersion >= 12 {
					p.LastFetchedEpoch = d.UInt32()
					p.LogStartOffset = d.UInt64()
				}
				p.PartitionMaxBytes = d.UInt32()
				topic.Partitions = append(topic.Partitions, p)
			}
			fetchRequest.Topics = append(fetchRequest.Topics, topic)
		}
		if apiVersion >= 7 {
			// forgotten topics (int32 array)
			lenForgotten := int(int32(d.UInt32()))
			for i := 0; i < lenForgotten; i++ {
				ft := FetchRequestForgottenTopic{Topic: d.NullableString()}
				lenParts := int(int32(d.UInt32()))
				for j := 0; j < lenParts; j++ {
					ft.Partitions = append(ft.Partitions, d.UInt32())
				}
				fetchRequest.ForgottenTopicsData = append(fetchRequest.ForgottenTopicsData, ft)
			}
		}
		if apiVersion >= 11 {
			fetchRequest.RackID = d.NullableString()
		}
	}
}

func (b *Broker) getFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	fetchRequest := &FetchRequest{}
	decodeFetchRequest(decoder, fetchRequest, req.RequestAPIVersion)
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
	return encodeFetchResponse(req, response)
}

// encodeFetchResponse manually encodes the Fetch response based on API version.
// v0-v11: non-flexible format (int32 arrays, int16 strings, no tagged fields).
// v12+: flexible format (compact arrays/strings, tagged field terminators).
func encodeFetchResponse(req types.Request, response FetchResponse) []byte {
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)

	if req.RequestAPIVersion >= 12 {
		// Flexible response header
		e.EndStruct()
		e.Encode(response)
	} else {
		// Non-flexible response
		if req.RequestAPIVersion >= 1 {
			e.PutInt32(response.ThrottleTimeMs)
		}
		if req.RequestAPIVersion >= 7 {
			e.PutInt16(response.ErrorCode)
			e.PutInt32(response.SessionID)
		}
		// responses array
		e.PutArrayLen(len(response.Responses))
		for _, topic := range response.Responses {
			e.PutString(topic.TopicName)
			e.PutArrayLen(len(topic.Partitions))
			for _, p := range topic.Partitions {
				e.PutInt32(p.PartitionIndex)
				e.PutInt16(p.ErrorCode)
				e.PutInt64(p.HighWatermark)
				if req.RequestAPIVersion >= 4 {
					e.PutInt64(p.LastStableOffset)
				}
				if req.RequestAPIVersion >= 5 {
					e.PutInt64(p.LogStartOffset)
				}
				if req.RequestAPIVersion >= 4 {
					// aborted transactions (int32 array, -1 = null)
					if len(p.AbortedTransactions) == 0 {
						e.PutInt32(uint32(MinusOne)) // null array = -1
					} else {
						e.PutArrayLen(len(p.AbortedTransactions))
						for _, at := range p.AbortedTransactions {
							e.PutInt64(at.ProducerID)
							e.PutInt64(at.FirstOffset)
						}
					}
				}
				if req.RequestAPIVersion >= 11 {
					e.PutInt32(p.PreferredReadReplica)
				}
				// records: int32 length prefix + bytes (-1 = null)
				if len(p.Records) == 0 {
					e.PutInt32(uint32(MinusOne)) // null
				} else {
					e.PutInt32(uint32(len(p.Records)))
					e.PutBytes(p.Records)
				}
			}
		}
	}

	e.PutLen()
	return e.Bytes()
}
