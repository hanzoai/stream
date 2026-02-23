package protocol

import (
	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
	"github.com/hanzoai/stream/utils"
)

// ProduceRequest represents the details of a ProduceRequest.
type ProduceRequest struct {
	TransactionalID string `kafka:"CompactNullableString"`
	Acks            uint16
	TimeoutMs       uint32
	TopicData       []ProduceRequestTopicData
}

// ProduceRequestTopicData represents the topic data in a ProduceRequest.
type ProduceRequestTopicData struct {
	Name          string `kafka:"CompactString"`
	PartitionData []ProduceRequestPartitionData
}

// ProduceRequestPartitionData represents the partition data in a ProduceRequest.
type ProduceRequestPartitionData struct {
	Index   uint32
	Records []byte
}

// ProduceResponse represents the response to a produce request.
type ProduceResponse struct {
	ProduceTopicResponses []ProduceTopicResponse
	ThrottleTimeMs        uint32
}

// ProduceTopicResponse represents the response for a topic in a produce request.
type ProduceTopicResponse struct {
	Name                      string `kafka:"CompactString"`
	ProducePartitionResponses []ProducePartitionResponse
}

// ProducePartitionResponse represents the response for a partition in a produce request.
type ProducePartitionResponse struct {
	Index           uint32
	ErrorCode       uint16
	BaseOffset      uint64
	LogAppendTimeMs uint64
	LogStartOffset  uint64
	RecordErrors    []RecordError
	ErrorMessage    string `kafka:"CompactString"`
}

// RecordError represents an error in a specific batch of records.
type RecordError struct {
	BatchIndex             uint32
	BatchIndexErrorMessage string // compact_nullable
}

func decodeProduceRequest(d serde.Decoder, produceRequest *ProduceRequest, apiVersion uint16) {
	if apiVersion >= 9 {
		// Flexible version: compact strings/arrays + tagged fields
		produceRequest.TransactionalID = d.CompactString()
		produceRequest.Acks = d.UInt16()
		produceRequest.TimeoutMs = d.UInt32()
		lenTopicData := int(d.CompactArrayLen())
		for i := 0; i < lenTopicData; i++ {
			topic := ProduceRequestTopicData{Name: d.CompactString()}
			lenPartitionData := int(d.CompactArrayLen())
			for j := 0; j < lenPartitionData; j++ {
				topic.PartitionData = append(topic.PartitionData, ProduceRequestPartitionData{
					Index: d.UInt32(), Records: d.CompactBytes(),
				})
				d.EndStruct()
			}
			produceRequest.TopicData = append(produceRequest.TopicData, topic)
			d.EndStruct()
		}
	} else {
		// Non-flexible: int16 strings, int32 arrays, int32 byte lengths
		produceRequest.TransactionalID = string(d.String())
		produceRequest.Acks = d.UInt16()
		produceRequest.TimeoutMs = d.UInt32()
		lenTopicData := int(int32(d.UInt32()))
		for i := 0; i < lenTopicData; i++ {
			topic := ProduceRequestTopicData{Name: string(d.String())}
			lenPartitionData := int(int32(d.UInt32()))
			for j := 0; j < lenPartitionData; j++ {
				index := d.UInt32()
				// Records: int32 length prefix + bytes
				recordsLen := int(int32(d.UInt32()))
				var records []byte
				if recordsLen > 0 {
					records = d.GetNBytes(recordsLen)
				}
				topic.PartitionData = append(topic.PartitionData, ProduceRequestPartitionData{
					Index: index, Records: records,
				})
			}
			produceRequest.TopicData = append(produceRequest.TopicData, topic)
		}
	}
}

func (b *Broker) getProduceResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	produceRequest := &ProduceRequest{}
	decodeProduceRequest(decoder, produceRequest, req.RequestAPIVersion)
	log.Debug("ProduceRequest %+v", produceRequest)
	response := ProduceResponse{}

	for _, td := range produceRequest.TopicData {
		produceTopicResponse := ProduceTopicResponse{Name: td.Name}
		for _, pd := range td.PartitionData {
			partitionResponse := ProducePartitionResponse{
				Index: pd.Index,
			}
			_, err := b.PubSub.GetStreamInfo(td.Name, pd.Index)
			if err != nil {
				partitionResponse.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
				partitionResponse.ErrorMessage = ErrUnknownTopicOrPartition.Message
			} else {
				seq, err := b.PubSub.Publish(td.Name, pd.Index, pd.Records)
				if err != nil {
					log.Error("Error publishing to PubSub: %v", err)
					partitionResponse.ErrorCode = uint16(ErrUnknownServerError.Code)
					partitionResponse.ErrorMessage = err.Error()
				} else {
					// NATS seq is 1-based, Kafka offset is 0-based
					partitionResponse.BaseOffset = seq - 1
					partitionResponse.LogAppendTimeMs = utils.NowAsUnixMilli()
				}
			}
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, partitionResponse)
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}
