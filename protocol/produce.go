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
		// Non-flexible: NULLABLE_STRING for transactional_id, STRING for names,
		// INT32 for array counts and byte lengths (-1 = null for nullable fields)
		produceRequest.TransactionalID = d.NullableString()
		produceRequest.Acks = d.UInt16()
		produceRequest.TimeoutMs = d.UInt32()
		lenTopicData := int(int32(d.UInt32()))
		for i := 0; i < lenTopicData; i++ {
			topic := ProduceRequestTopicData{Name: d.NullableString()}
			lenPartitionData := int(int32(d.UInt32()))
			for j := 0; j < lenPartitionData; j++ {
				index := d.UInt32()
				// Records: int32 length prefix + bytes (-1 = null)
				recordsLen := int32(d.UInt32())
				var records []byte
				if recordsLen > 0 {
					records = d.GetNBytes(int(recordsLen))
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
			// Debug: log first bytes of incoming RecordBatch
			if len(pd.Records) > 0 {
				hexLen := len(pd.Records)
				if hexLen > 80 {
					hexLen = 80
				}
				log.Info("Produce RecordBatch %s/%d len=%d first_bytes=%x",
					td.Name, pd.Index, len(pd.Records), pd.Records[:hexLen])
			}
			_, err := b.PubSub.GetStreamInfo(td.Name, pd.Index)
			if err != nil {
				partitionResponse.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
				partitionResponse.ErrorMessage = ErrUnknownTopicOrPartition.Message
			} else {
				mu := b.partitionLock(td.Name, pd.Index)
				mu.Lock()
				nextOffset, offsetErr := b.nextKafkaOffset(td.Name, pd.Index)
				if offsetErr != nil {
					mu.Unlock()
					log.Error("Error computing next offset: %v", offsetErr)
					partitionResponse.ErrorCode = uint16(ErrUnknownServerError.Code)
					partitionResponse.ErrorMessage = offsetErr.Error()
				} else {
					// Stamp the correct Kafka base offset into the RecordBatch header.
					// baseOffset (bytes 0-7) is NOT covered by the CRC, so this is safe.
					if len(pd.Records) >= recordBatchHeaderMinSize {
						SetBaseOffset(pd.Records, nextOffset)
					}
					_, err := b.PubSub.Publish(td.Name, pd.Index, pd.Records)
					mu.Unlock()
					if err != nil {
						log.Error("Error publishing to PubSub: %v", err)
						partitionResponse.ErrorCode = uint16(ErrUnknownServerError.Code)
						partitionResponse.ErrorMessage = err.Error()
					} else {
						partitionResponse.BaseOffset = uint64(nextOffset)
						partitionResponse.LogAppendTimeMs = utils.NowAsUnixMilli()
					}
				}
			}
			produceTopicResponse.ProducePartitionResponses = append(produceTopicResponse.ProducePartitionResponses, partitionResponse)
		}
		response.ProduceTopicResponses = append(response.ProduceTopicResponses, produceTopicResponse)
	}
	return encodeProduceResponse(req, response)
}

// encodeProduceResponse manually encodes the Produce response based on API version.
// v0-v8: non-flexible format (int32 arrays, int16 strings, no tagged fields).
//
//	v7 fields: index, error_code, base_offset, log_append_time_ms, log_start_offset
//	v8 adds: record_errors, error_message
//
// v9+: flexible format (compact arrays/strings, tagged field terminators).
func encodeProduceResponse(req types.Request, response ProduceResponse) []byte {
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)

	if req.RequestAPIVersion >= 9 {
		// Flexible response header: tagged fields after correlation_id
		e.EndStruct()
		e.Encode(response)
	} else {
		// Non-flexible response: manual encoding without tagged fields
		// topics array (int32 count)
		e.PutArrayLen(len(response.ProduceTopicResponses))
		for _, topic := range response.ProduceTopicResponses {
			e.PutString(topic.Name)
			// partitions array (int32 count)
			e.PutArrayLen(len(topic.ProducePartitionResponses))
			for _, p := range topic.ProducePartitionResponses {
				e.PutInt32(p.Index)
				e.PutInt16(p.ErrorCode)
				e.PutInt64(p.BaseOffset)
				if req.RequestAPIVersion >= 2 {
					e.PutInt64(p.LogAppendTimeMs)
				}
				if req.RequestAPIVersion >= 5 {
					e.PutInt64(p.LogStartOffset)
				}
				if req.RequestAPIVersion >= 8 {
					// record_errors: empty array
					e.PutArrayLen(len(p.RecordErrors))
					// error_message: nullable string
					e.PutNullableString(p.ErrorMessage)
				}
			}
		}
		if req.RequestAPIVersion >= 1 {
			e.PutInt32(response.ThrottleTimeMs)
		}
	}

	e.PutLen()
	return e.Bytes()
}
