package types

// PartitionIndex is the type representing the partition index of a Kafka topic.
type PartitionIndex uint32

// TopicName is the type representing the name of a Kafka topic.
type TopicName string

// Record represents a single Kafka message or record, which can include optional headers, key, and value.
type Record struct {
	Attributes     int8
	TimestampDelta int64
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	Headers        []Header
}

// Header represents a single header in a Kafka record, consisting of a key and its associated value.
type Header struct {
	HeaderKeyLength   int
	HeaderKey         string
	HeaderValueLength int
	Value             []byte
}

// RecordBatch represents a batch of records in a Kafka partition, with metadata such as offsets, timestamps, and producer information.
type RecordBatch struct {
	BaseOffset           uint64
	BatchLength          uint32
	PartitionLeaderEpoch uint32
	Magic                uint8
	CRC                  uint32
	Attributes           uint16
	LastOffsetDelta      uint32 // delta added to BaseOffset to get the Batch's last offset
	BaseTimestamp        uint64
	MaxTimestamp         uint64
	ProducerID           uint64
	ProducerEpoch        uint16
	BaseSequence         uint32
	NumRecord            uint32
	Records              []byte
}
