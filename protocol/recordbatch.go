package protocol

import (
	"encoding/binary"
	"fmt"
)

// RecordBatchHeader contains parsed fields from a Kafka RecordBatch v2 header.
//
// Header layout (61 bytes minimum):
//
//	baseOffset:             int64   (bytes 0-7)    — NOT covered by CRC
//	batchLength:            int32   (bytes 8-11)   — NOT covered by CRC
//	partitionLeaderEpoch:   int32   (bytes 12-15)  — NOT covered by CRC
//	magic:                  int8    (byte 16)      — NOT covered by CRC
//	crc:                    int32   (bytes 17-20)  — NOT covered by CRC
//	attributes:             int16   (bytes 21-22)  — CRC starts here
//	lastOffsetDelta:        int32   (bytes 23-26)
//	baseTimestamp:           int64   (bytes 27-34)
//	maxTimestamp:            int64   (bytes 35-42)
//	producerId:             int64   (bytes 43-50)
//	producerEpoch:          int16   (bytes 51-52)
//	baseSequence:           int32   (bytes 53-56)
//	recordCount:            int32   (bytes 57-60)
type RecordBatchHeader struct {
	BaseOffset      int64
	BatchLength     int32
	LastOffsetDelta int32
	RecordCount     int32
}

const recordBatchHeaderMinSize = 61

// ParseRecordBatchHeader extracts header fields from raw RecordBatch bytes.
func ParseRecordBatchHeader(data []byte) (RecordBatchHeader, error) {
	if len(data) < recordBatchHeaderMinSize {
		return RecordBatchHeader{}, fmt.Errorf("record batch too short: %d bytes", len(data))
	}
	return RecordBatchHeader{
		BaseOffset:      int64(binary.BigEndian.Uint64(data[0:8])),
		BatchLength:     int32(binary.BigEndian.Uint32(data[8:12])),
		LastOffsetDelta: int32(binary.BigEndian.Uint32(data[23:27])),
		RecordCount:     int32(binary.BigEndian.Uint32(data[57:61])),
	}, nil
}

// SetBaseOffset overwrites the baseOffset field in RecordBatch bytes.
// This field is NOT covered by the CRC (which starts at the attributes field,
// byte 21), so modifying it does not invalidate the batch checksum.
func SetBaseOffset(data []byte, baseOffset int64) {
	binary.BigEndian.PutUint64(data[0:8], uint64(baseOffset))
}

// OffsetCount returns the number of Kafka offsets consumed by this batch.
func (h RecordBatchHeader) OffsetCount() int64 {
	return int64(h.LastOffsetDelta) + 1
}
