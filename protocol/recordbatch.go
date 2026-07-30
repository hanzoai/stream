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

// recordBatchMagic is the only batch format this gateway stores or serves.
const recordBatchMagic = 2

// batchPrefix is the number of bytes batchLength does not count:
// baseOffset (8) + batchLength itself (4).
const batchPrefix = 12

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

// Size returns the batch's total wire size (batchLength counts from the
// partitionLeaderEpoch field, so the 12-byte prefix is added back).
func (h RecordBatchHeader) Size() int {
	return batchPrefix + int(h.BatchLength)
}

// batchAt is a parsed batch header plus its byte position in a record set.
type batchAt struct {
	At     int
	Header RecordBatchHeader
}

// walkBatches parses the RecordBatch chain covering data, succeeding only when
// the chain is well-formed: every batch has magic v2 and a sane length, and
// the last one ends exactly at len(data). Anything else is not a record set
// this broker will store or serve — a raw publish onto a kafka-* subject, a
// truncated write, corruption. One such message served verbatim poisons every
// consumer that fetches it (the client reads its garbage bytes as a batch
// length), so this walk is the single gate both produce and fetch trust.
func walkBatches(data []byte) ([]batchAt, bool) {
	var out []batchAt
	at := 0
	for at < len(data) {
		rest := data[at:]
		if len(rest) < recordBatchHeaderMinSize || rest[16] != recordBatchMagic {
			return nil, false
		}
		h, err := ParseRecordBatchHeader(rest)
		if err != nil {
			return nil, false
		}
		if h.BatchLength < recordBatchHeaderMinSize-batchPrefix ||
			h.LastOffsetDelta < 0 || h.RecordCount < 1 ||
			at+h.Size() > len(data) {
			return nil, false
		}
		out = append(out, batchAt{At: at, Header: h})
		at += h.Size()
	}
	return out, len(out) > 0
}

// stampBaseOffsets rewrites every batch's baseOffset so the chain occupies
// Kafka offsets [base, base+n). Returns n, or ok=false if data is not a valid
// batch chain (in which case nothing is modified).
func stampBaseOffsets(data []byte, base int64) (int64, bool) {
	batches, ok := walkBatches(data)
	if !ok {
		return 0, false
	}
	n := int64(0)
	for _, b := range batches {
		SetBaseOffset(data[b.At:], base+n)
		n += b.Header.OffsetCount()
	}
	return n, true
}

// batchSpan reports the Kafka offsets [first, last] covered by a stored record
// set, ok=false if the data does not walk as a valid chain.
func batchSpan(data []byte) (first, last int64, ok bool) {
	batches, ok := walkBatches(data)
	if !ok {
		return 0, 0, false
	}
	h0 := batches[0].Header
	hn := batches[len(batches)-1].Header
	return h0.BaseOffset, hn.BaseOffset + int64(hn.LastOffsetDelta), true
}
