package serde

import (
	"encoding/binary"
	"testing"

	"github.com/hanzoai/stream/types"
)

func TestParseHeader_NonFlexible(t *testing.T) {
	// Produce v8 (non-flexible, threshold is v9)
	// Header v1: api_key(2) + api_version(2) + correlation_id(4) + client_id(int16 len + bytes) + body
	clientID := "rdkafka"
	bodyPayload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	buf := make([]byte, 0, 128)
	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint16(buf, 0) // api_key = Produce
	buf = binary.BigEndian.AppendUint16(buf, 8) // non-flexible
	buf = binary.BigEndian.AppendUint32(buf, 42)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, []byte(clientID)...)
	buf = append(buf, bodyPayload...)
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))

	req := ParseHeader(buf, "127.0.0.1:1234")
	if req.RequestAPIKey != 0 {
		t.Fatalf("expected api_key 0, got %d", req.RequestAPIKey)
	}
	if req.RequestAPIVersion != 8 {
		t.Fatalf("expected api_version 8, got %d", req.RequestAPIVersion)
	}
	if req.CorrelationID != 42 {
		t.Fatalf("expected correlation_id 42, got %d", req.CorrelationID)
	}
	if req.ClientID != clientID {
		t.Fatalf("expected client_id %q, got %q", clientID, req.ClientID)
	}
	if len(req.Body) != len(bodyPayload) {
		t.Fatalf("expected body len %d, got %d", len(bodyPayload), len(req.Body))
	}
	for i, b := range bodyPayload {
		if req.Body[i] != b {
			t.Fatalf("body[%d]: expected 0x%02X, got 0x%02X", i, b, req.Body[i])
		}
	}
}

func TestParseHeader_Flexible(t *testing.T) {
	// Produce v9 (flexible)
	// Header v2: api_key(2) + api_version(2) + correlation_id(4) + client_id(int16 NULLABLE_STRING) + tagged_fields(uvarint 0) + body
	// Note: client_id is ALWAYS int16-prefixed NULLABLE_STRING, even in header v2
	clientID := "rdkafka"
	bodyPayload := []byte{0xCA, 0xFE, 0xBA, 0xBE}

	buf := make([]byte, 0, 128)
	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint16(buf, 0) // api_key = Produce
	buf = binary.BigEndian.AppendUint16(buf, 9) // flexible
	buf = binary.BigEndian.AppendUint32(buf, 99)
	// client_id: int16 length + bytes (same as non-flexible!)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, []byte(clientID)...)
	// tagged_fields: 0 fields (uvarint 0)
	buf = append(buf, 0)
	// body
	buf = append(buf, bodyPayload...)
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))

	req := ParseHeader(buf, "10.0.0.1:9092")
	if req.RequestAPIKey != 0 {
		t.Fatalf("expected api_key 0, got %d", req.RequestAPIKey)
	}
	if req.RequestAPIVersion != 9 {
		t.Fatalf("expected api_version 9, got %d", req.RequestAPIVersion)
	}
	if req.CorrelationID != 99 {
		t.Fatalf("expected correlation_id 99, got %d", req.CorrelationID)
	}
	if req.ClientID != clientID {
		t.Fatalf("expected client_id %q, got %q", clientID, req.ClientID)
	}
	if len(req.Body) != len(bodyPayload) {
		t.Fatalf("expected body len %d, got %d", len(bodyPayload), len(req.Body))
	}
	for i, b := range bodyPayload {
		if req.Body[i] != b {
			t.Fatalf("body[%d]: expected 0x%02X, got 0x%02X", i, b, req.Body[i])
		}
	}
}

func TestParseHeader_FlexibleWithTaggedFields(t *testing.T) {
	// Produce v10 (flexible) with 1 tagged field in header
	clientID := "test-client"
	bodyPayload := []byte{0x01, 0x02, 0x03}
	tagData := []byte{0xAA, 0xBB}

	buf := make([]byte, 0, 128)
	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint16(buf, 0)  // Produce
	buf = binary.BigEndian.AppendUint16(buf, 10) // flexible
	buf = binary.BigEndian.AppendUint32(buf, 7)
	// client_id: int16 length + bytes
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, []byte(clientID)...)
	// tagged_fields: 1 field
	buf = append(buf, 1) // numFields = 1
	buf = append(buf, 0) // tag = 0
	buf = append(buf, 2) // data length = 2
	buf = append(buf, tagData...)
	// body
	buf = append(buf, bodyPayload...)
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))

	req := ParseHeader(buf, "10.0.0.1:9092")
	if req.ClientID != clientID {
		t.Fatalf("expected client_id %q, got %q", clientID, req.ClientID)
	}
	if len(req.Body) != len(bodyPayload) {
		t.Fatalf("expected body len %d, got %d (body: %v)", len(bodyPayload), len(req.Body), req.Body)
	}
	for i, b := range bodyPayload {
		if req.Body[i] != b {
			t.Fatalf("body[%d]: expected 0x%02X, got 0x%02X", i, b, req.Body[i])
		}
	}
}

func TestParseHeader_ApiVersionsAlwaysNonFlexible(t *testing.T) {
	// ApiVersions v3 — always non-flexible request header (bootstrap exception)
	clientID := "rdkafka"
	bodyPayload := []byte{0xFF}

	buf := make([]byte, 0, 64)
	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint16(buf, 18) // ApiVersions
	buf = binary.BigEndian.AppendUint16(buf, 3)
	buf = binary.BigEndian.AppendUint32(buf, 1)
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, []byte(clientID)...)
	buf = append(buf, bodyPayload...)
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))

	req := ParseHeader(buf, "127.0.0.1:1234")
	if req.RequestAPIKey != 18 {
		t.Fatalf("expected api_key 18, got %d", req.RequestAPIKey)
	}
	if req.ClientID != clientID {
		t.Fatalf("expected client_id %q, got %q", clientID, req.ClientID)
	}
	if len(req.Body) != 1 || req.Body[0] != 0xFF {
		t.Fatalf("expected body [0xFF], got %v", req.Body)
	}
}

func TestEncodeResponseBytes_NonFlexible(t *testing.T) {
	// Produce v7 (non-flexible) response should NOT have tagged field terminators
	type SimpleResponse struct {
		ErrorCode uint16
		Name      string
	}
	req := types.Request{
		RequestAPIKey:     0, // Produce
		RequestAPIVersion: 7, // non-flexible (threshold is v9)
		CorrelationID:     42,
	}
	resp := SimpleResponse{ErrorCode: 0, Name: "test-topic"}
	encoder := NewEncoder()
	result := encoder.EncodeResponseBytes(req, resp)

	// Parse the result: int32 length prefix + int32 correlation_id + body
	if len(result) < 8 {
		t.Fatalf("result too short: %d bytes", len(result))
	}
	totalLen := Encoding.Uint32(result[0:4])
	if int(totalLen) != len(result)-4 {
		t.Fatalf("length prefix mismatch: got %d, expected %d", totalLen, len(result)-4)
	}
	corrID := Encoding.Uint32(result[4:8])
	if corrID != 42 {
		t.Fatalf("correlation_id: expected 42, got %d", corrID)
	}
	// Next should be ErrorCode (uint16 = 0), NOT a 0x00 tagged field terminator
	// followed by Name as int16-prefixed string (NOT compact string)
	off := 8
	errCode := Encoding.Uint16(result[off:])
	off += 2
	if errCode != 0 {
		t.Fatalf("error_code: expected 0, got %d", errCode)
	}
	nameLen := Encoding.Uint16(result[off:])
	off += 2
	if nameLen != 10 { // "test-topic" = 10 chars
		t.Fatalf("name length: expected 10 (int16 format), got %d", nameLen)
	}
	name := string(result[off : off+int(nameLen)])
	if name != "test-topic" {
		t.Fatalf("name: expected 'test-topic', got %q", name)
	}
}

func TestEncodeResponseBytes_Flexible(t *testing.T) {
	// Produce v9 (flexible) response SHOULD have tagged field terminator after correlation_id
	type SimpleResponse struct {
		ErrorCode uint16
	}
	req := types.Request{
		RequestAPIKey:     0, // Produce
		RequestAPIVersion: 9, // flexible
		CorrelationID:     99,
	}
	resp := SimpleResponse{ErrorCode: 0}
	encoder := NewEncoder()
	result := encoder.EncodeResponseBytes(req, resp)

	off := 4 // skip length prefix
	corrID := Encoding.Uint32(result[off:])
	off += 4
	if corrID != 99 {
		t.Fatalf("correlation_id: expected 99, got %d", corrID)
	}
	// Next byte should be 0x00 (tagged fields terminator for response header)
	if result[off] != 0 {
		t.Fatalf("expected tagged field terminator 0x00 at offset %d, got 0x%02X", off, result[off])
	}
}

func TestParseHeader_NullClientID(t *testing.T) {
	// Flexible request with null client_id (int16 = -1 = 0xFFFF)
	bodyPayload := []byte{0x42}

	buf := make([]byte, 0, 64)
	buf = append(buf, 0, 0, 0, 0)
	buf = binary.BigEndian.AppendUint16(buf, 0)  // Produce
	buf = binary.BigEndian.AppendUint16(buf, 11) // flexible
	buf = binary.BigEndian.AppendUint32(buf, 5)
	buf = binary.BigEndian.AppendUint16(buf, 0xFFFF) // null client_id
	buf = append(buf, 0)                              // tagged_fields: 0
	buf = append(buf, bodyPayload...)
	binary.BigEndian.PutUint32(buf, uint32(len(buf)-4))

	req := ParseHeader(buf, "127.0.0.1:1234")
	if req.ClientID != "" {
		t.Fatalf("expected empty client_id, got %q", req.ClientID)
	}
	if len(req.Body) != 1 || req.Body[0] != 0x42 {
		t.Fatalf("expected body [0x42], got %v", req.Body)
	}
}
