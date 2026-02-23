package serde

import (
	"encoding/binary"
	"testing"
)

func TestParseHeader_NonFlexible(t *testing.T) {
	// Simulate a Produce v8 request (non-flexible, threshold is v9)
	// Header v1: api_key(2) + api_version(2) + correlation_id(4) + client_id(int16 len + bytes) + body
	clientID := "rdkafka"
	bodyPayload := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	buf := make([]byte, 0, 128)
	// length placeholder (4 bytes)
	buf = append(buf, 0, 0, 0, 0)
	// api_key = 0 (Produce)
	buf = binary.BigEndian.AppendUint16(buf, 0)
	// api_version = 8 (non-flexible)
	buf = binary.BigEndian.AppendUint16(buf, 8)
	// correlation_id = 42
	buf = binary.BigEndian.AppendUint32(buf, 42)
	// client_id: int16 length + bytes
	buf = binary.BigEndian.AppendUint16(buf, uint16(len(clientID)))
	buf = append(buf, []byte(clientID)...)
	// body
	buf = append(buf, bodyPayload...)
	// fill in length
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
	// Simulate a Produce v9 request (flexible)
	// Header v2: api_key(2) + api_version(2) + correlation_id(4) + client_id(uvarint compact len + bytes) + tagged_fields(uvarint 0) + body
	clientID := "rdkafka"
	bodyPayload := []byte{0xCA, 0xFE, 0xBA, 0xBE}

	buf := make([]byte, 0, 128)
	// length placeholder (4 bytes)
	buf = append(buf, 0, 0, 0, 0)
	// api_key = 0 (Produce)
	buf = binary.BigEndian.AppendUint16(buf, 0)
	// api_version = 9 (flexible)
	buf = binary.BigEndian.AppendUint16(buf, 9)
	// correlation_id = 99
	buf = binary.BigEndian.AppendUint32(buf, 99)
	// client_id: compact string (uvarint length = actual_len + 1)
	buf = append(buf, byte(len(clientID)+1)) // uvarint fits in 1 byte
	buf = append(buf, []byte(clientID)...)
	// tagged_fields: 0 fields
	buf = append(buf, 0)
	// body
	buf = append(buf, bodyPayload...)
	// fill in length
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
	// Simulate a Produce v10 request with 1 tagged field
	clientID := "test-client"
	bodyPayload := []byte{0x01, 0x02, 0x03}
	tagData := []byte{0xAA, 0xBB}

	buf := make([]byte, 0, 128)
	// length placeholder
	buf = append(buf, 0, 0, 0, 0)
	// api_key = 0 (Produce), api_version = 10
	buf = binary.BigEndian.AppendUint16(buf, 0)
	buf = binary.BigEndian.AppendUint16(buf, 10)
	// correlation_id = 7
	buf = binary.BigEndian.AppendUint32(buf, 7)
	// client_id: compact string
	buf = append(buf, byte(len(clientID)+1))
	buf = append(buf, []byte(clientID)...)
	// tagged_fields: 1 field
	buf = append(buf, 1)    // numFields = 1
	buf = append(buf, 0)    // tag = 0
	buf = append(buf, 2)    // data length = 2
	buf = append(buf, tagData...)
	// body
	buf = append(buf, bodyPayload...)
	// fill in length
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
	// ApiVersions v3 should use non-flexible header (bootstrap exception)
	clientID := "rdkafka"
	bodyPayload := []byte{0xFF}

	buf := make([]byte, 0, 64)
	buf = append(buf, 0, 0, 0, 0) // length placeholder
	buf = binary.BigEndian.AppendUint16(buf, 18) // api_key = 18 (ApiVersions)
	buf = binary.BigEndian.AppendUint16(buf, 3)  // v3 (would be flexible for other APIs, but not ApiVersions)
	buf = binary.BigEndian.AppendUint32(buf, 1)  // correlation_id
	// Non-flexible header: int16 len + bytes
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
