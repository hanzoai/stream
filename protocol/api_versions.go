package protocol

import (
	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
)

// APIKey represents an API key and its supported version range.
type APIKey struct {
	APIKey     uint16
	MinVersion uint16
	MaxVersion uint16
}

// APIVersionsResponse represents the response for API versions request.
type APIVersionsResponse struct {
	ErrorCode    uint16
	APIKeys      []APIKey
	ThrottleTime uint32
}

// APIVersion (Api key = 18)
// v0-v2: non-flexible response (int32 array counts, no tagged fields).
// v3+: flexible response (compact arrays, tagged field terminators).
// Note: ApiVersions request header is always non-flexible (even v3+), but the
// response switches to flexible format at v3.
func (b *Broker) getAPIVersionResponse(req types.Request) []byte {
	apiKeys := []APIKey{
		{APIKey: produceKey, MinVersion: 0, MaxVersion: 11},
		{APIKey: fetchKey, MinVersion: 0, MaxVersion: 12},
		{APIKey: listOffsetsKey, MinVersion: 0, MaxVersion: 9},
		{APIKey: metadataKey, MinVersion: 0, MaxVersion: 12},
		{APIKey: offsetCommitKey, MinVersion: 0, MaxVersion: 9},
		{APIKey: offsetFetchKey, MinVersion: 0, MaxVersion: 9},
		{APIKey: findCoordinatorKey, MinVersion: 0, MaxVersion: 6},
		{APIKey: joinGroupKey, MinVersion: 0, MaxVersion: 9},
		{APIKey: heartbeatKey, MinVersion: 0, MaxVersion: 4},
		{APIKey: syncGroupKey, MinVersion: 0, MaxVersion: 5},
		{APIKey: apiVersionKey, MinVersion: 0, MaxVersion: 4},
		{APIKey: createTopicKey, MinVersion: 0, MaxVersion: 7},
		{APIKey: initProducerIDKey, MinVersion: 0, MaxVersion: 5},
		{APIKey: describeConfigsKey, MinVersion: 0, MaxVersion: 4},
	}

	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)

	if req.RequestAPIVersion >= 3 {
		// ApiVersions response header is ALWAYS v0 (just correlation_id, NO TAG_BUFFER),
		// even for v3+. This is a Kafka protocol special case — the client doesn't know
		// whether flexible headers are supported until after parsing this response.
		// The body itself uses flexible format (compact arrays + tagged field terminators).
		e.Encode(APIVersionsResponse{APIKeys: apiKeys})
	} else {
		// Non-flexible: int32 array counts, int16 strings, no tagged fields
		e.PutInt16(0) // error_code
		e.PutArrayLen(len(apiKeys))
		for _, k := range apiKeys {
			e.PutInt16(k.APIKey)
			e.PutInt16(k.MinVersion)
			e.PutInt16(k.MaxVersion)
		}
		if req.RequestAPIVersion >= 1 {
			e.PutInt32(0) // throttle_time_ms (v1+)
		}
	}

	e.PutLen()
	resp := e.Bytes()
	hexLen := len(resp)
	if hexLen > 60 {
		hexLen = 60
	}
	log.Info("ApiVersions v%d response len=%d hex=%x", req.RequestAPIVersion, len(resp), resp[:hexLen])
	return resp
}
