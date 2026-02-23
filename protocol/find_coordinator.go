package protocol

import (
	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
)

// FindCoordinatorRequestV1To3 represents the request for coordinator finding
type FindCoordinatorRequestV1To3 struct {
	Key     types.NonCompactString
	KeyType uint8
}

// FindCoordinatorRequestV4Plus represents the request for coordinator finding
type FindCoordinatorRequestV4Plus struct {
	KeyType         uint8
	CoordinatorKeys []string
}

// FindCoordinatorResponse represents the response for a coordinator finding request.
type FindCoordinatorResponse struct {
	ThrottleTimeMs uint32
	Coordinators   []FindCoordinatorResponseCoordinator
}

// FindCoordinatorResponseCoordinator represents a coordinator.
type FindCoordinatorResponseCoordinator struct {
	Key          string `kafka:"CompactString"`
	NodeID       uint32
	Host         string `kafka:"CompactString"`
	Port         uint32
	ErrorCode    uint16
	ErrorMessage string `kafka:"CompactString"`
}

func (b *Broker) getFindCoordinatorResponse(req types.Request) []byte {
	log.Info("getFindCoordinatorResponse v%d %v", req.RequestAPIVersion, req.Body)
	decoder := serde.NewDecoder(req.Body)
	var key string
	if req.RequestAPIVersion == 0 {
		// v0: Key (STRING only, no KeyType)
		key = decoder.NullableString()
		log.Info("FindCoordinatorRequest v0 key=%q", key)
	} else if req.RequestAPIVersion < 4 {
		// v1-v3: non-flexible, Key (STRING) + KeyType (INT8)
		findCoordinatorRequest := decoder.Decode(&FindCoordinatorRequestV1To3{}).(*FindCoordinatorRequestV1To3)
		log.Info("FindCoordinatorRequestV1To3 %+v", findCoordinatorRequest)
		key = string(findCoordinatorRequest.Key)
	} else {
		// v4+: flexible, KeyType (INT8) + CoordinatorKeys (COMPACT_ARRAY)
		findCoordinatorRequest := decoder.Decode(&FindCoordinatorRequestV4Plus{}).(*FindCoordinatorRequestV4Plus)
		log.Info("FindCoordinatorRequestV4Plus %+v", findCoordinatorRequest)

		if len(findCoordinatorRequest.CoordinatorKeys) > 0 {
			key = findCoordinatorRequest.CoordinatorKeys[0]
		}
	}

	return encodeFindCoordinatorResponse(req, key, b.Config.NodeID, b.Config.BrokerHost, uint32(b.Config.BrokerPort))
}

// encodeFindCoordinatorResponse encodes the FindCoordinator response based on API version.
// v0: error_code, node_id, host, port (flat, no throttle)
// v1-v2: throttle_time_ms, error_code, error_message, node_id, host, port (flat, non-flexible)
// v3: same as v1-v2 but flexible encoding
// v4+: flexible with Coordinators array
func encodeFindCoordinatorResponse(req types.Request, key string, nodeID int, host string, port uint32) []byte {
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)

	if req.RequestAPIVersion >= 3 {
		// Flexible response header
		e.EndStruct()
		if req.RequestAPIVersion >= 4 {
			// v4+: Coordinators array (flexible)
			response := FindCoordinatorResponse{
				Coordinators: []FindCoordinatorResponseCoordinator{{
					Key:    key,
					NodeID: uint32(nodeID),
					Host:   host,
					Port:   port,
				}},
			}
			e.Encode(response)
		} else {
			// v3: flat response but flexible encoding
			e.PutInt32(0) // throttle_time_ms
			e.PutInt16(0) // error_code
			e.PutCompactString("") // error_message (nullable)
			e.PutInt32(uint32(nodeID))
			e.PutCompactString(host)
			e.PutInt32(port)
			e.EndStruct() // response tagged fields
		}
	} else {
		// Non-flexible (v0-v2)
		if req.RequestAPIVersion >= 1 {
			e.PutInt32(0) // throttle_time_ms
		}
		e.PutInt16(0) // error_code
		if req.RequestAPIVersion >= 1 {
			e.PutNullableString("") // error_message (null)
		}
		e.PutInt32(uint32(nodeID))
		e.PutString(host)
		e.PutInt32(port)
	}

	e.PutLen()
	return e.Bytes()
}
