package protocol

import (
	log "github.com/hanzoai/stream/logging"
	"github.com/hanzoai/stream/serde"
	"github.com/hanzoai/stream/types"
)

// ClusterID identifies this Hanzo Kafka cluster
var ClusterID = "HANZO-KAFKA-CLUSTER"

// MinusOne used through variable because setting it as uint directly is rejected
var MinusOne int = -1

// DefaultNumPartition represents the num of partitions during creation if unspecified
const DefaultNumPartition = 1

// InitProducerID (Api key = 22)
func (b *Broker) getInitProducerIDResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	initProducerIDRequest := decoder.Decode(&InitProducerIDRequest{}).(*InitProducerIDRequest)
	log.Debug(" initProducerIDRequest %+v", initProducerIDRequest)

	if int(initProducerIDRequest.ProducerID) == -1 {
		initProducerIDRequest.ProducerID = 1
	}
	if int16(initProducerIDRequest.ProducerEpoch) == -1 {
		initProducerIDRequest.ProducerEpoch = 1
	}

	response := InitProducerIDResponse{
		ProducerID:    initProducerIDRequest.ProducerID,
		ProducerEpoch: initProducerIDRequest.ProducerEpoch,
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func decodeJoinGroupRequest(d serde.Decoder, req *JoinGroupRequest, apiVersion uint16) {
	if apiVersion >= 6 {
		// Flexible: compact strings/arrays
		req.GroupID = d.CompactString()
		req.SessionTimeoutMs = d.UInt32()
		req.RebalanceTimeoutMs = d.UInt32()
		req.MemberID = d.CompactString()
		req.GroupInstanceID = d.CompactString()
		req.ProtocolType = d.CompactString()
		lenProtocols := int(d.CompactArrayLen())
		for i := 0; i < lenProtocols; i++ {
			p := JoinGroupRequestProtocol{
				Name:     d.CompactString(),
				Metadata: d.CompactBytes(),
			}
			req.Protocols = append(req.Protocols, p)
			d.EndStruct()
		}
		if apiVersion >= 8 {
			req.Reason = d.CompactString()
		}
	} else {
		// Non-flexible (v0-v5): int16 strings, int32 arrays
		req.GroupID = d.NullableString()
		req.SessionTimeoutMs = d.UInt32()
		if apiVersion >= 1 {
			req.RebalanceTimeoutMs = d.UInt32()
		}
		req.MemberID = d.NullableString()
		if apiVersion >= 5 {
			req.GroupInstanceID = d.NullableString()
		}
		req.ProtocolType = d.NullableString()
		lenProtocols := int(int32(d.UInt32()))
		for i := 0; i < lenProtocols; i++ {
			name := d.NullableString()
			metaLen := int32(d.UInt32())
			var metadata []byte
			if metaLen > 0 {
				metadata = d.GetNBytes(int(metaLen))
			}
			req.Protocols = append(req.Protocols, JoinGroupRequestProtocol{
				Name:     name,
				Metadata: metadata,
			})
		}
	}
}

func (b *Broker) getJoinGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	joinGroupRequest := &JoinGroupRequest{}
	decodeJoinGroupRequest(decoder, joinGroupRequest, req.RequestAPIVersion)
	log.Debug("joinGroupRequest %+v", joinGroupRequest)

	protocolName := ""
	var protocolMetadata []byte
	if len(joinGroupRequest.Protocols) > 0 {
		protocolName = joinGroupRequest.Protocols[0].Name
		protocolMetadata = joinGroupRequest.Protocols[0].Metadata
	}

	return encodeJoinGroupResponse(req, joinGroupRequest, protocolName, protocolMetadata)
}

// encodeJoinGroupResponse encodes JoinGroup response based on API version.
func encodeJoinGroupResponse(req types.Request, jgr *JoinGroupRequest, protocolName string, protocolMetadata []byte) []byte {
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)

	if req.RequestAPIVersion >= 6 {
		// Flexible response
		e.EndStruct()
		response := JoinGroupResponse{
			GenerationID:   1,
			ProtocolType:   jgr.ProtocolType,
			ProtocolName:   protocolName,
			Leader:         jgr.MemberID,
			SkipAssignment: false,
			MemberID:       jgr.MemberID,
			Members: []JoinGroupResponseMember{
				{MemberID: jgr.MemberID, Metadata: protocolMetadata},
			},
		}
		e.Encode(response)
	} else {
		// Non-flexible (v0-v5)
		if req.RequestAPIVersion >= 2 {
			e.PutInt32(0) // throttle_time_ms
		}
		e.PutInt16(0)                  // error_code
		e.PutInt32(1)                  // generation_id
		e.PutString(protocolName)      // protocol_name (v7+ adds protocol_type before this)
		e.PutString(jgr.MemberID)      // leader
		e.PutString(jgr.MemberID)      // member_id
		// members array
		e.PutArrayLen(1)
		e.PutString(jgr.MemberID)              // member_id
		if req.RequestAPIVersion >= 5 {
			e.PutNullableString(jgr.GroupInstanceID) // group_instance_id
		}
		// metadata: int32 len + bytes
		if protocolMetadata == nil {
			e.PutInt32(uint32(MinusOne)) // null
		} else {
			e.PutInt32(uint32(len(protocolMetadata)))
			e.PutBytes(protocolMetadata)
		}
	}

	e.PutLen()
	return e.Bytes()
}

func (b *Broker) getHeartbeatResponse(req types.Request) []byte {
	// Heartbeat: flexible at v4. Response is just throttle_time_ms + error_code.
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)
	if req.RequestAPIVersion >= 4 {
		e.EndStruct()                // response header tagged fields
		e.PutInt32(0)                // throttle_time_ms
		e.PutInt16(0)                // error_code
		e.EndStruct()                // response tagged fields
	} else {
		e.PutInt32(0) // throttle_time_ms
		e.PutInt16(0) // error_code
	}
	e.PutLen()
	return e.Bytes()
}

func decodeSyncGroupRequest(d serde.Decoder, req *SyncGroupRequest, apiVersion uint16) {
	if apiVersion >= 4 {
		// Flexible
		req.GroupID = d.CompactString()
		req.GenerationID = d.UInt32()
		req.MemberID = d.CompactString()
		req.GroupInstanceID = d.CompactString()
		req.ProtocolType = d.CompactString()
		req.ProtocolName = d.CompactString()
		lenAssignments := int(d.CompactArrayLen())
		for i := 0; i < lenAssignments; i++ {
			a := SyncGroupRequestMember{
				MemberID:   d.CompactString(),
				Assignment: d.CompactBytes(),
			}
			req.Assignments = append(req.Assignments, a)
			d.EndStruct()
		}
	} else {
		// Non-flexible (v0-v3)
		req.GroupID = d.NullableString()
		req.GenerationID = d.UInt32()
		req.MemberID = d.NullableString()
		if apiVersion >= 3 {
			req.GroupInstanceID = d.NullableString()
		}
		// protocol_type and protocol_name are v5+ only — not in v0-v3
		lenAssignments := int(int32(d.UInt32()))
		for i := 0; i < lenAssignments; i++ {
			memberID := d.NullableString()
			assignLen := int32(d.UInt32())
			var assignment []byte
			if assignLen > 0 {
				assignment = d.GetNBytes(int(assignLen))
			}
			req.Assignments = append(req.Assignments, SyncGroupRequestMember{
				MemberID:   memberID,
				Assignment: assignment,
			})
		}
	}
}

func (b *Broker) getSyncGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	syncGroupRequest := &SyncGroupRequest{}
	decodeSyncGroupRequest(decoder, syncGroupRequest, req.RequestAPIVersion)
	log.Debug("syncGroupRequest %+v", syncGroupRequest)

	var assignmentBytes []byte
	if len(syncGroupRequest.Assignments) > 0 {
		assignmentBytes = syncGroupRequest.Assignments[0].Assignment
	}

	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)

	if req.RequestAPIVersion >= 4 {
		// Flexible
		e.EndStruct() // response header tagged fields
		e.PutInt32(0) // throttle_time_ms
		e.PutInt16(0) // error_code
		if req.RequestAPIVersion >= 5 {
			e.PutCompactString(syncGroupRequest.ProtocolType) // protocol_type (v5+)
			e.PutCompactString(syncGroupRequest.ProtocolName) // protocol_name (v5+)
		}
		e.PutCompactBytes(assignmentBytes) // assignment
		e.EndStruct()                       // response tagged fields
	} else {
		// Non-flexible (v0-v3)
		if req.RequestAPIVersion >= 1 {
			e.PutInt32(0) // throttle_time_ms
		}
		e.PutInt16(0) // error_code
		// assignment: int32 len + bytes
		if assignmentBytes == nil {
			e.PutInt32(uint32(MinusOne))
		} else {
			e.PutInt32(uint32(len(assignmentBytes)))
			e.PutBytes(assignmentBytes)
		}
	}

	e.PutLen()
	return e.Bytes()
}

func (b *Broker) getOffsetFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)

	if req.RequestAPIVersion >= 8 {
		// v8+: batched groups format (flexible)
		offsetFetchRequest := decoder.Decode(&OffsetFetchRequest{}).(*OffsetFetchRequest)
		log.Debug("offsetFetchRequest %+v", offsetFetchRequest)

		response := OffsetFetchResponse{}
		for _, group := range offsetFetchRequest.Groups {
			g := OffsetFetchGroup{GroupID: group.GroupID}
			for _, topic := range group.Topics {
				t := OffsetFetchTopic{Name: topic.Name}
				for _, partitionIndex := range topic.PartitionIndexes {
					committedOffset, _ := b.PubSub.GetCommittedOffset(group.GroupID, topic.Name, partitionIndex)
					t.Partitions = append(t.Partitions, OffsetFetchPartition{
						PartitionIndex:  partitionIndex,
						CommittedOffset: uint64(committedOffset),
					})
				}
				g.Topics = append(g.Topics, t)
			}
			response.Groups = append(response.Groups, g)
		}
		log.Debug("OffsetFetchResponse %+v", response)
		encoder := serde.NewEncoder()
		return encoder.EncodeResponseBytes(req, response)
	}

	// v0-v7: flat format — GroupID + Topics
	groupID := decoder.NullableString()
	var topics []OffsetFetchRequestTopic
	if req.RequestAPIVersion <= 7 {
		lenTopics := int(int32(decoder.UInt32()))
		for i := 0; i < lenTopics; i++ {
			name := decoder.NullableString()
			lenParts := int(int32(decoder.UInt32()))
			t := OffsetFetchRequestTopic{Name: name}
			for j := 0; j < lenParts; j++ {
				t.PartitionIndexes = append(t.PartitionIndexes, decoder.UInt32())
			}
			topics = append(topics, t)
		}
	}
	log.Debug("offsetFetchRequest v%d group=%s topics=%+v", req.RequestAPIVersion, groupID, topics)

	// Encode non-flexible response
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)
	if req.RequestAPIVersion >= 3 {
		e.PutInt32(0) // throttle_time_ms
	}
	// topics array
	e.PutArrayLen(len(topics))
	for _, topic := range topics {
		e.PutString(topic.Name)
		e.PutArrayLen(len(topic.PartitionIndexes))
		for _, partIdx := range topic.PartitionIndexes {
			e.PutInt32(partIdx) // partition_index
			committedOffset, _ := b.PubSub.GetCommittedOffset(groupID, topic.Name, partIdx)
			e.PutInt64(uint64(committedOffset))  // committed_offset
			if req.RequestAPIVersion >= 5 {
				e.PutInt32(uint32(MinusOne)) // committed_leader_epoch
			}
			e.PutNullableString("")  // metadata (null)
			e.PutInt16(0)            // error_code
		}
	}
	if req.RequestAPIVersion >= 2 {
		e.PutInt16(0) // top-level error_code
	}
	e.PutLen()
	return e.Bytes()
}

func (b *Broker) getOffsetCommitResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)

	if req.RequestAPIVersion >= 8 {
		// v8+: flexible
		offsetCommitRequest := decoder.Decode(&OffsetCommitRequest{}).(*OffsetCommitRequest)
		log.Debug("offsetCommitRequest %+v", offsetCommitRequest)

		response := OffsetCommitResponse{}
		for _, topic := range offsetCommitRequest.Topics {
			offsetCommitTopic := OffsetCommitResponseTopic{Name: topic.Name}
			for _, p := range topic.Partitions {
				errCode := uint16(0)
				err := b.PubSub.CommitOffset(
					offsetCommitRequest.GroupID, topic.Name,
					p.PartitionIndex, int64(p.CommittedOffset))
				if err != nil {
					log.Error("Error committing offset: %v", err)
					errCode = uint16(ErrUnknownServerError.Code)
				}
				offsetCommitTopic.Partitions = append(offsetCommitTopic.Partitions,
					OffsetCommitResponsePartition{PartitionIndex: p.PartitionIndex, ErrorCode: errCode})
			}
			response.Topics = append(response.Topics, offsetCommitTopic)
		}
		encoder := serde.NewEncoder()
		return encoder.EncodeResponseBytes(req, response)
	}

	// v0-v7: non-flexible
	groupID := decoder.NullableString()
	var generationID uint32
	var memberID string
	if req.RequestAPIVersion >= 1 {
		generationID = decoder.UInt32()
		memberID = decoder.NullableString()
	}
	if req.RequestAPIVersion >= 7 {
		_ = decoder.NullableString() // group_instance_id
	}
	_ = generationID
	_ = memberID

	type topicCommit struct {
		name       string
		partitions []struct {
			idx    uint32
			offset uint64
		}
	}
	var topics []topicCommit
	lenTopics := int(int32(decoder.UInt32()))
	for i := 0; i < lenTopics; i++ {
		tc := topicCommit{name: decoder.NullableString()}
		lenParts := int(int32(decoder.UInt32()))
		for j := 0; j < lenParts; j++ {
			partIdx := decoder.UInt32()
			committedOffset := decoder.UInt64()
			if req.RequestAPIVersion >= 6 {
				_ = decoder.UInt32() // committed_leader_epoch
			}
			if req.RequestAPIVersion >= 2 && req.RequestAPIVersion <= 3 {
				_ = decoder.UInt64() // commit_timestamp (v2-v3 only)
			}
			_ = decoder.NullableString() // committed_metadata
			tc.partitions = append(tc.partitions, struct {
				idx    uint32
				offset uint64
			}{partIdx, committedOffset})
		}
		topics = append(topics, tc)
	}
	log.Debug("offsetCommitRequest v%d group=%s topics=%+v", req.RequestAPIVersion, groupID, topics)

	// Non-flexible response
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)
	if req.RequestAPIVersion >= 3 {
		e.PutInt32(0) // throttle_time_ms
	}
	e.PutArrayLen(len(topics))
	for _, topic := range topics {
		e.PutString(topic.name)
		e.PutArrayLen(len(topic.partitions))
		for _, p := range topic.partitions {
			err := b.PubSub.CommitOffset(groupID, topic.name, p.idx, int64(p.offset))
			errCode := uint16(0)
			if err != nil {
				log.Error("Error committing offset: %v", err)
				errCode = uint16(ErrUnknownServerError.Code)
			}
			e.PutInt32(p.idx)    // partition_index
			e.PutInt16(errCode)  // error_code
		}
	}
	e.PutLen()
	return e.Bytes()
}

func decodeListOffsetsRequest(d serde.Decoder, req *ListOffsetsRequest, apiVersion uint16) {
	if apiVersion >= 7 {
		// Flexible
		req.ReplicaID = d.UInt32()
		req.IsolationLevel = d.UInt8()
		lenTopics := int(d.CompactArrayLen())
		for i := 0; i < lenTopics; i++ {
			t := ListOffsetsRequestTopic{Name: d.CompactString()}
			lenParts := int(d.CompactArrayLen())
			for j := 0; j < lenParts; j++ {
				p := ListOffsetsRequestPartition{
					PartitionIndex: d.UInt32(),
				}
				if apiVersion >= 4 {
					p.CurrentLeaderEpoch = d.UInt32()
				}
				p.Timestamp = d.UInt64()
				t.Partitions = append(t.Partitions, p)
				d.EndStruct()
			}
			req.Topics = append(req.Topics, t)
			d.EndStruct()
		}
	} else {
		// Non-flexible (v0-v6)
		req.ReplicaID = d.UInt32()
		if apiVersion >= 2 {
			req.IsolationLevel = d.UInt8()
		}
		lenTopics := int(int32(d.UInt32()))
		for i := 0; i < lenTopics; i++ {
			t := ListOffsetsRequestTopic{Name: d.NullableString()}
			lenParts := int(int32(d.UInt32()))
			for j := 0; j < lenParts; j++ {
				p := ListOffsetsRequestPartition{
					PartitionIndex: d.UInt32(),
				}
				if apiVersion >= 4 {
					p.CurrentLeaderEpoch = d.UInt32()
				}
				p.Timestamp = d.UInt64()
				t.Partitions = append(t.Partitions, p)
			}
			req.Topics = append(req.Topics, t)
		}
	}
}

func (b *Broker) getListOffsetsResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	listOffsetsRequest := &ListOffsetsRequest{}
	decodeListOffsetsRequest(decoder, listOffsetsRequest, req.RequestAPIVersion)
	log.Debug("listOffsetsRequest %+v", listOffsetsRequest)

	response := ListOffsetsResponse{}
	for _, t := range listOffsetsRequest.Topics {
		topic := ListOffsetsResponseTopic{Name: t.Name}
		for _, p := range t.Partitions {
			partition := ListOffsetsResponsePartition{PartitionIndex: p.PartitionIndex, LeaderEpoch: uint32(MinusOne)}
			info, err := b.PubSub.GetStreamInfo(t.Name, p.PartitionIndex)
			if err != nil {
				partition.ErrorCode = uint16(ErrUnknownTopicOrPartition.Code)
			} else if p.Timestamp == uint64(ListOffsetsEarliestTimestamp) {
				if info.State.Msgs > 0 && info.State.FirstSeq > 0 {
					partition.Offset = info.State.FirstSeq - 1 // 0-based
				}
			} else if p.Timestamp == uint64(ListOffsetsLatestTimestamp) {
				if info.State.Msgs > 0 {
					partition.Offset = info.State.LastSeq // next offset
				}
			} else {
				log.Error("ListOffsetsMaxTimestamp not implemented")
			}
			topic.Partitions = append(topic.Partitions, partition)
		}
		response.Topics = append(response.Topics, topic)
	}
	log.Debug("ListOffsetsResponse %+v", response)

	if req.RequestAPIVersion >= 7 {
		encoder := serde.NewEncoder()
		return encoder.EncodeResponseBytes(req, response)
	}

	// Non-flexible response
	e := serde.NewEncoder()
	e.PutInt32(req.CorrelationID)
	if req.RequestAPIVersion >= 2 {
		e.PutInt32(response.ThrottleTimeMs)
	}
	e.PutArrayLen(len(response.Topics))
	for _, topic := range response.Topics {
		e.PutString(topic.Name)
		e.PutArrayLen(len(topic.Partitions))
		for _, p := range topic.Partitions {
			e.PutInt32(p.PartitionIndex)
			e.PutInt16(p.ErrorCode)
			e.PutInt64(p.Timestamp)
			e.PutInt64(p.Offset)
			if req.RequestAPIVersion >= 4 {
				e.PutInt32(p.LeaderEpoch)
			}
		}
	}
	e.PutLen()
	return e.Bytes()
}
