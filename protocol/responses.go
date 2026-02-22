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

func (b *Broker) getJoinGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	joinGroupRequest := decoder.Decode(&JoinGroupRequest{}).(*JoinGroupRequest)
	log.Debug("joinGroupRequest %+v", joinGroupRequest)

	response := JoinGroupResponse{
		GenerationID:   1,
		ProtocolType:   joinGroupRequest.ProtocolType,
		ProtocolName:   joinGroupRequest.Protocols[0].Name,
		Leader:         joinGroupRequest.MemberID,
		SkipAssignment: false,
		MemberID:       joinGroupRequest.MemberID,
		Members: []JoinGroupResponseMember{
			{MemberID: joinGroupRequest.MemberID,
				Metadata: joinGroupRequest.Protocols[0].Metadata},
		},
	}

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func (b *Broker) getHeartbeatResponse(req types.Request) []byte {
	response := HeartbeatResponse{}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func (b *Broker) getSyncGroupResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	syncGroupRequest := decoder.Decode(&SyncGroupRequest{}).(*SyncGroupRequest)

	response := SyncGroupResponse{
		ProtocolType:    syncGroupRequest.ProtocolType,
		ProtocolName:    syncGroupRequest.ProtocolName,
		AssignmentBytes: syncGroupRequest.Assignments[0].Assignment,
	}
	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}

func (b *Broker) getOffsetFetchResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
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

func (b *Broker) getOffsetCommitResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
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

func (b *Broker) getListOffsetsResponse(req types.Request) []byte {
	decoder := serde.NewDecoder(req.Body)
	listOffsetsRequest := decoder.Decode(&ListOffsetsRequest{}).(*ListOffsetsRequest)
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

	encoder := serde.NewEncoder()
	return encoder.EncodeResponseBytes(req, response)
}
