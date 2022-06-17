package rawtopicreader

import (
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type (
	SupportedCodecs []rawtopic.Codec
)

type PartitionSessionID int64

func (id *PartitionSessionID) FromInt64(v int64) {
	*id = PartitionSessionID(v)
}

func (id PartitionSessionID) ToInt64() int64 {
	return int64(id)
}

type Offset int64

func (offset *Offset) FromInt64(v int64) {
	*offset = Offset(v)
}

func (offset Offset) ToInt64() int64 {
	return int64(offset)
}

type OptionalOffset struct {
	Offset   Offset
	HasValue bool
}

func (offset *OptionalOffset) FromInt64Pointer(v *int64) {
	if v == nil {
		offset.HasValue = false
		offset.Offset.FromInt64(-1)
	} else {
		offset.HasValue = true
		offset.Offset.FromInt64(*v)
	}
}

func (offset OptionalOffset) ToInt64() int64 {
	return offset.Offset.ToInt64()
}

func (offset OptionalOffset) ToOffsetPointer() *Offset {
	if offset.HasValue {
		v := offset.Offset
		return &v
	} else {
		return nil
	}
}

type StreamReader struct {
	Stream GrpcStream
}

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V1.StreamingReadClientMessage) error
	Recv() (*Ydb_PersQueue_V1.StreamingReadServerMessage, error)
}

func (s StreamReader) Close() error {
	panic("not implemented")
}

func (s StreamReader) Recv() (ServerMessage, error) {
	grpcMess, err := s.Stream.Recv()
	if err != nil {
		return nil, err
	}

	var meta ServerMessageMetadata
	meta.metaFromProto(grpcMess)
	if !meta.Status.IsSuccess() {
		return nil, xerrors.WithStackTrace(fmt.Errorf("bad status from pq server: %v", meta.Status))
	}

	switch m := grpcMess.ServerMessage.(type) {
	case *Ydb_PersQueue_V1.StreamingReadServerMessage_InitResponse_:
		resp := &InitResponse{}
		resp.ServerMessageMetadata = meta
		resp.fromProto(m.InitResponse)
		return resp, nil
	case *Ydb_PersQueue_V1.StreamingReadServerMessage_StartPartitionSessionRequest_:
		resp := &StartPartitionSessionRequest{}
		resp.ServerMessageMetadata = meta
		resp.fromProto(m.StartPartitionSessionRequest)
		return resp, nil
	case *Ydb_PersQueue_V1.StreamingReadServerMessage_ReadResponse_:
		resp := &ReadResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.ReadResponse); err != nil {
			return nil, err
		}
		return resp, nil
	case *Ydb_PersQueue_V1.StreamingReadServerMessage_CommitResponse_:
		resp := &CommitOffsetResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.CommitResponse); err != nil {
			return nil, err
		}
		return resp, nil
	}

	panic(fmt.Errorf("not implemented: %#v", grpcMess.ServerMessage))
}

func (s StreamReader) Send(mess ClientMessage) error {
	switch m := mess.(type) {
	case *InitRequest:
		grpcMess := &Ydb_PersQueue_V1.StreamingReadClientMessage{
			ClientMessage: &Ydb_PersQueue_V1.StreamingReadClientMessage_InitRequest_{InitRequest: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	case *ReadRequest:
		grpcMess := &Ydb_PersQueue_V1.StreamingReadClientMessage{
			ClientMessage: &Ydb_PersQueue_V1.StreamingReadClientMessage_ReadRequest_{ReadRequest: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	case *StartPartitionSessionResponse:
		grpcMess := &Ydb_PersQueue_V1.StreamingReadClientMessage{
			ClientMessage: &Ydb_PersQueue_V1.StreamingReadClientMessage_StartPartitionSessionResponse_{StartPartitionSessionResponse: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	case *CommitOffsetRequest:
		grpcMess := &Ydb_PersQueue_V1.StreamingReadClientMessage{
			ClientMessage: &Ydb_PersQueue_V1.StreamingReadClientMessage_CommitRequest_{CommitRequest: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	default:
		// TODO: return error
	}

	panic("not implemented")
}

//
// ClientStreamMessage
//

type ClientMessage interface {
	isClientMessage()
}

type clientMessageImpl struct{}

func (*clientMessageImpl) isClientMessage() {}

type ServerMessageMetadata struct {
	Status rawydb.StatusCode
	Issues []YdbIssueMessage
}

func (m *ServerMessageMetadata) metaFromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage) {
	m.Status.FromProto(p.Status)
	// TODO
}

func (s ServerMessageMetadata) StatusData() ServerMessageMetadata {
	return s
}

type YdbIssueMessage struct{}

type ServerMessage interface {
	isServerMessage()
	StatusData() ServerMessageMetadata
}

type serverMessageImpl struct{}

func (*serverMessageImpl) isServerMessage() {}

//
// UpdateTokenRequest
//

type UpdateTokenRequest struct {
	clientMessageImpl

	rawtopic.UpdateTokenRequest
}

type UpdateTokenResponse struct {
	serverMessageImpl
	ServerMessageMetadata

	rawtopic.UpdateTokenResponse
}

//
// InitRequest
//

type InitRequest struct {
	clientMessageImpl

	TopicsReadSettings []TopicReadSettings

	Consumer string
}

func (g *InitRequest) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_InitRequest {
	p := &Ydb_PersQueue_V1.StreamingReadClientMessage_InitRequest{
		TopicsReadSettings:        nil,
		Consumer:                  g.Consumer,
		MaxSupportedFormatVersion: 0,
		MaxMetaCacheSize:          1024 * 1024 * 1024, // TODO: fix
		IdleTimeoutMs:             time.Minute.Milliseconds(),
	}

	p.TopicsReadSettings = make([]*Ydb_PersQueue_V1.StreamingReadClientMessage_TopicReadSettings, 0, len(g.TopicsReadSettings))
	for _, gSettings := range g.TopicsReadSettings {
		pSettings := &Ydb_PersQueue_V1.StreamingReadClientMessage_TopicReadSettings{
			Topic: gSettings.Path,
		}
		pSettings.PartitionGroupIds = make([]int64, 0, len(gSettings.PartitionsID))
		for _, partitionID := range gSettings.PartitionsID {
			pSettings.PartitionGroupIds = append(pSettings.PartitionGroupIds, partitionID+1)
		}
		p.TopicsReadSettings = append(p.TopicsReadSettings, pSettings)
	}
	return p
}

type TopicReadSettings struct {
	Path         string
	PartitionsID []int64

	MaxLag   time.Duration // Optional
	ReadFrom time.Time     // Optional
}

type InitResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	SessionID string
}

func (g *InitResponse) fromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage_InitResponse) {
	g.SessionID = p.SessionId
}

//
// ReadRequest
//

type ReadRequest struct {
	clientMessageImpl

	BytesSize int
}

func (r *ReadRequest) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_ReadRequest {
	return &Ydb_PersQueue_V1.StreamingReadClientMessage_ReadRequest{
		RequestUncompressedSize: int64(r.BytesSize),
	}
}

type ReadResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	BytesSize     int // TODO: FillFromProto
	PartitionData []PartitionData
}

func (r *ReadResponse) fromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage_ReadResponse) error {
	r.PartitionData = make([]PartitionData, len(p.PartitionData))
	for partitionIndex := range p.PartitionData {
		dstPartition := &r.PartitionData[partitionIndex]
		srcPartition := p.PartitionData[partitionIndex]
		if srcPartition == nil {
			return xerrors.WithStackTrace(fmt.Errorf("unexpected nil partition data"))
		}

		dstPartition.PartitionSessionID.FromInt64(srcPartition.PartitionSessionId)
		dstPartition.Batches = make([]Batch, len(srcPartition.Batches))

		for batchIndex := range srcPartition.Batches {
			dstBatch := &dstPartition.Batches[batchIndex]
			srcBatch := srcPartition.Batches[batchIndex]
			if srcBatch == nil {
				return xerrors.WithStackTrace(fmt.Errorf("unexpected nil batch"))
			}

			dstBatch.MessageGroupID = string(srcBatch.MessageGroupId)
			dstBatch.WrittenAt = time.UnixMilli(srcBatch.WriteTimestampMs)

			if srcMeta := srcBatch.GetSessionMeta().GetValue(); len(srcMeta) > 0 {
				dstBatch.WriteSessionMeta = make(map[string]string, len(srcMeta))
				for key, val := range srcMeta {
					dstBatch.WriteSessionMeta[key] = val
				}
			}

			dstBatch.MessageData = make([]MessageData, len(srcBatch.MessageData))
			for messageIndex := range srcBatch.MessageData {
				dstMess := &dstBatch.MessageData[messageIndex]
				srcMess := srcBatch.MessageData[messageIndex]
				if srcMess == nil {
					return xerrors.WithStackTrace(fmt.Errorf("unexpected nil message"))
				}

				dstMess.Offset.FromInt64(srcMess.Offset)
				dstMess.SeqNo = srcMess.SeqNo
				dstMess.CreatedAt = time.UnixMilli(srcMess.CreateTimestampMs)
				dstMess.Data = srcMess.Data
				dstMess.UncompressedSize = srcMess.UncompressedSize
				dstMess.PartitionKey = srcMess.PartitionKey
				dstMess.ExplicitHash = srcMess.ExplicitHash
				dstBatch.Codec.FromProto(srcMess.Codec) // TODO: move to batch level
			}
		}
	}
	return nil
}

type PartitionData struct {
	PartitionSessionID PartitionSessionID

	Batches []Batch
}
type Batch struct {
	Codec rawtopic.Codec

	MessageGroupID   string
	WriteSessionMeta map[string]string // nil if session meta is empty
	WrittenAt        time.Time

	MessageData []MessageData
}

type MessageData struct {
	Offset           Offset
	SeqNo            int64
	CreatedAt        time.Time
	Data             []byte
	UncompressedSize int64

	PartitionKey string
	ExplicitHash []byte
}

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	clientMessageImpl

	CommitOffsets []PartitionCommitOffset
}

func (r *CommitOffsetRequest) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_CommitRequest {
	res := &Ydb_PersQueue_V1.StreamingReadClientMessage_CommitRequest{}
	res.Commits = make([]*Ydb_PersQueue_V1.StreamingReadClientMessage_PartitionCommit, len(r.CommitOffsets))

	for partitionIndex := range r.CommitOffsets {
		partition := &r.CommitOffsets[partitionIndex]

		grpcPartition := &Ydb_PersQueue_V1.StreamingReadClientMessage_PartitionCommit{}
		res.Commits[partitionIndex] = grpcPartition
		grpcPartition.PartitionSessionId = partition.PartitionSessionID.ToInt64()
		grpcPartition.Offsets = make([]*Ydb_PersQueue_V1.OffsetsRange, len(partition.Offsets))

		for offsetIndex := range partition.Offsets {
			offset := partition.Offsets[offsetIndex]
			grpcPartition.Offsets[offsetIndex] = &Ydb_PersQueue_V1.OffsetsRange{
				StartOffset: offset.Start.ToInt64(),
				EndOffset:   offset.End.ToInt64(),
			}
		}
	}
	return res
}

type PartitionCommitOffset struct {
	PartitionSessionID PartitionSessionID
	Offsets            []OffsetRange
}

type OffsetRange struct {
	Start Offset
	End   Offset
}

type CommitOffsetResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	PartitionsCommittedOffsets []PartitionCommittedOffset
}

func (r *CommitOffsetResponse) fromProto(response *Ydb_PersQueue_V1.StreamingReadServerMessage_CommitResponse) error {
	r.PartitionsCommittedOffsets = make([]PartitionCommittedOffset, len(response.PartitionsCommittedOffsets))
	for i := range r.PartitionsCommittedOffsets {
		grpcCommited := response.PartitionsCommittedOffsets[i]
		if grpcCommited == nil {
			return xerrors.WithStackTrace(errors.New("unexpected nil while parse commit offset response"))
		}

		commited := &r.PartitionsCommittedOffsets[i]
		commited.PartitionSessionID.FromInt64(grpcCommited.PartitionSessionId)
		commited.CommittedOffset.FromInt64(grpcCommited.CommittedOffset)
	}

	return nil
}

type PartitionCommittedOffset struct {
	PartitionSessionID PartitionSessionID
	CommittedOffset    Offset
}

//
// PartitionSessionStatusRequest
//

type PartitionSessionStatusRequest struct {
	clientMessageImpl
	PartitionSessionID PartitionSessionID
}

type PartitionSessionStatusResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	PartitionSessionID     PartitionSessionID
	PartitionOffsets       OffsetRange
	WriteTimeHighWatermark time.Time
}

//
// StartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	serverMessageImpl

	ServerMessageMetadata
	PartitionSession PartitionSession
	CommittedOffset  Offset
	PartitionOffsets OffsetRange
}

func (r *StartPartitionSessionRequest) fromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage_StartPartitionSessionRequest) {
	r.PartitionSession.PartitionID = p.PartitionSession.PartitionId
	r.PartitionSession.Path = p.PartitionSession.Topic
	r.PartitionSession.PartitionSessionID.FromInt64(p.PartitionSession.PartitionSessionId)
	r.CommittedOffset.FromInt64(p.CommittedOffset)
	// TODO: PartitionOffsets
}

type PartitionSession struct {
	PartitionSessionID PartitionSessionID
	Path               string // Topic path of partition
	PartitionID        int64
}

type StartPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
	ReadOffset         OptionalOffset
	CommitOffset       OptionalOffset
}

func (r *StartPartitionSessionResponse) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_StartPartitionSessionResponse {
	res := &Ydb_PersQueue_V1.StreamingReadClientMessage_StartPartitionSessionResponse{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
	}
	if r.ReadOffset.HasValue {
		res.ReadOffset = r.ReadOffset.ToInt64()
	}
	if r.CommitOffset.HasValue {
		res.CommitOffset = r.CommitOffset.ToInt64()
	}
	return res
}

//
// StopPartitionSessionRequest
//

type StopPartitionSessionRequest struct {
	serverMessageImpl

	ServerMessageMetadata
	PartitionSessionID PartitionSessionID
	Graceful           bool
	CommittedOffset    Offset
}
type StopPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}
