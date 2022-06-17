package rawtopicreader

import (
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type (
	SupportedCodecs []Codec
	Codec           int
)

const (
	CodecUNSPECIFIED Codec = iota
	CodecRaw         Codec = 1
	CodecGzip        Codec = 2
)

const (
	CodecCustomerFirst = 10000
	CodecCustomerLast  = 19999
)

func (c Codec) IsCustomerCodec() bool {
	return c >= CodecCustomerFirst && c <= CodecCustomerLast
}

func (c *Codec) fromProto(codec Ydb_PersQueue_V1.Codec) {
	*c = Codec(codec)
}

type PartitionSessionID struct {
	v int64
}

func (id PartitionSessionID) Less(other PartitionSessionID) bool {
	return id.v < other.v
}

func (id *PartitionSessionID) FromInt64(v int64) {
	id.v = v
}

func (id PartitionSessionID) ToInt64() int64 {
	return id.v
}

type Offset int64

func (offset *Offset) FromInt64(v int64) {
	*offset = Offset(v)
}

func (offset Offset) ToInt64() int64 {
	return int64(offset)
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
// StartPartitionSessionRequest
//

type StartPartitionSessionRequest struct {
	serverMessageImpl

	ServerMessageMetadata
	PartitionSession PartitionSession
	CommittedOffset  Offset
	EndOffset        Offset
}

func (r *StartPartitionSessionRequest) fromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage_StartPartitionSessionRequest) {
	r.PartitionSession.PartitionID = p.PartitionSession.PartitionId
	r.PartitionSession.Topic = p.PartitionSession.Topic
	r.PartitionSession.PartitionSessionID.FromInt64(p.PartitionSession.PartitionSessionId)
	r.CommittedOffset.FromInt64(p.CommittedOffset)
	r.EndOffset.FromInt64(p.EndOffset)
}

type PartitionSession struct {
	Topic              string
	PartitionID        int64
	PartitionSessionID PartitionSessionID
}

type StartPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
	ReadOffset         Offset
	ReadOffsetUse      bool // ReadOffset used only if ReadOffsetUse=true to distinguish zero and unset variables
	CommitOffset       Offset
	CommitOffsetUse    bool // CommitOffset used only if CommitOffsetUse=true to distinguish zero and unset variables
}

func (r *StartPartitionSessionResponse) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_StartPartitionSessionResponse {
	res := &Ydb_PersQueue_V1.StreamingReadClientMessage_StartPartitionSessionResponse{
		PartitionSessionId: r.PartitionSessionID.ToInt64(),
	}
	if r.ReadOffsetUse {
		res.ReadOffset = r.ReadOffset.ToInt64()
	}
	if r.CommitOffsetUse {
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

//
// InitRequest
//

type InitRequest struct {
	clientMessageImpl

	// Message that describes topic to read.
	// Topics that will be read by this session.
	TopicsReadSettings []TopicReadSettings

	// Path of consumer that is used for reading by this session.
	Consumer string

	// Skip all messages that has write timestamp smaller than now - max_time_lag_ms.
	MaxLagDuration time.Duration

	// Read data only after this timestamp from all topics.
	StartFromWrittenAt time.Time

	// Session identifier for retries. Must be the same as session_id from Inited server response. If this is first connect, not retry - do not use this field.
	SessionID string

	// 0 for first init message and incremental value for connect retries.
	ConnectionAttempt int64

	// Formed state for retries. If not retry - do not use this field.
	State State

	IdleTimeoutMs int64
}

func (g *InitRequest) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_InitRequest {
	p := &Ydb_PersQueue_V1.StreamingReadClientMessage_InitRequest{
		TopicsReadSettings:        nil,
		Consumer:                  g.Consumer,
		MaxLagDurationMs:          g.MaxLagDuration.Milliseconds(),
		MaxSupportedFormatVersion: 0,
		MaxMetaCacheSize:          1024 * 1024 * 1024, // TODO: fix
		IdleTimeoutMs:             time.Minute.Milliseconds(),
	}
	if startFromMs := g.StartFromWrittenAt.UnixMilli(); startFromMs >= 0 {
		p.StartFromWrittenAtMs = startFromMs
	}

	p.TopicsReadSettings = make([]*Ydb_PersQueue_V1.StreamingReadClientMessage_TopicReadSettings, 0, len(g.TopicsReadSettings))
	for _, gSettings := range g.TopicsReadSettings {
		pSettings := &Ydb_PersQueue_V1.StreamingReadClientMessage_TopicReadSettings{
			Topic: gSettings.Topic,
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
	// Topic path.
	Topic string

	// Partitions id that will be read by this session.
	// If list is empty - then session will read all partition groups.
	PartitionsID []int64

	// Read data only after this timestamp from this topic.
	StartFromWritten time.Time
}

type PartitionSessionState struct {
	Status int // TODO: Enum from pb
}

type State struct{}

type InitResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	SessionID string
}

func (g *InitResponse) fromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage_InitResponse) {
	g.SessionID = p.SessionId
	return
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
	Partitions []PartitionData
}

func (r *ReadResponse) fromProto(p *Ydb_PersQueue_V1.StreamingReadServerMessage_ReadResponse) error {
	r.Partitions = make([]PartitionData, len(p.PartitionData))
	for partitionIndex := range p.PartitionData {
		dstPartition := &r.Partitions[partitionIndex]
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
			dstBatch.WriteTimeStamp = time.UnixMilli(srcBatch.WriteTimestampMs)

			if srcMeta := srcBatch.GetSessionMeta().GetValue(); len(srcMeta) > 0 {
				dstBatch.SessionMeta = make(map[string]string, len(srcMeta))
				for key, val := range srcMeta {
					dstBatch.SessionMeta[key] = val
				}
			}

			dstBatch.Messages = make([]MessageData, len(srcBatch.MessageData))
			for messageIndex := range srcBatch.MessageData {
				dstMess := &dstBatch.Messages[messageIndex]
				srcMess := srcBatch.MessageData[messageIndex]
				if srcMess == nil {
					return xerrors.WithStackTrace(fmt.Errorf("unexpected nil message"))
				}

				dstMess.Offset.FromInt64(srcMess.Offset)
				dstMess.SeqNo = srcMess.SeqNo
				dstMess.Created = time.UnixMilli(srcMess.CreateTimestampMs)
				dstMess.Codec.fromProto(srcMess.Codec)
				dstMess.Data = srcMess.Data
				dstMess.UncompressedSize = srcMess.UncompressedSize
				dstMess.PartitionKey = srcMess.PartitionKey
				dstMess.ExplicitHash = srcMess.ExplicitHash
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
	MessageGroupID string
	SessionMeta    map[string]string // nil if session meta is empty
	WriteTimeStamp time.Time
	WriterIP       string

	Messages  []MessageData
	SizeBytes int
}

type MessageData struct {
	Offset Offset
	SeqNo  int64

	// Timestamp of creation of message provided on write from client.
	Created          time.Time
	Codec            Codec
	Data             []byte
	UncompressedSize int64

	// Filled if partition_key / hash was used on message write.
	PartitionKey string
	ExplicitHash []byte
}

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	clientMessageImpl

	PartitionsOffsets []PartitionCommitOffset
}

func (r *CommitOffsetRequest) toProto() *Ydb_PersQueue_V1.StreamingReadClientMessage_CommitRequest {
	res := &Ydb_PersQueue_V1.StreamingReadClientMessage_CommitRequest{}
	res.Commits = make([]*Ydb_PersQueue_V1.StreamingReadClientMessage_PartitionCommit, len(r.PartitionsOffsets))

	for partitionIndex := range r.PartitionsOffsets {
		partition := &r.PartitionsOffsets[partitionIndex]

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

// OffsetRange represents range [start_offset, end_offset).
type OffsetRange struct {
	Start Offset
	End   Offset
}

type CommitOffsetResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	Committed []PartitionCommittedOffset
}

func (r *CommitOffsetResponse) fromProto(response *Ydb_PersQueue_V1.StreamingReadServerMessage_CommitResponse) error {
	r.Committed = make([]PartitionCommittedOffset, len(response.PartitionsCommittedOffsets))
	for i := range r.Committed {
		grpcCommited := response.PartitionsCommittedOffsets[i]
		if grpcCommited == nil {
			return xerrors.WithStackTrace(errors.New("unexpected nil while parse commit offset response"))
		}

		commited := &r.Committed[i]
		commited.PartitionSessionID.FromInt64(grpcCommited.PartitionSessionId)
		commited.Committed.FromInt64(grpcCommited.CommittedOffset)
	}

	return nil
}

type PartitionCommittedOffset struct {
	PartitionSessionID PartitionSessionID
	Committed          Offset
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
	PartitionSessionID PartitionSessionID
	Committed          Offset
	End                Offset
	WrittenAtWatermark time.Time
}

//
// UpdateTokenRequest
//

type UpdateTokenRequest struct {
	clientMessageImpl

	Token string
}
type UpdateTokenResponse struct {
	serverMessageImpl

	ServerMessageMetadata
}
