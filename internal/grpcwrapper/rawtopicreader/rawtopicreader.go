package rawtopicreader

import (
	"errors"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

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

func NewOffset(v int64) Offset {
	return Offset(v)
}

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

func (offset *OptionalOffset) FromInt64(v int64) {
	offset.FromInt64Pointer(&v)
}

func (offset OptionalOffset) ToInt64() int64 {
	return offset.Offset.ToInt64()
}

func (offset OptionalOffset) ToOffsetPointer() *Offset {
	if offset.HasValue {
		v := offset.Offset
		return &v
	}
	return nil
}

type StreamReader struct {
	Stream GrpcStream
}

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V1.MigrationStreamingReadClientMessage) error
	Recv() (*Ydb_PersQueue_V1.MigrationStreamingReadServerMessage, error)
	CloseSend() error
}

func (s StreamReader) CloseSend() error {
	return s.Stream.CloseSend()
}

func (s StreamReader) Recv() (ServerMessage, error) {
	grpcMess, err := s.Stream.Recv()
	if err != nil {
		return nil, err
	}

	var meta ServerMessageMetadata
	if err = meta.metaFromProto(grpcMess); err != nil {
		return nil, err
	}
	if !meta.Status.IsSuccess() {
		return nil, xerrors.WithStackTrace(fmt.Errorf("bad status from pq server: %v", meta.Status))
	}

	switch m := grpcMess.Response.(type) {
	case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_InitResponse_:
		resp := &InitResponse{}
		resp.ServerMessageMetadata = meta
		resp.fromProto(m.InitResponse)
		return resp, nil
	case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Assigned_:
		resp := &StartPartitionSessionRequest{}
		resp.ServerMessageMetadata = meta
		resp.fromProto(m.Assigned)
		return resp, nil
	case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_DataBatch_:
		resp := &ReadResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.DataBatch); err != nil {
			return nil, err
		}
		return resp, nil
	case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Release_:
		req := &StopPartitionSessionRequest{}
		req.ServerMessageMetadata = meta
		if err = req.fromProto(m.Release); err != nil {
			return nil, err
		}
		return req, nil
	case *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Committed_:
		resp := &CommitOffsetResponse{}
		resp.ServerMessageMetadata = meta
		if err = resp.fromProto(m.Committed); err != nil {
			return nil, err
		}
		return resp, nil
	}

	panic(fmt.Errorf("not implemented: %#v", grpcMess.Response))
}

func (s StreamReader) Send(mess ClientMessage) error {
	switch m := mess.(type) {
	case *InitRequest:
		grpcMess := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
			Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_InitRequest_{InitRequest: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	case *ReadRequest:
		grpcMess := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
			Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Read_{Read: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	case *StartPartitionSessionResponse:
		grpcMess := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
			Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_StartRead_{StartRead: m.toProto()},
		}
		return s.Stream.Send(grpcMess)
	case *CommitOffsetRequest:
		grpcMess := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage{
			Request: &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Commit_{Commit: m.toProto()},
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
	Issues rawydb.Issues
}

func (m *ServerMessageMetadata) metaFromProto(p *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage) error {
	if err := m.Status.FromProto(p.Status); err != nil {
		return err
	}

	return m.Issues.FromProto(p.Issues)
}

func (m *ServerMessageMetadata) StatusData() ServerMessageMetadata {
	return *m
}

func (m *ServerMessageMetadata) SetStatus(status rawydb.StatusCode) {
	m.Status = status
}

type ServerMessage interface {
	isServerMessage()
	StatusData() ServerMessageMetadata
	SetStatus(status rawydb.StatusCode)
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

func (g *InitRequest) toProto() *Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_InitRequest {
	p := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_InitRequest{
		TopicsReadSettings: nil,
		Consumer:           g.Consumer,
		MaxMetaCacheSize:   1024 * 1024 * 1024, // TODO: fix
		IdleTimeoutMs:      (time.Second * 15).Milliseconds(),
	}

	p.TopicsReadSettings = make([]*Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_TopicReadSettings, 0, len(g.TopicsReadSettings))
	for _, gSettings := range g.TopicsReadSettings {
		pSettings := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_TopicReadSettings{
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

func (g *InitResponse) fromProto(p *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_InitResponse) {
	g.SessionID = p.SessionId
}

//
// ReadRequest
//

type ReadRequest struct {
	clientMessageImpl

	BytesSize int
}

func (r *ReadRequest) toProto() *Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Read {
	return &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Read{}
}

type ReadResponse struct {
	serverMessageImpl

	ServerMessageMetadata
	BytesSize     int // TODO: FillFromProto
	PartitionData []PartitionData
}

func (r *ReadResponse) fromProto(p *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_DataBatch) error {
	r.PartitionData = make([]PartitionData, len(p.PartitionData))
	for partitionIndex := range p.PartitionData {
		dstPartition := &r.PartitionData[partitionIndex]
		srcPartition := p.PartitionData[partitionIndex]
		if srcPartition == nil {
			return xerrors.WithStackTrace(fmt.Errorf("unexpected nil partition data"))
		}

		dstPartition.PartitionSessionID.FromInt64(-1) // TODO: Migration protocol workaround
		dstPartition.PartitionID = int64(srcPartition.Partition)
		dstPartition.Batches = make([]Batch, len(srcPartition.Batches))

		for batchIndex := range srcPartition.Batches {
			dstBatch := &dstPartition.Batches[batchIndex]
			srcBatch := srcPartition.Batches[batchIndex]
			if srcBatch == nil {
				return xerrors.WithStackTrace(fmt.Errorf("unexpected nil batch"))
			}

			dstBatch.MessageGroupID = string(srcBatch.SourceId)
			dstBatch.WrittenAt = unixMilli(int64(srcBatch.WriteTimestampMs))

			dstBatch.WriteSessionMeta = make(map[string]string, len(srcBatch.ExtraFields))
			for _, keyValue := range srcBatch.ExtraFields {
				dstBatch.WriteSessionMeta[keyValue.GetKey()] = keyValue.Value
			}

			dstBatch.MessageData = make([]MessageData, len(srcBatch.MessageData))
			for messageIndex := range srcBatch.MessageData {
				dstMess := &dstBatch.MessageData[messageIndex]
				srcMess := srcBatch.MessageData[messageIndex]
				if srcMess == nil {
					return xerrors.WithStackTrace(fmt.Errorf("unexpected nil message"))
				}

				dstMess.Offset.FromInt64(int64(srcMess.Offset))
				dstMess.SeqNo = int64(srcMess.SeqNo)
				dstMess.CreatedAt = unixMilli(int64(srcMess.CreateTimestampMs))
				dstMess.Data = srcMess.Data
				dstMess.UncompressedSize = int64(srcMess.UncompressedSize)
				// TODO: dstMess.MessageGroupID
				dstBatch.Codec.MustFromProto(Ydb_Topic.Codec(srcMess.Codec)) // TODO: move to batch level
			}
		}
	}
	return nil
}

type PartitionData struct {
	PartitionSessionID PartitionSessionID

	// PartitionID use for migration protocol only
	// Deprecated
	PartitionID int64

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
	MessageGroupID   string
}

//
// CommitOffsetRequest
//

type CommitOffsetRequest struct {
	clientMessageImpl

	CommitOffsets []PartitionCommitOffset
}

func (r *CommitOffsetRequest) toProto() *Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Commit {
	res := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_Commit{}

	for partitionIndex := range r.CommitOffsets {
		partition := &r.CommitOffsets[partitionIndex]

		for offsetIndex := range partition.Offsets {
			offset := partition.Offsets[offsetIndex]

			grpcOffset := &Ydb_PersQueue_V1.CommitOffsetRange{}
			grpcOffset.AssignId = uint64(partition.PartitionSessionID)
			grpcOffset.StartOffset = uint64(offset.Start.ToInt64())
			grpcOffset.EndOffset = uint64(offset.End.ToInt64())
			res.OffsetRanges = append(res.OffsetRanges, grpcOffset)
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

func (r *CommitOffsetResponse) fromProto(response *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Committed) error {
	r.PartitionsCommittedOffsets = make([]PartitionCommittedOffset, len(response.OffsetRanges))
	for i := range r.PartitionsCommittedOffsets {
		grpcCommited := response.OffsetRanges[i]
		if grpcCommited == nil {
			return xerrors.WithStackTrace(errors.New("unexpected nil while parse commit offset response"))
		}

		commited := &r.PartitionsCommittedOffsets[i]
		commited.PartitionSessionID.FromInt64(int64(grpcCommited.AssignId))
		commited.CommittedOffset.FromInt64(int64(grpcCommited.EndOffset))
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

func (r *StartPartitionSessionRequest) fromProto(p *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Assigned) {
	r.PartitionSession.PartitionID = int64(p.Partition)
	r.PartitionSession.Path = p.Topic.Path
	r.PartitionSession.PartitionSessionID.FromInt64(int64(p.AssignId))
	r.CommittedOffset.FromInt64(int64(p.EndOffset))
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

	// Deprecated
	Topic string

	// Deprecated
	PartitionID uint64
}

func (r *StartPartitionSessionResponse) toProto() *Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_StartRead {
	res := &Ydb_PersQueue_V1.MigrationStreamingReadClientMessage_StartRead{
		Topic:     &Ydb_PersQueue_V1.Path{Path: r.Topic},
		Partition: r.PartitionID,
		AssignId:  uint64(r.PartitionSessionID),
	}
	if r.ReadOffset.HasValue {
		res.ReadOffset = uint64(r.ReadOffset.ToInt64())
	}
	if r.CommitOffset.HasValue {
		res.CommitOffset = uint64(r.CommitOffset.ToInt64())
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

func (r *StopPartitionSessionRequest) fromProto(request *Ydb_PersQueue_V1.MigrationStreamingReadServerMessage_Release) error {
	if request == nil {
		return xerrors.NewWithIssues("ydb: unexpected grpc nil stop partition session request")
	}
	r.PartitionSessionID.FromInt64(int64(request.AssignId))
	r.Graceful = !request.ForcefulRelease
	r.CommittedOffset.FromInt64(int64(request.CommitOffset))
	return nil
}

type StopPartitionSessionResponse struct {
	clientMessageImpl

	PartitionSessionID PartitionSessionID
}

// unixMilli is copy from go 1.18 library for use with older golang version
func unixMilli(msec int64) time.Time {
	return time.Unix(msec/1e3, (msec%1e3)*1e6)
}
