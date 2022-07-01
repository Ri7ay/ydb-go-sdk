package rawtopicreader

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V1.MigrationStreamingReadClientMessage) error
	Recv() (*Ydb_PersQueue_V1.MigrationStreamingReadServerMessage, error)
	CloseSend() error
}

type StreamReader struct {
	Stream GrpcStream
}

func (s StreamReader) CloseSend() error {
	return s.Stream.CloseSend()
}

func (s StreamReader) Recv() (ServerMessage, error) {
	grpcMess, err := s.Stream.Recv()
	if err != nil {
		return nil, err
	}

	var meta rawtopiccommon.ServerMessageMetadata
	if err = meta.MetaFromProto(grpcMess); err != nil {
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

type ClientMessage interface {
	isClientMessage()
}

type clientMessageImpl struct{}

func (*clientMessageImpl) isClientMessage() {}

type ServerMessage interface {
	isServerMessage()
	StatusData() rawtopiccommon.ServerMessageMetadata
	SetStatus(status rawydb.StatusCode)
}

type serverMessageImpl struct{}

func (*serverMessageImpl) isServerMessage() {}
