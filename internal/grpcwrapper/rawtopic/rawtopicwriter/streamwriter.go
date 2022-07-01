package rawtopicwriter

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

type StreamWriter struct {
	Stream GrpcStream
}

type GrpcStream interface {
	Send(messageNew *Ydb_PersQueue_V1.StreamingWriteClientMessage) error
	Recv() (*Ydb_PersQueue_V1.StreamingWriteServerMessage, error)
	CloseSend() error
}
