package rawtopic

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type CreateTopicRequest struct {
	OperationParams rawydb.OperationParams

	Path            string
	SupportedCodecs rawtopiccommon.SupportedCodecs
	Consumers       []Consumer
}

func (req *CreateTopicRequest) ToProto() *Ydb_Topic.CreateTopicRequest {
	proto := &Ydb_Topic.CreateTopicRequest{
		Path:            req.Path,
		SupportedCodecs: req.SupportedCodecs.ToProto(),
	}

	proto.Consumers = make([]*Ydb_Topic.Consumer, len(req.Consumers))
	for i := range proto.Consumers {
		proto.Consumers[i] = req.Consumers[i].ToProto()
	}

	return proto
}

type CreateTopicResult struct {
	Operation rawydb.Operation
}

func (r *CreateTopicResult) FromProto(proto *Ydb_Topic.CreateTopicResponse) error {
	return r.Operation.FromProtoWithStatusCheck(proto.Operation)
}
