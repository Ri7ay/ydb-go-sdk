package rawtopic

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type CreateTopicRequest struct {
	Path            string
	SupportedCodecs SupportedCodecs
	Consumers       []Consumer
}

func (req *CreateTopicRequest) ToProto() *Ydb_PersQueue_V1.CreateTopicRequest {
	topicSettings := &Ydb_PersQueue_V1.TopicSettings{
		SupportedCodecs: req.SupportedCodecs.ToProto(),
	}

	proto := &Ydb_PersQueue_V1.CreateTopicRequest{
		Path:     req.Path,
		Settings: topicSettings,
	}
	return proto
}

type CreateTopicResult struct{}

func (r *CreateTopicResult) FromProto(proto *Ydb_PersQueue_V1.CreateTopicResponse) error {
	var operation rawydb.Operation
	if err := operation.FromProto(proto.Operation); err != nil {
		return err
	}
	if !operation.Status.IsSuccess() {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: create topic error [%v]: %v", operation.Status, operation.Issues))
	}

	return nil
}
