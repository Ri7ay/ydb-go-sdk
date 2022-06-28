package rawtopic

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type AlterTopicRequest struct {
	OperationParams rawydb.OperationParams
	Path            string
	AddConsumer     []Consumer
}

func (req *AlterTopicRequest) ToProto() *Ydb_PersQueue_V1.AlterTopicRequest {
	topicSettings := &Ydb_PersQueue_V1.TopicSettings{}

	for _, consumer := range req.AddConsumer {
		readRule := &Ydb_PersQueue_V1.TopicSettings_ReadRule{
			ConsumerName: consumer.Name,
			Important:    consumer.Important,
		}
		topicSettings.ReadRules = append(topicSettings.ReadRules, readRule)
	}

	res := &Ydb_PersQueue_V1.AlterTopicRequest{
		Path:     req.Path,
		Settings: topicSettings,
	}
	return res
}

type Consumer struct {
	Name            string
	Important       bool
	SupportedCodecs SupportedCodecs
	Attributes      map[string]string
}

type AlterTopicResult struct{}

func (r *AlterTopicResult) FromProto(proto *Ydb_PersQueue_V1.AlterTopicResponse) error {
	var operation rawydb.Operation
	if err := operation.FromProto(proto.Operation); err != nil {
		return err
	}
	if !operation.Status.IsSuccess() {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: alter topic error: %v", operation.Issues))
	}
	return nil
}
