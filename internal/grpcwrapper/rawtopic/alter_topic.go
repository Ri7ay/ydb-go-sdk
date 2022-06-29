package rawtopic

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type AlterTopicRequest struct {
	OperationParams rawydb.OperationParams
	Path            string
	AddConsumer     []Consumer
}

func (req *AlterTopicRequest) ToProto() *Ydb_PersQueue_V1.AddReadRuleRequest {
	panic("not implemented")
}

type Consumer struct {
	Name            string
	Important       bool
	SupportedCodecs SupportedCodecs
	Attributes      map[string]string
}

type AlterTopicResult struct{}

func (r *AlterTopicResult) FromProto(proto *Ydb_PersQueue_V1.AlterTopicResponse) error {
	panic("not implemented")
}
