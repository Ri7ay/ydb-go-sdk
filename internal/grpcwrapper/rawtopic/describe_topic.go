package rawtopic

import (
	"fmt"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
	"google.golang.org/protobuf/proto"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"
)

type DescribeTopicRequest struct {
	OperationParams rawydb.OperationParams
	Path            string
}

func (req *DescribeTopicRequest) ToProto() *Ydb_Topic.DescribeTopicRequest {
	return &Ydb_Topic.DescribeTopicRequest{
		OperationParams: req.OperationParams.ToProto(),
		Path:            req.Path,
	}
}

type DescribeTopicResult struct {
	Operation rawydb.Operation

	Consumers []Consumer
}

func (res *DescribeTopicResult) FromProto(protoResponse *Ydb_Topic.DescribeTopicResponse) error {
	if err := res.Operation.FromProtoWithStatusCheck(protoResponse.Operation); err != nil {
		return nil
	}

	protoResult := &Ydb_Topic.DescribeTopicResult{}
	if err := proto.Unmarshal(protoResponse.GetOperation().GetResult().GetValue(), protoResult); err != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: describe topic result failed on unmarshal grpc result: %w", err))
	}

	res.Consumers = make([]Consumer, len(protoResult.Consumers))
	for i := range res.Consumers {
		res.Consumers[i].MustFromProto(protoResult.Consumers[i])
	}

	// TODO: other fields
	return nil
}
