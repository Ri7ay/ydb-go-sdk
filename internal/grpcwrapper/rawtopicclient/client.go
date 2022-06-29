package rawtopicclient

import (
	"context"

	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Client struct {
	Service Ydb_PersQueue_V12.PersQueueServiceClient
}

func (c *Client) AlterTopic(ctx context.Context, req rawtopic.AlterTopicRequest) (resp rawtopic.AlterTopicResult, err error) {
	protoReq := req.ToProto()
	protoResp, err := c.Service.AlterTopic(ctx, protoReq)
	if err != nil {
		return rawtopic.AlterTopicResult{}, err
	}
	err = resp.FromProto(protoResp)
	return resp, err
}

func (c *Client) StreamRead(ctxLifeTime context.Context) (rawtopicreader.StreamReader, error) {
	protoResp, err := c.Service.StreamingRead(ctxLifeTime)
	if err != nil {
		return rawtopicreader.StreamReader{}, xerrors.WithStackTrace(err)
	}
	return rawtopicreader.StreamReader{Stream: protoResp}, nil
}
