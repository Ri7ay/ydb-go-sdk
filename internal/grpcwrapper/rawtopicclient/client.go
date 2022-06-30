package rawtopicclient

import (
	"context"
	"fmt"

	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Client struct {
	PQService    Ydb_PersQueue_V12.PersQueueServiceClient
	TopicService Ydb_Topic_V1.TopicServiceClient
}

func (c *Client) CreateTopic(
	ctx context.Context,
	req rawtopic.CreateTopicRequest,
) (res rawtopic.CreateTopicResult, err error) {
	resp, err := c.TopicService.CreateTopic(ctx, req.ToProto())
	if err != nil {
		return res, xerrors.WithStackTrace(fmt.Errorf("ydb: create topic grpc failed: %w", err))
	}
	err = res.FromProto(resp)
	return res, err
}

func (c *Client) DescribeTopic(ctx context.Context, req rawtopic.DescribeTopicRequest) (res rawtopic.DescribeTopicResult, err error) {
	resp, err := c.TopicService.DescribeTopic(ctx, req.ToProto())
	err = res.FromProto(resp)
	return res, err
}

func (c *Client) DropTopic(
	ctx context.Context,
	req rawtopic.DropTopicRequest,
) (res rawtopic.DropTopicResult, err error) {
	resp, err := c.TopicService.DropTopic(ctx, req.ToProto())
	if err != nil {
		return res, xerrors.WithStackTrace(fmt.Errorf("ydb: drop topic grpc failed: %w", err))
	}
	err = res.FromProto(resp)
	return res, err
}

func (c *Client) StreamRead(ctxStreamLifeTime context.Context) (rawtopicreader.StreamReader, error) {
	protoResp, err := c.PQService.MigrationStreamingRead(ctxStreamLifeTime)
	if err != nil {
		return rawtopicreader.StreamReader{}, xerrors.WithStackTrace(err)
	}
	return rawtopicreader.StreamReader{Stream: protoResp}, nil
}
