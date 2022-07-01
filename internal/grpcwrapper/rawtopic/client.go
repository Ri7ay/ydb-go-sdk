package rawtopic

import (
	"context"
	"fmt"

	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Client struct {
	PQService    Ydb_PersQueue_V12.PersQueueServiceClient
	TopicService Ydb_Topic_V1.TopicServiceClient
}

func (c *Client) CreateTopic(
	ctx context.Context,
	req CreateTopicRequest,
) (res CreateTopicResult, err error) {
	resp, err := c.TopicService.CreateTopic(ctx, req.ToProto())
	if err != nil {
		return res, xerrors.WithStackTrace(fmt.Errorf("ydb: create topic grpc failed: %w", err))
	}
	err = res.FromProto(resp)
	return res, err
}

func (c *Client) DescribeTopic(ctx context.Context, req DescribeTopicRequest) (res DescribeTopicResult, err error) {
	resp, err := c.TopicService.DescribeTopic(ctx, req.ToProto())
	err = res.FromProto(resp)
	return res, err
}

func (c *Client) DropTopic(
	ctx context.Context,
	req DropTopicRequest,
) (res DropTopicResult, err error) {
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
		return rawtopicreader.StreamReader{}, xerrors.WithStackTrace(fmt.Errorf("ydb: failed start grpc topic stream read: %w", err))
	}
	return rawtopicreader.StreamReader{Stream: protoResp}, nil
}

func (c *Client) StreamWrite(ctxStreamLifeTime context.Context) (rawtopicwriter.StreamWriter, error) {
	protoResp, err := c.PQService.StreamingWrite(ctxStreamLifeTime)
	if err != nil {
		return rawtopicwriter.StreamWriter{}, xerrors.WithStackTrace(fmt.Errorf("ydb: failed start grpc topic stream write: %w", err))
	}
	return rawtopicwriter.StreamWriter{Stream: protoResp}, nil
}
