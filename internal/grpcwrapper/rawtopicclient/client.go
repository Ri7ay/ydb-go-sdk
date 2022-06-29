package rawtopicclient

import (
	"context"

	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Client struct {
	Service Ydb_PersQueue_V12.PersQueueServiceClient
}

func (c *Client) StreamRead(ctxLifeTime context.Context) (rawtopicreader.StreamReader, error) {
	protoResp, err := c.Service.MigrationStreamingRead(ctxLifeTime)
	if err != nil {
		return rawtopicreader.StreamReader{}, xerrors.WithStackTrace(err)
	}
	return rawtopicreader.StreamReader{Stream: protoResp}, nil
}
