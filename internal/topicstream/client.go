package topicstream

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// var _ persqueue.Client = &Client{}

type Client struct {
	service Ydb_PersQueue_V1.PersQueueServiceClient

	topicsPrefix string
}

type Connector interface {
	grpc.ClientConnInterface
	Name() string
}

func New(service Ydb_PersQueue_V1.PersQueueServiceClient) *Client {
	return &Client{service: service}
}

func (c *Client) Close(_ context.Context) error {
	return nil
}

func (c *Client) StartRead(
	connectionCtx context.Context,
	consumer string,
	readSelectors []topicreader.ReadSelector,
	opts ...topicreader.ReaderOption,
) (*topicreader.Reader, error) {
	var connector topicreader.TopicSteamReaderConnect = func(
		ctx context.Context,
	) (
		topicreader.RawTopicReaderStream, error,
	) {
		stream, err := c.service.StreamingRead(ctx)
		if err != nil {
			return nil, xerrors.WithStackTrace(err)
		}
		return rawtopicreader.StreamReader{Stream: stream}, nil
	}

	return topicreader.NewReader(connectionCtx, connector, consumer, readSelectors, opts...), nil
}
