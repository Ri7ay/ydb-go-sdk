package topicstream

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// var _ persqueue.Client = &Client{}

type Client struct {
	rawClient rawtopic.Client

	topicsPrefix string
}

type Connector interface {
	grpc.ClientConnInterface
	Name() string
}

func New(cc grpc.ClientConnInterface) *Client {
	service := Ydb_PersQueue_V1.NewPersQueueServiceClient(cc)
	return &Client{rawClient: rawtopic.Client{Service: service}}
}

func (c *Client) Close(_ context.Context) error {
	return nil
}

func (c *Client) AlterTopic(ctx context.Context, topic scheme.Path, opts ...AlterTopicOption) error {
	var req rawtopic.AlterTopicRequest
	req.Path = topic.String()

	for _, f := range opts {
		f(&req)
	}
	_, err := c.rawClient.AlterTopic(ctx, req)
	return err
}

func (c *Client) StartRead(
	consumer string,
	readSelectors []topicreader.ReadSelector,
	opts ...topicreader.ReaderOption,
) (*topicreader.Reader, error) {
	var connector topicreader.TopicSteamReaderConnect = func(ctx context.Context) (
		topicreader.RawTopicReaderStream, error,
	) {
		return c.rawClient.StreamRead(ctx)
	}

	return topicreader.NewReader(connector, consumer, readSelectors, opts...), nil
}
