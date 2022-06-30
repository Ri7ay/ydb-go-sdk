package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/config"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicclient"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// var _ persqueue.Client = &Client{}

type Client struct {
	rawClient rawtopicclient.Client
	config    config.Config
}

func New(cc grpc.ClientConnInterface, opts ...config.Option) *Client {
	cfg := config.Config{}
	for _, f := range opts {
		f(&cfg)
	}
	service := Ydb_PersQueue_V1.NewPersQueueServiceClient(cc)
	return &Client{rawClient: rawtopicclient.Client{Service: service}, config: cfg}
}

func (c *Client) Close(_ context.Context) error {
	return nil
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
