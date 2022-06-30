package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-genproto/Ydb_Topic_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/options"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic/config"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicclient"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

// var _ persqueue.Client = &Client{}

type Client struct {
	rawClient              rawtopicclient.Client
	config                 config.Config
	defaultOperationParams rawydb.OperationParams
}

func New(cc grpc.ClientConnInterface, opts ...config.Option) *Client {
	cfg := config.Config{}
	for _, f := range opts {
		f(&cfg)
	}
	var defaultParams rawydb.OperationParams
	operationParamsFromConfig(&defaultParams, &cfg)

	return &Client{
		config:                 cfg,
		defaultOperationParams: defaultParams,
		rawClient: rawtopicclient.Client{
			PQService:    Ydb_PersQueue_V1.NewPersQueueServiceClient(cc),
			TopicService: Ydb_Topic_V1.NewTopicServiceClient(cc),
		},
	}
}

func (c *Client) Close(_ context.Context) error {
	return nil
}

func (c *Client) CreateTopic(ctx context.Context, path scheme.Path, opts ...options.CreateTopicOption) error {
	req := rawtopic.CreateTopicRequest{}
	req.OperationParams = c.defaultOperationParams
	req.Path = path.String()

	for _, f := range opts {
		f(&req)
	}

	_, err := c.rawClient.CreateTopic(ctx, req)
	return err
}

func (c *Client) DescribeTopic(ctx context.Context, path scheme.Path) (interface{}, error) {
	req := rawtopic.DescribeTopicRequest{
		OperationParams: c.defaultOperationParams,
		Path:            path.String(),
	}

	return c.rawClient.DescribeTopic(ctx, req)
}

func (c *Client) DropTopic(ctx context.Context, path scheme.Path, opts ...options.DropTopicOption) error {
	req := rawtopic.DropTopicRequest{}
	req.OperationParams = c.defaultOperationParams
	req.Path = path.String()

	for _, f := range opts {
		f(&req)
	}
	_, err := c.rawClient.DropTopic(ctx, req)
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

func operationParamsFromConfig(operationParams *rawydb.OperationParams, cfg *config.Config) {
	operationParams.OperationMode = rawydb.OperationParamsModeSync

	operationParams.OperationTimeout.Value = cfg.OperationTimeout()
	operationParams.OperationTimeout.HasValue = operationParams.OperationTimeout.Value != 0

	operationParams.CancelAfter.Value = cfg.OperationCancelAfter()
	operationParams.CancelAfter.HasValue = operationParams.CancelAfter.Value != 0
}
