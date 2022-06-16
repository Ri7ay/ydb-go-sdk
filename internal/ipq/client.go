package ipq

import (
	"context"
	"fmt"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	pqproto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/config"
)

// var _ persqueue.Client = &Client{}

type Client struct {
	config  config.Config
	service Ydb_PersQueue_V1.PersQueueServiceClient

	topicsPrefix string
}

type Connector interface {
	grpc.ClientConnInterface
	Name() string
}

func New(cc Connector, options []config.Option) *Client {
	c := Client{
		config:  config.New(options...),
		service: Ydb_PersQueue_V1.NewPersQueueServiceClient(cc),
	}
	c.topicsPrefix = fmt.Sprintf("%s/PQ/rt3.%s--", cc.Name(), c.config.Cluster())

	return &c
}

func (c *Client) Close(_ context.Context) error {
	return nil
}

func (c *Client) DescribeStream(ctx context.Context, stream scheme.Path) (info topic.StreamInfo, err error) {
	err = retry.Retry(ctx, func(ctx context.Context) (err error) {
		info, err = c.describeStream(ctx, stream)
		return xerrors.WithStackTrace(err)
	}, retry.WithIdempotent(true))
	return info, err
}

func (c *Client) DropStream(ctx context.Context, stream scheme.Path) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		return c.dropStream(ctx, stream)
	})
}

func (c *Client) CreateStream(ctx context.Context, stream scheme.Path,
	settings topic.StreamSettings, opts ...topic.StreamOption,
) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		return c.createStream(ctx, stream, settings, opts...)
	})
}

func (c *Client) AlterStream(ctx context.Context, stream scheme.Path,
	settings topic.StreamSettings, opts ...topic.StreamOption,
) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		return c.alterStream(ctx, stream, settings, opts...)
	})
}

func (c *Client) AddReadRule(ctx context.Context, stream scheme.Path, rule topic.ReadRule) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		return c.addReadRule(ctx, stream, rule)
	})
}

func (c *Client) RemoveReadRule(ctx context.Context, stream scheme.Path, consumer topic.Consumer) error {
	return retry.Retry(ctx, func(ctx context.Context) error {
		return c.removeReadRule(ctx, stream, consumer)
	})
}

func (c *Client) describeStream(ctx context.Context, stream scheme.Path) (topic.StreamInfo, error) {
	var result topic.StreamInfo

	response, err := c.service.DescribeTopic(ctx,
		&pqproto.DescribeTopicRequest{
			Path:            c.streamToTopic(stream),
			OperationParams: operation.Sync(ctx, c.config),
		},
	)
	if err != nil {
		return result, xerrors.WithStackTrace(err)
	}

	var opResult pqproto.DescribeTopicResult
	err = proto.Unmarshal(response.GetOperation().GetResult().GetValue(), &opResult)
	if err != nil {
		return result, xerrors.WithStackTrace(err)
	}
	result.Entry.From(opResult.Self)
	result.StreamSettings.From(opResult.Settings)

	result.ReadRules = make([]topic.ReadRule, len(opResult.Settings.ReadRules))
	for i := range result.ReadRules {
		result.ReadRules[i].From(opResult.Settings.ReadRules[i])
	}
	result.RemoteMirrorRule.From(opResult.Settings.RemoteMirrorRule)

	return result, nil
}

func (c *Client) dropStream(ctx context.Context, stream scheme.Path) error {
	_, err := c.service.DropTopic(ctx,
		&pqproto.DropTopicRequest{
			Path:            c.streamToTopic(stream),
			OperationParams: operation.Sync(ctx, c.config),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) createStream(ctx context.Context, stream scheme.Path,
	settings topic.StreamSettings, opts ...topic.StreamOption,
) error {
	_, err := c.service.CreateTopic(ctx,
		&pqproto.CreateTopicRequest{
			Path:            c.streamToTopic(stream),
			OperationParams: operation.Sync(ctx, c.config),
			Settings:        encodeTopicSettings(settings, opts...),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) alterStream(ctx context.Context, stream scheme.Path,
	settings topic.StreamSettings, opts ...topic.StreamOption,
) error {
	_, err := c.service.AlterTopic(ctx,
		&pqproto.AlterTopicRequest{
			Path:            c.streamToTopic(stream),
			OperationParams: operation.Sync(ctx, c.config),
			Settings:        encodeTopicSettings(settings, opts...),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) addReadRule(ctx context.Context, stream scheme.Path, rule topic.ReadRule) error {
	_, err := c.service.AddReadRule(ctx,
		&pqproto.AddReadRuleRequest{
			Path:            c.streamToTopic(stream),
			OperationParams: operation.Sync(ctx, c.config),
			ReadRule:        encodeReadRule(rule),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) removeReadRule(ctx context.Context, stream scheme.Path, consumer topic.Consumer) error {
	_, err := c.service.RemoveReadRule(ctx,
		&pqproto.RemoveReadRuleRequest{
			Path:            c.streamToTopic(stream),
			OperationParams: operation.Sync(ctx, c.config),
			ConsumerName:    string(consumer),
		},
	)
	return xerrors.WithStackTrace(err)
}

func (c *Client) streamToTopic(p scheme.Path) string {
	topic := strings.TrimPrefix(string(p), "/")

	if parts := strings.Split(topic, "/"); len(parts) > 1 {
		dir := strings.Join(parts[:len(parts)-1], "@")
		topic = strings.Join([]string{dir, parts[len(parts)-1]}, "--")
	}
	return fmt.Sprintf("%s%s", c.topicsPrefix, topic)
}
