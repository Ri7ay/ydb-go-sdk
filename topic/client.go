package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type Client interface {
	Close(context.Context) error

	AlterTopic(ctx context.Context, path scheme.Path, opts ...options.AlterTopicOption) error

	CreateTopic(ctx context.Context, path scheme.Path, opts ...options.CreateTopicOption) error

	DropTopic(ctx context.Context, path scheme.Path, opts ...options.DropTopicOption) error

	StreamRead(
		consumer string, // may be todo optional - for set in topic config, with optional redefine
		readSelectors []topicreader.ReadSelector,
		opts ...topicreader.ReaderOption,
	) (*topicreader.Reader, error)
}
