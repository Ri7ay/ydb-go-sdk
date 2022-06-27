package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type Client interface {
	Close(context.Context) error

	StartRead(
		connectionCtx context.Context,
		consumer string,
		readSelectors []topicreader.ReadSelector,
		opts ...topicreader.ReaderOption,
	) (*topicreader.Reader, error)
}
