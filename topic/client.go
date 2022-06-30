package topic

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

type Client interface {
	Close(context.Context) error

	StartRead(
		consumer string, // may be todo optional - for set in topic config, with optional redefine
		readSelectors []topicreader.ReadSelector,
		opts ...topicreader.ReaderOption,
	) (*topicreader.Reader, error)
}
