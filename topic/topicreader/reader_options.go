package topicreader

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ReaderOption func(cfg *topicStreamReaderConfig)

// WithBatchPreferCount set internal prefer count for ReadMessageBatch
// result messages count may be smaller (when max lag timeout is end) or greater (some extra messages from last server batch)
func WithBatchPreferCount(preferCount int) ReaderOption {
	panic("not implemented")
}

// WithBatchMaxCount set maximum limit of messages in ReadMessageBatch method
// see WithBatchPreferCount if you don't need strong up limit
func WithBatchMaxCount(maxCount int) ReaderOption {
	panic("not implemented")
}

func WithBatchMaxTimeLag(duration time.Duration) ReaderOption {
	panic("not implemented")
}

func WithBaseContext(ctx context.Context) ReaderOption {
	return func(cfg *topicStreamReaderConfig) {
		cfg.BaseContext = ctx
	}
}

func WithMaxMemoryUsageBytes(size int) ReaderOption {
	panic("not implemented")
}

func WithRetry(backoff backoff.Backoff) ReaderOption {
	panic("not implemented")
}

func WithSyncCommit(enabled bool) ReaderOption {
	panic("not implemented")
}

func WithReadSelector(readSelector ReadSelector) ReaderOption {
	return func(cfg *topicStreamReaderConfig) {
		cfg.ReadSelectors = append(cfg.ReadSelectors, readSelector.clone())
	}
}

type GetPartitionStartOffsetRequest struct {
	Topic       string
	PartitionID int64
}
type GetPartitionStartOffsetResponse struct {
	readOffset   rawtopicreader.Offset
	commitOffset rawtopicreader.Offset

	readOffsetUsed   bool
	commitOffsetUsed bool
}

func (r *GetPartitionStartOffsetResponse) StartWithAutoCommitFrom(offset int64) {
	r.readOffset.FromInt64(offset)
	r.readOffsetUsed = true
}

func WithGetPartitionStartOffset(f func(ctx context.Context, req GetPartitionStartOffsetRequest) (res GetPartitionStartOffsetResponse, err error)) ReaderOption {
	panic("not implemented")
}

func WithTracer(tracer trace.TopicReader) ReaderOption {
	return func(cfg *topicStreamReaderConfig) {
		cfg.Tracer = cfg.Tracer.Compose(tracer)
	}
}
