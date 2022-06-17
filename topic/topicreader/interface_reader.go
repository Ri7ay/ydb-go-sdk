package topicreader

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topicstream/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ReaderOption func()

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
	panic("not implemented")
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
	panic("not implemented")
}

type GetPartitionStartOffsetRequest struct {
	Topic       string
	PartitionID int64
}
type GetPartitionStartOffsetResponse struct {
	readOffset   pqstreamreader.Offset
	commitOffset pqstreamreader.Offset

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

func WithTracer(reader trace.TopicReader) ReaderOption {
	panic("not implemented")
}
