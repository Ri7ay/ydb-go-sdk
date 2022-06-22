package topicreader

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type ReaderOption func(cfg *readerConfig)

func WithBatchOptions(options ...ReadBatchOption) ReaderOption {
	return func(cfg *readerConfig) {
		batchOptions := newReadMessageBatchOptions()
		for _, opt := range options {
			opt(&batchOptions)
		}
		cfg.DefaultBatchConfig = batchOptions
	}
}

func WithBatchMaxTimeLag(duration time.Duration) ReaderOption {
	panic("not implemented")
}

func WithBaseContext(ctx context.Context) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.BaseContext = ctx
	}
}

func WithMaxMemoryUsageBytes(size int) ReaderOption {
	panic("not implemented")
}

func WithRetry(backoff backoff.Backoff) ReaderOption {
	panic("not implemented")
}

type CommitMode int

const (
	// CommitModeAsync - commit return true if commit success add to internal send buffer (but not sent to server)
	// now it is grpc buffer, in feature it may be internal sdk buffer
	CommitModeAsync CommitMode = iota // default

	// CommitModeNone - reader will not be commit operation
	CommitModeNone

	// CommitModeSync - commit return true when sdk receive ack of commit from server
	// The mode needs strong ordering client code for prevent deadlock.
	// Example:
	// Good:
	// CommitOffset(1)
	// CommitOffset(2)
	//
	// Deadlock:
	// CommitOffset(2) - server will wait commit offset 1 before send ack about offset 1 and 2 committed.
	// CommitOffset(1)
	CommitModeSync
)

func (m CommitMode) commitsEnabled() bool {
	return m != CommitModeNone
}

func WithCommitMode(mode CommitMode) ReaderOption {
	panic("not implemented")
}

func WithReadSelector(readSelector ...ReadSelector) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.ReadSelectors = make([]ReadSelector, len(readSelector))
		for i := range readSelector {
			cfg.ReadSelectors[i] = readSelector[i].clone()
		}
	}
}

type GetPartitionStartOffsetRequest struct {
	Session *PartitionSession
}
type GetPartitionStartOffsetResponse struct {
	startOffset     rawtopicreader.Offset
	startOffsetUsed bool
}

func (r *GetPartitionStartOffsetResponse) StartFrom(offset int64) {
	r.startOffset.FromInt64(offset)
	r.startOffsetUsed = true
}

type GetPartitionStartOffsetFunc func(
	ctx context.Context,
	req GetPartitionStartOffsetRequest,
) (res GetPartitionStartOffsetResponse, err error)

func WithGetPartitionStartOffset(f GetPartitionStartOffsetFunc) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.GetPartitionStartOffsetCallback = f
	}
}

func WithTracer(tracer trace.TopicReader) ReaderOption {
	return func(cfg *readerConfig) {
		cfg.Tracer = cfg.Tracer.Compose(tracer)
	}
}
