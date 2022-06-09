package pq

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

type ReaderExample struct {
	stream *ReadStream
}

type ReaderConfig struct {
	PartitionStreamDestroyTimeout time.Duration // rekby: зачем?

	UserHandlers struct {
		CreatePartitionStream  StartReadingValidator
		DestroyPartitionStream StopReadingHandler
	}
	// и прочие полезные опции вроде размера inflight ...

}

type readerOption func()

// WithBatchPreferCount set internal prefer count for ReadMessageBatch
// result messages count may be smaller (when max lag timeout is end) or greater (some extra messages from last server batch)
func WithBatchPreferCount(preferCount int) readerOption {
	panic("not implemented")
}

// WithBatchMaxCount set maximum limit of messages in ReadMessageBatch method
// see WithBatchPreferCount if you don't need strong up limit
func WithBatchMaxCount(maxCount int) readerOption {
	panic("not implemented")
}

func WithBatchMaxTimeLag(duration time.Duration) {
	panic("not implemented")
}

func WithBaseContext(ctx context.Context) readerOption {
	panic("not implemented")
}

func WithMaxMemoryUsageBytes(size int) readerOption {
	panic("not implemented")
}

func WithRetry(backoff backoff.Backoff) readerOption {
	panic("not implemented")
}

func WithSyncCommit(enabled bool) readerOption {
	panic("not implemented")
}

func WithReadSelector(readSelector ReadSelector) readerOption {
	panic("not implemented")
}

func NewReaderExample(consumer string, readSelectors []ReadSelector) (*ReaderExample, error) {
	return nil, nil
}

func (r *ReaderExample) Close() error {
	// check alive
	// stop send
	// stop read
	// cancel all allocations -> cancel all batches and messages
	return nil
}

func (r *ReaderExample) CloseWithContext(ctx context.Context) {
	panic("not implemented")
}

// ReadBatchOption для различных пожеланий к батчу вроде WithMaxMessages(int)
type ReadBatchOption func()

func (r *ReaderExample) ReadMessageBatch(context.Context, ...ReadBatchOption) (Batch, error) {
	return Batch{}, nil
}

func (r *ReaderExample) ReadMessage(context.Context) (Message, error) {
	return Message{}, nil
}

func (r *ReaderExample) Commit(ctx context.Context, commits ...CommitableByOffset) error {
	batch := make(CommitBatch, 0, len(commits))
	batch.Append(commits...)
	return r.CommitBatch(ctx, batch)
}

func (r *ReaderExample) CommitBatch(ctx context.Context, batch CommitBatch) error {
	panic("not implemented")
}

func DeferMessageCommits(msg ...Message) []CommitOffset {
	// кажется тут можно сразу собрать интервалы оффсетов
	result := make([]CommitOffset, len(msg))
	for i := range msg {
		result[i] = msg[i].CommitOffset
	}
	return result
}

func (r *ReaderExample) Stats() ReaderStats {
	// Нужна настройка и периодические запросы PartitionStreamState
	// Возвращать из памяти
	return ReaderStats{}
}

func (r *ReaderExample) PartitionStreamState(context.Context, PartitionStream) (PartitionStreamState, error) {
	// метод для запроса статуса конкретного стрима с сервера, синхронный
	return PartitionStreamState{}, nil
}

type ReaderStats struct {
	PartitionStreams []PartitionStream
	// other internal stats
}

type StartReadingValidator interface {
	ValidateReadStart(context.Context, *CreatePartitionStreamResponse) error
}

type StopReadingHandler interface {
	OnReadStop(context.Context, DestroyPartitionStreamRequest)
}
