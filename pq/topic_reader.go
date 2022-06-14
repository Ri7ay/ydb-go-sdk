package pq

import (
	"context"
	"errors"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	errUnconnected  = xerrors.Retryable(errors.New("first connection attempt not finished"))
	errReaderClosed = errors.New("reader closed")
)

type ReaderStream interface {
	Recv() (pqstreamreader.ServerMessage, error)
	Send(mess pqstreamreader.ClientMessage) error
	Close() error
}

type TopicSteamReaderConnect func(ctx context.Context) (ReaderStream, error)

type Reader struct {
	reader *readerReconnector
}

type topicStreamReader interface {
	ReadMessageBatch(ctx context.Context) (*Batch, error)
	Commit(ctx context.Context, offset CommitBatch) error
	Close(ctx context.Context, err error)
}

func NewReader(connectCtx context.Context, connector TopicSteamReaderConnect, consumer string, readSelectors []ReadSelector, opts ...readerOption) *Reader {
	readerConnector := func(ctx context.Context) (topicStreamReader, error) {
		stream, err := connector(ctx)
		if err != nil {
			return nil, err
		}

		// TODO
		return newTopicStreamReader(stream, topicStreamReaderConfig{
			BaseContext:        context.Background(),
			CredUpdateInterval: time.Hour,
		})
	}

	res := &Reader{
		reader: newReaderReconnector(connectCtx, readerConnector),
	}

	return res
}

func (r *Reader) Close() error {
	r.reader.Close(nil, errReaderClosed)
	return nil
}

// ReadMessageBatch read batch of messages.
// Batch is collection of messages, which can be atomically committed
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	return r.reader.ReadMessageBatch(ctx)
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	panic("not implemented")
}

func (r *Reader) Commit(ctx context.Context, offset CommitableByOffset) error {
	return r.CommitBatch(ctx, CommitBatchFromCommitableByOffset(offset))
}

func (r *Reader) CommitBatch(ctx context.Context, commitBatch CommitBatch) error {
	panic("not implemented")
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...Message) error {
	return r.CommitBatch(ctx, CommitBatchFromMessages(messages...))
}
