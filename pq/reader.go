package pq

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
)

type ReaderStream interface {
	Recv() (pqstreamreader.ServerMessage, error)
	Send(mess pqstreamreader.ClientMessage) error
	Close() error
}

type ReaderStreamConnector interface {
	Connect(ctx context.Context) (ReaderStream, error)
}

type Reader struct {
	connector ReaderStreamConnector

	m         sync.RWMutex
	streamVal *readerPump
}

func NewReader(consumer string, readSelectors []ReadSelector) *Reader {
	panic("not implemented")
}

func (r *Reader) stream() *readerPump {
	r.m.RLock()
	defer r.m.RUnlock()

	return r.streamVal
}

func (r *Reader) ReadMessageBatch(context.Context, ...ReadBatchOption) (Batch, error) {
	panic("not implemented")
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	return Message{}, nil
}

func (r *Reader) Commit(ctx context.Context, offset CommitableByOffset) error {
	batch := make(CommitBatch, 0, 1)
	batch.Append(offset)
	return r.stream().Commit(ctx, batch)
}

func (r *Reader) CommitOffsets(ctx context.Context, message ...CommitOffset) {
	panic("not implemented")
}

func (r *Reader) CommitMessages(ctx context.Context, message ...Message) error {
	panic("not implemented")
}
