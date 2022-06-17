package topic

import (
	"context"
	"errors"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topicstream/pqstreamreader"
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
	reader     topicStreamReader
	oneMessage chan *Message

	messageReaderLoopOnce sync.Once
	background            backgroundworkers.BackgroundWorker
}

type ReadMessageBatchOptions struct {
	maxMessages int
}

func NewReader(connectCtx context.Context, connector TopicSteamReaderConnect, consumer string, readSelectors []ReadSelector, opts ...readerOption) *Reader {
	readerConfig := convertNewParamsToStreamConfig(consumer, readSelectors, opts...)
	readerConnector := func(ctx context.Context) (topicStreamReader, error) {
		stream, err := connector(ctx)
		if err != nil {
			return nil, err
		}

		return newTopicStreamReader(stream, readerConfig)
	}

	res := &Reader{
		reader: newReaderReconnector(connectCtx, readerConnector),
	}
	res.initChannels()

	return res
}

func (r *Reader) initChannels() {
	r.oneMessage = make(chan *Message)
}

func (r *Reader) Close() error {
	err := r.background.Close(context.TODO())
	r.reader.Close(context.TODO(), errReaderClosed)
	return err
}

// ReadMessageBatch read batch of messages.
// Batch is collection of messages, which can be atomically committed
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	var readOptions ReadMessageBatchOptions
	for _, optFunc := range opts {
		optFunc(&readOptions)
	}

forReadBatch:
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		batch, err := r.reader.ReadMessageBatch(ctx, readOptions)
		if err != nil {
			return nil, err
		}

		if batch.Context().Err() != nil {
			continue forReadBatch
		}
		return batch, nil
	}
}

// ReadBatchOption для различных пожеланий к батчу вроде WithMaxMessages(int)
type ReadBatchOption func(options *ReadMessageBatchOptions)

func ReadMaxMessagesCount(count int) ReadBatchOption {
	return func(options *ReadMessageBatchOptions) {
		options.maxMessages = count
	}
}

func (r *Reader) ReadMessage(ctx context.Context) (*Message, error) {
	res, err := r.ReadMessageBatch(ctx, ReadMaxMessagesCount(1))
	if err != nil {
		return nil, err
	}

	return &res.Messages[0], nil
}

func (r *Reader) Commit(ctx context.Context, offset CommitableByOffset) error {
	return r.CommitBatch(ctx, CommitBatchFromCommitableByOffset(offset))
}

func (r *Reader) CommitBatch(ctx context.Context, commitBatch CommitBatch) error {
	return r.reader.Commit(ctx, commitBatch)
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...*Message) error {
	return r.CommitBatch(ctx, CommitBatchFromMessages(messages...))
}

func (r *Reader) messageReaderLoop(ctx context.Context) {
	ctxDone := ctx.Done()
	for {
		if ctx.Err() != nil {
			return
		}

		batch, err := r.ReadMessageBatch(ctx)
		// TODO: log
		if err != nil {
			continue
		}

		for index := range batch.Messages {
			select {
			case r.oneMessage <- &batch.Messages[index]:
				// pass
			case <-ctxDone:
				// stop work
				return
			}
		}
	}
}

func convertNewParamsToStreamConfig(consumer string, readSelectors []ReadSelector, opts ...readerOption) (cfg topicStreamReaderConfig) {
	cfg = newTopicStreamReaderConfig()
	cfg.Consumer = consumer

	// make own copy, for prevent changing internal states if readSelectors will change outside
	cfg.ReadSelectors = make([]ReadSelector, len(readSelectors))
	for i := range readSelectors {
		cfg.ReadSelectors[i] = readSelectors[i].clone()
	}

	if len(opts) > 0 {
		panic("not implemented")
	}

	return cfg
}
