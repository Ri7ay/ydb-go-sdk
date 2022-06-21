package topicreader

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

var (
	errUnconnected  = xerrors.Retryable(errors.New("first connection attempt not finished"))
	errReaderClosed = errors.New("reader closed")
)

//nolint:lll
//go:generate mockgen -destination raw_stream_reader_mock_test.go -package topicreader -write_package_comment=false . RawStreamReader

type RawStreamReader interface {
	Recv() (rawtopicreader.ServerMessage, error)
	Send(mess rawtopicreader.ClientMessage) error
	CloseSend() error
}

type TopicSteamReaderConnect func(ctx context.Context) (RawStreamReader, error)

type Reader struct {
	reader             batchedStreamReader
	defaultBatchConfig readMessageBatchOptions
	oneMessage         chan *Message

	messageReaderLoopOnce sync.Once
	background            backgroundworkers.BackgroundWorker
}

type readMessageBatchOptions struct {
	batcherGetOptions
}

func (o *readMessageBatchOptions) clone() readMessageBatchOptions {
	return *o
}

func newReadMessageBatchOptions() readMessageBatchOptions {
	return readMessageBatchOptions{}
}

func NewReader(
	connectCtx context.Context,
	connector TopicSteamReaderConnect,
	consumer string,
	readSelectors []ReadSelector,
	opts ...ReaderOption,
) *Reader {
	readerConfig := convertNewParamsToStreamConfig(consumer, readSelectors, opts...)
	readerConnector := func(ctx context.Context) (batchedStreamReader, error) {
		stream, err := connector(ctx)
		if err != nil {
			return nil, err
		}

		return newTopicStreamReader(stream, readerConfig)
	}

	res := &Reader{
		reader:             newReaderReconnector(connectCtx, readerConnector),
		defaultBatchConfig: readerConfig.DefaultBatchConfig,
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
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (Batch, error) {
	readOptions := r.defaultBatchConfig.clone()

	for _, optFunc := range opts {
		optFunc(&readOptions)
	}

forReadBatch:
	for {
		if err := ctx.Err(); err != nil {
			return Batch{}, err
		}

		batch, err := r.reader.ReadMessageBatch(ctx, readOptions)
		if err != nil {
			return Batch{}, err
		}

		if batch.Context().Err() != nil {
			continue forReadBatch
		}
		return batch, nil
	}
}

type ReadSelector struct {
	Stream     scheme.Path
	Partitions []int64
	ReadFrom   time.Time
}

func (s ReadSelector) clone() ReadSelector {
	dst := s

	dst.Partitions = make([]int64, len(s.Partitions))
	copy(dst.Partitions, s.Partitions)

	return dst
}

func (r *Reader) ReadMessage(ctx context.Context) (Message, error) {
	res, err := r.ReadMessageBatch(ctx, readExplicitMessagesCount(1))
	if err != nil {
		return Message{}, err
	}

	return res.Messages[0], nil
}

func (r *Reader) Commit(ctx context.Context, offset CommitableByOffset) error {
	return r.CommitBatch(ctx, CommitBatchFromCommitableByOffset(offset))
}

func (r *Reader) CommitBatch(ctx context.Context, commitBatch CommitBatch) error {
	return r.reader.Commit(ctx, commitBatch)
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...Message) error {
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

func convertNewParamsToStreamConfig(consumer string, readSelectors []ReadSelector, opts ...ReaderOption) (cfg topicStreamReaderConfig) {
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
