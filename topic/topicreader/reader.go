package topicreader

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

var (
	errUnconnected  = xerrors.Retryable(errors.New("first connection attempt not finished"))
	ErrReaderClosed = errors.New("reader closed")
)

//nolint:lll
//go:generate mockgen -destination raw_topic_reader_stream_mock_test.go -package topicreader -write_package_comment=false . RawTopicReaderStream

type RawTopicReaderStream interface {
	Recv() (rawtopicreader.ServerMessage, error)
	Send(mess rawtopicreader.ClientMessage) error
	CloseSend() error
}

// TopicSteamReaderConnect connect to grpc stream
// when connectionCtx closed stream must stop work and return errors for all methods
type TopicSteamReaderConnect func(connectionCtx context.Context) (RawTopicReaderStream, error)

type Reader struct {
	reader             batchedStreamReader
	defaultBatchConfig readMessageBatchOptions
	oneMessage         chan *Message

	messageReaderLoopOnce sync.Once
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
	connector TopicSteamReaderConnect,
	consumer string,
	readSelectors []ReadSelector,
	opts ...ReaderOption,
) *Reader {
	cfg := convertNewParamsToStreamConfig(consumer, readSelectors, opts...)
	readerConnector := func(ctx context.Context) (batchedStreamReader, error) {
		stream, err := connector(ctx)
		if err != nil {
			return nil, err
		}

		return newTopicStreamReader(stream, cfg.topicStreamReaderConfig)
	}

	res := &Reader{
		reader:             newReaderReconnector(readerConnector),
		defaultBatchConfig: cfg.DefaultBatchConfig,
	}
	res.initChannels()

	return res
}

func (r *Reader) initChannels() {
	r.oneMessage = make(chan *Message)
}

func (r *Reader) Close() error {
	r.reader.Close(context.TODO(), ErrReaderClosed)
	// TODO: err
	return nil
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
	ReadFrom   time.Time     // zero value mean skip read from filter
	MaxTimeLag time.Duration // 0 mean skip time lag filter
}

func (s ReadSelector) clone() ReadSelector {
	dst := s

	dst.Partitions = make([]int64, len(s.Partitions))
	copy(dst.Partitions, s.Partitions)

	return dst
}

var readOneMessageOption = readExplicitMessagesCount(1)

func (r *Reader) ReadMessage(ctx context.Context) (Message, error) {
	res, err := r.ReadMessageBatch(ctx, readOneMessageOption)
	if err != nil {
		return Message{}, err
	}

	return res.Messages[0], nil
}

func (r *Reader) Commit(ctx context.Context, offset CommittedBySingleRange) error {
	return r.reader.Commit(ctx, offset.getCommitRange())
}

// CommitRanges commit all from commitRanges
// commitRanges will reset before return and can use for accumulate new commits
func (r *Reader) CommitRanges(ctx context.Context, commitRanges *CommitRages) error {
	defer commitRanges.Reset()

	commitRanges.optimize()

	commitErrors := make(chan error, commitRanges.len())

	var wg sync.WaitGroup

	commit := func(cr commitRange) {
		defer wg.Done()
		commitErrors <- r.Commit(ctx, cr)
	}

	wg.Add(commitRanges.len())
	for _, cr := range commitRanges.ranges {
		go commit(cr)
	}
	wg.Wait()
	close(commitErrors)

	// return first error
	for err := range commitErrors {
		if err != nil {
			return err
		}
	}

	return nil
}

type readerConfig struct {
	DefaultBatchConfig readMessageBatchOptions
	topicStreamReaderConfig
}

func convertNewParamsToStreamConfig(consumer string, readSelectors []ReadSelector, opts ...ReaderOption) (cfg readerConfig) {
	cfg.topicStreamReaderConfig = newTopicStreamReaderConfig()
	cfg.Consumer = consumer

	// make own copy, for prevent changing internal states if readSelectors will change outside
	cfg.ReadSelectors = make([]ReadSelector, len(readSelectors))
	for i := range readSelectors {
		cfg.ReadSelectors[i] = readSelectors[i].clone()
	}

	for _, f := range opts {
		f(&cfg)
	}

	return cfg
}
