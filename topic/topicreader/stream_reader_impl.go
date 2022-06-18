package topicreader

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	errGracefulShutdownPartition = xerrors.Wrap(errors.New("ydb: graceful shutdown partition"))
	errPartitionStopped          = xerrors.Wrap(errors.New("ydb: partition stopped"))
)

type partitionSessionID = rawtopicreader.PartitionSessionID

type topicStreamReaderImpl struct {
	cfg    topicStreamReaderConfig
	ctx    context.Context
	cancel xcontext.CancelErrFunc

	freeBytes         chan int
	stream            ReaderStream
	sessionController partitionSessionStorage

	readResponsesParseSignal chan struct{}
	batcher                  *batcher

	m       xsync.RWMutex
	err     error
	started bool
}

type topicStreamReaderConfig struct {
	BaseContext          context.Context
	BufferSizeProtoBytes int
	Cred                 credentials.Credentials
	CredUpdateInterval   time.Duration
	Consumer             string
	ReadSelectors        []ReadSelector
}

func (cfg *topicStreamReaderConfig) initMessage() rawtopicreader.ClientMessage {
	// TODO improve
	res := &rawtopicreader.InitRequest{
		Consumer: cfg.Consumer,
	}

	res.TopicsReadSettings = make([]rawtopicreader.TopicReadSettings, len(cfg.ReadSelectors))
	for i, selector := range cfg.ReadSelectors {
		res.TopicsReadSettings[i] = rawtopicreader.TopicReadSettings{
			Path:         selector.Stream.String(),
			PartitionsID: selector.Partitions,
			ReadFrom:     selector.ReadFrom,
		}
	}

	return res
}

func newTopicStreamReaderConfig() topicStreamReaderConfig {
	return topicStreamReaderConfig{
		BaseContext:          context.Background(),
		BufferSizeProtoBytes: 1024 * 1024,
		CredUpdateInterval:   time.Hour,
	}
}

func newTopicStreamReader(stream ReaderStream, cfg topicStreamReaderConfig) (*topicStreamReaderImpl, error) {
	stopPump, cancel := xcontext.WithErrCancel(cfg.BaseContext)

	res := &topicStreamReaderImpl{
		cfg:                      cfg,
		ctx:                      stopPump,
		freeBytes:                make(chan int, 1),
		stream:                   stream,
		cancel:                   cancel,
		readResponsesParseSignal: make(chan struct{}, 1),
		batcher:                  newBatcher(),
	}
	res.sessionController.init(res.ctx, res)
	res.freeBytes <- cfg.BufferSizeProtoBytes
	err := res.start()
	if err == nil {
		return res, nil
	}
	return nil, err
}

func (r *topicStreamReaderImpl) ReadMessageBatch(
	ctx context.Context,
	opts readMessageBatchOptions,
) (batch Batch, _ error) {
	ctx, cancel := xcontext.Merge(ctx, r.ctx)
	defer func() {
		cancel(errors.New("ydb: topic stream read message batch competed"))
		r.freeBufferFromMessages(batch)
	}()

	return r.consumeMessagesUntilBatch(ctx, opts)
}

func (r *topicStreamReaderImpl) consumeMessagesUntilBatch(
	ctx context.Context,
	opts readMessageBatchOptions,
) (Batch, error) {
	item, err := r.batcher.Pop(ctx, opts.batcherGetOptions)
	if err != nil {
		return Batch{}, err
	}

	switch {
	case item.IsBatch():
		return item.Batch, nil
	default:
		return Batch{}, xerrors.NewWithIssues("ydb: unexpected item type from batch")
	}
}

func (r *topicStreamReaderImpl) Commit(ctx context.Context, offset CommitBatch) error {
	req := &rawtopicreader.CommitOffsetRequest{
		CommitOffsets: offset.toPartitionsOffsets(),
	}
	return r.stream.Send(req)
}

func (r *topicStreamReaderImpl) send(mess rawtopicreader.ClientMessage) error {
	err := r.stream.Send(mess)
	if err != nil {
		r.Close(nil, err)
	}
	return err
}

func (r *topicStreamReaderImpl) start() error {
	if err := r.setStarted(); err != nil {
		return err
	}

	if err := r.initSession(); err != nil {
		r.Close(nil, err)
	}

	go r.readMessagesLoop()
	go r.dataRequestLoop()
	go r.updateTokenLoop()
	return nil
}

func (r *topicStreamReaderImpl) setStarted() error {
	r.m.Lock()
	defer r.m.Unlock()

	if r.started {
		return xerrors.WithStackTrace(errors.New("already started"))
	}

	r.started = true
	return nil
}

func (r *topicStreamReaderImpl) initSession() error {
	if err := r.stream.Send(r.cfg.initMessage()); err != nil {
		return err
	}

	resp, err := r.stream.Recv()
	if err != nil {
		return err
	}

	if status := resp.StatusData(); !status.Status.IsSuccess() {
		return xerrors.WithStackTrace(fmt.Errorf("bad status on initial error: %v (%v)", status.Status, status.Issues))
	}

	_, ok := resp.(*rawtopicreader.InitResponse)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf("bad message type on session init: %v (%v)", resp, reflect.TypeOf(resp)))
	}

	// TODO: log session id
	return nil
}

func (r *topicStreamReaderImpl) readMessagesLoop() {
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	defer cancel(xerrors.NewWithIssues("ydb: topic stream reader messages loop finished"))

	for {
		serverMessage, err := r.stream.Recv()
		if err != nil {
			r.Close(nil, err)
			return
		}

		status := serverMessage.StatusData()
		if !status.Status.IsSuccess() {
			// TODO: actualize error message
			r.Close(nil, xerrors.WithStackTrace(fmt.Errorf("bad status from pq grpc stream: %v", status.Status)))
		}

		switch m := serverMessage.(type) {
		case *rawtopicreader.ReadResponse:
			if err := r.onReadResponse(m); err != nil {
				r.Close(ctx, err)
			}
		case *rawtopicreader.StartPartitionSessionRequest:
			if err = r.sessionController.onStartPartitionSessionRequest(m); err != nil {
				r.Close(ctx, err)
				return
			}
		case *rawtopicreader.StopPartitionSessionRequest:
			if err = r.sessionController.onStopPartitionSessionRequest(m); err != nil {
				r.Close(ctx, err)
				return
			}
		case *rawtopicreader.CommitOffsetResponse:
			if err = r.onCommitResponse(m); err != nil {
				r.Close(ctx, err)
				return
			}

		case *rawtopicreader.UpdateTokenResponse:
			// skip
		default:
			// TODO: remove before release
			r.Close(ctx, xerrors.WithStackTrace(fmt.Errorf("receive unexpected message: %#v (%v)", m, reflect.TypeOf(m))))
		}
	}
}

func (r *topicStreamReaderImpl) dataRequestLoop() {
	if r.ctx.Err() != nil {
		return
	}

	doneChan := r.ctx.Done()

	for {
		select {
		case <-doneChan:
			r.Close(nil, r.ctx.Err())
			return

		case free := <-r.freeBytes:
			sum := free

			// consume all messages from order and compress it to one data request
		forConsumeRequests:
			for {
				select {
				case free = <-r.freeBytes:
					sum += free
				default:
					break forConsumeRequests
				}
			}

			err := r.stream.Send(&rawtopicreader.ReadRequest{BytesSize: sum})
			if err != nil {
				r.Close(nil, err)
			}
		}
	}
}

func (r *topicStreamReaderImpl) freeBufferFromMessages(batch Batch) {
	size := 0
	for messageIndex := range batch.Messages {
		size += batch.Messages[messageIndex].bufferBytesAccount
	}
	r.freeBytes <- size
}

func (r *topicStreamReaderImpl) updateTokenLoop() {
	ticker := time.NewTicker(r.cfg.CredUpdateInterval)
	defer ticker.Stop()

	readerCancel := r.ctx.Done()
	for {
		select {
		case <-readerCancel:
			return
		case <-ticker.C:
			tokenCtx, cancel := context.WithCancel(r.ctx)
			err := r.updateToken(tokenCtx)
			cancel()
			if err != nil {
				// TODO: log
			}
		}
	}
}

func (r *topicStreamReaderImpl) onReadResponse(mess *rawtopicreader.ReadResponse) error {
	batchesCount := 0
	for i := range mess.PartitionData {
		batchesCount += len(mess.PartitionData[i].Batches)
	}

	var batches []Batch
	for pIndex := range mess.PartitionData {
		p := &mess.PartitionData[pIndex]
		session, err := r.sessionController.Get(p.PartitionSessionID)
		if err != nil {
			return err
		}

		for bIndex := range p.Batches {
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}

			batch, err := NewBatchFromStream(session, p.Batches[bIndex])
			if err != nil {
				return err
			}
			batches = append(batches, batch)
		}
	}

	if err := splitBytesByMessagesInBatches(batches, mess.BytesSize); err != nil {
		return err
	}

	for i := range batches {
		if err := r.batcher.PushBatch(batches[i]); err != nil {
			return err
		}
	}

	return nil
}

func (r *topicStreamReaderImpl) Close(ctx context.Context, err error) {
	r.m.WithLock(func() {
		if r.err != nil {
			return
		}

		r.err = err
		r.cancel(err)
	})
}

func (r *topicStreamReaderImpl) onCommitResponse(mess *rawtopicreader.CommitOffsetResponse) error {
	for i := range mess.PartitionsCommittedOffsets {
		commit := &mess.PartitionsCommittedOffsets[i]
		partition, err := r.sessionController.Get(commit.PartitionSessionID)
		if err != nil {
			return err
		}
		partition.setCommittedOffset(commit.CommittedOffset.ToInt64())
	}

	return nil
}

func (r *topicStreamReaderImpl) updateToken(ctx context.Context) error {
	token, err := r.cfg.Cred.Token(ctx)
	if err != nil {
		// TODO: log
		return xerrors.WithStackTrace(err)
	}

	err = r.send(&rawtopicreader.UpdateTokenRequest{UpdateTokenRequest: rawtopic.UpdateTokenRequest{Token: token}})
	if err != nil {
		return err
	}
	return nil
}

type commitWaiter struct {
	offset rawtopicreader.Offset
	notify func(error)
}
