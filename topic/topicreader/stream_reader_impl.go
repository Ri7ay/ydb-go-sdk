package topicreader

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"runtime/pprof"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/background"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

var (
	ErrPartitionStopped = errors.New("ydb: pq partition stopped")
	ErrUnsupportedCodec = errors.New("ydb: unsupported codec")
)

type partitionSessionID = rawtopicreader.PartitionSessionID

type topicStreamReaderImpl struct {
	cfg    topicStreamReaderConfig
	ctx    context.Context
	cancel xcontext.CancelErrFunc

	freeBytes         chan int
	sessionController partitionSessionStorage
	backgroundWorkers background.Worker

	rawMessagesFromBuffer chan rawtopicreader.ServerMessage

	batcher   *batcher
	committer *committer

	stream           RawTopicReaderStream
	readConnectionID string

	m       xsync.RWMutex
	err     error
	started bool
	closed  bool
}

type topicStreamReaderConfig struct {
	SendBatchTimeLagTrigger         time.Duration
	SendBatchCounterTrigger         int
	BaseContext                     context.Context
	BufferSizeProtoBytes            int
	Cred                            credentials.Credentials
	CredUpdateInterval              time.Duration
	Consumer                        string
	ReadSelectors                   []ReadSelector
	Tracer                          trace.Topic
	GetPartitionStartOffsetCallback GetPartitionStartOffsetFunc
	CommitMode                      CommitMode
}

func (cfg *topicStreamReaderConfig) initMessage() *rawtopicreader.InitRequest {
	// TODO improve
	res := &rawtopicreader.InitRequest{
		Consumer: cfg.Consumer,
	}

	res.TopicsReadSettings = make([]rawtopicreader.TopicReadSettings, len(cfg.ReadSelectors))
	for i, selector := range cfg.ReadSelectors {
		settings := &res.TopicsReadSettings[i]
		settings.Path = selector.Stream.String()
		settings.PartitionsID = selector.Partitions
		if !selector.ReadFrom.IsZero() {
			settings.ReadFrom.HasValue = true
			settings.ReadFrom.Value = selector.ReadFrom
		}
		if selector.MaxTimeLag != 0 {
			settings.MaxLag.HasValue = true
			settings.MaxLag.Value = selector.MaxTimeLag
		}
	}

	return res
}

func newTopicStreamReaderConfig() topicStreamReaderConfig {
	return topicStreamReaderConfig{
		BaseContext:             context.Background(),
		BufferSizeProtoBytes:    1024 * 1024,
		Cred:                    credentials.NewAnonymousCredentials(),
		CredUpdateInterval:      time.Hour,
		CommitMode:              CommitModeAsync,
		SendBatchTimeLagTrigger: time.Second,
	}
}

func newTopicStreamReader(stream RawTopicReaderStream, cfg topicStreamReaderConfig) (_ *topicStreamReaderImpl, err error) {
	defer func() {
		if err != nil {
			_ = stream.CloseSend()
		}
	}()

	reader, err := newTopicStreamReaderStopped(stream, cfg)
	if err != nil {
		return nil, err
	}
	if err = reader.initSession(); err != nil {
		return nil, err
	}
	if err = reader.startLoops(); err != nil {
		return nil, err
	}

	return reader, nil
}

func newTopicStreamReaderStopped(stream RawTopicReaderStream, cfg topicStreamReaderConfig) (*topicStreamReaderImpl, error) {
	labeledContext := pprof.WithLabels(cfg.BaseContext, pprof.Labels("base-context", "topic-stream-reader"))
	stopPump, cancel := xcontext.WithErrCancel(labeledContext)

	readerConnectionID, err := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	if err != nil {
		readerConnectionID = big.NewInt(-1)
	}

	res := &topicStreamReaderImpl{
		cfg:                   cfg,
		ctx:                   stopPump,
		freeBytes:             make(chan int, 1),
		stream:                &syncedStream{stream: stream},
		cancel:                cancel,
		batcher:               newBatcher(),
		backgroundWorkers:     *background.NewWorker(stopPump),
		readConnectionID:      readerConnectionID.String(),
		rawMessagesFromBuffer: make(chan rawtopicreader.ServerMessage, 1),
	}

	res.committer = newCommitter(labeledContext, cfg.CommitMode, res.send)
	res.committer.BufferTimeLagTrigger = cfg.SendBatchTimeLagTrigger
	res.committer.BufferCountTrigger = cfg.SendBatchCounterTrigger

	res.sessionController.init()
	return res, nil
}

func (r *topicStreamReaderImpl) ReadMessageBatch(
	ctx context.Context,
	opts readMessageBatchOptions,
) (batch Batch, err error) {
	if err := ctx.Err(); err != nil {
		return Batch{}, err
	}

	ctx, cancel := xcontext.Merge(ctx, r.ctx)
	defer func() {
		cancel(errors.New("ydb: topic stream read message batch competed"))
		if err == nil {
			r.freeBufferFromMessages(batch)
		}
	}()

	return r.consumeMessagesUntilBatch(ctx, opts)
}

func (r *topicStreamReaderImpl) consumeMessagesUntilBatch(
	ctx context.Context,
	opts readMessageBatchOptions,
) (Batch, error) {
	for {
		item, err := r.batcher.Pop(ctx, opts.batcherGetOptions)
		if err != nil {
			return Batch{}, err
		}

		switch {
		case item.IsBatch():
			return item.Batch, nil
		case item.IsRawMessage():
			r.sendRawMessageToChannelUnblocked(item.RawMessage)
		default:
			return Batch{}, xerrors.WithStackTrace(fmt.Errorf("ydb: unexpected item type from batcher: %#v", item))
		}
	}
}

func (r *topicStreamReaderImpl) sendRawMessageToChannelUnblocked(mess rawtopicreader.ServerMessage) {
	select {
	case r.rawMessagesFromBuffer <- mess:
		return
	default:
		// send in goroutine, without block caller
		r.backgroundWorkers.Start("sendMessageToRawChannel", func(ctx context.Context) {
			select {
			case r.rawMessagesFromBuffer <- mess:
			case <-ctx.Done():
			}
		})
	}
}

func (r *topicStreamReaderImpl) consumeRawMessageFromBuffer(ctx context.Context) {
	doneChan := ctx.Done()

	for {
		var mess rawtopicreader.ServerMessage
		select {
		case <-doneChan:
			return
		case mess = <-r.rawMessagesFromBuffer:
			// pass
		}

		switch m := mess.(type) {
		case *rawtopicreader.StartPartitionSessionRequest:
			if err := r.onStartPartitionSessionRequestFromBuffer(m); err != nil {
				r.Close(ctx, err)
				return
			}
		case *rawtopicreader.StopPartitionSessionRequest:
			if err := r.onStopPartitionSessionRequestFromBuffer(m); err != nil {
				r.Close(ctx, xerrors.WithStackTrace(fmt.Errorf("ydb: unexpected error on stop partition handler: %w", err)))
				return
			}
		case *rawtopicreader.PartitionSessionStatusResponse:
			r.onPartitionSessionStatusResponseFromBuffer(ctx, m)
		default:
			r.Close(ctx, xerrors.WithStackTrace(
				fmt.Errorf("ydb: unexpected server message from buffer: %v", reflect.TypeOf(mess))),
			)
		}
	}
}

func (r *topicStreamReaderImpl) onStopPartitionSessionRequestFromBuffer(
	mess *rawtopicreader.StopPartitionSessionRequest,
) error {
	session, err := r.sessionController.Get(mess.PartitionSessionID)
	if err != nil {
		return err
	}

	trace.TopicOnPartitionReadStop(
		r.cfg.Tracer,
		r.readConnectionID,
		session.Context(),
		session.Topic,
		session.PartitionID,
		session.partitionSessionID.ToInt64(),
		mess.CommittedOffset.ToInt64(),
		mess.Graceful,
	)

	if mess.Graceful {
		resp := &rawtopicreader.StopPartitionSessionResponse{
			PartitionSessionID: session.partitionSessionID,
		}
		if err = r.send(resp); err != nil {
			return err
		}
	}

	if _, err = r.sessionController.Remove(session.partitionSessionID); err != nil {
		if mess.Graceful {
			return err
		} else { //nolint:revive,staticcheck
			// double message with graceful=false is ok.
			// It may be received after message with graceful=true and session was removed while process that.

			// pass
		}
	}

	return nil
}

func (r *topicStreamReaderImpl) onPartitionSessionStatusResponseFromBuffer(ctx context.Context, m *rawtopicreader.PartitionSessionStatusResponse) {
	panic("not implemented")
}

func (r *topicStreamReaderImpl) Commit(ctx context.Context, commitRange commitRange) error {
	if err := r.checkCommitRange(commitRange); err != nil {
		return err
	}
	return r.committer.Commit(ctx, commitRange)
}

func (r *topicStreamReaderImpl) commitAsync(ctx context.Context, offsets CommitRages) error {
	req := &rawtopicreader.CommitOffsetRequest{
		CommitOffsets: offsets.toPartitionsOffsets(),
	}
	return r.send(req)
}

func (r *topicStreamReaderImpl) checkCommitRange(commitRange commitRange) error {
	session := commitRange.partitionSession

	if session == nil {
		return xerrors.NewWithStackTrace("ydb: commit with nil partition session")
	}

	if session.Context().Err() != nil {
		return xerrors.WithStackTrace(fmt.Errorf("ydb: commit error: %w", ErrPartitionStopped))
	}

	ownSession, err := r.sessionController.Get(session.partitionSessionID)
	if err != nil || session != ownSession {
		return xerrors.NewWithStackTrace("ydb: commit with session from other reader")
	}

	return nil
}

func (r *topicStreamReaderImpl) send(mess rawtopicreader.ClientMessage) error {
	err := r.stream.Send(mess)
	trace.TopicOnReadStreamRawSent(r.cfg.Tracer, r.readConnectionID, r.cfg.BaseContext, mess, err)
	if err != nil {
		// TODO: log
		r.Close(r.ctx, err)
	}
	return err
}

func (r *topicStreamReaderImpl) startLoops() error {
	if err := r.setStarted(); err != nil {
		return err
	}

	r.backgroundWorkers.Start("readMessagesLoop", r.readMessagesLoop)
	r.backgroundWorkers.Start("dataRequestLoop", r.dataRequestLoop)
	r.backgroundWorkers.Start("updateTokenLoop", r.updateTokenLoop)

	r.backgroundWorkers.Start("consumeRawMessageFromBuffer", r.consumeRawMessageFromBuffer)

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
	if err := r.send(r.cfg.initMessage()); err != nil {
		return err
	}

	resp, err := r.stream.Recv()
	trace.TopicOnReadStreamRawReceived(r.cfg.Tracer, r.readConnectionID, r.cfg.BaseContext, resp, err)
	if err != nil {
		return err
	}

	if status := resp.StatusData(); !status.Status.IsSuccess() {
		return xerrors.WithStackTrace(fmt.Errorf("bad status on initial error: %v (%v)", status.Status, status.Issues))
	}

	initResp, ok := resp.(*rawtopicreader.InitResponse)
	if !ok {
		return xerrors.WithStackTrace(fmt.Errorf("bad message type on session init: %v (%v)", resp, reflect.TypeOf(resp)))
	}

	r.readConnectionID = initResp.SessionID

	return r.sendDataRequest(r.cfg.BufferSizeProtoBytes)
}

func (r *topicStreamReaderImpl) readMessagesLoop(ctx context.Context) {
	ctx, cancel := xcontext.WithErrCancel(ctx)
	defer cancel(xerrors.NewWithIssues("ydb: topic stream reader messages loop finished"))

	for {
		serverMessage, err := r.stream.Recv()
		trace.TopicOnReadStreamRawReceived(r.cfg.Tracer, r.readConnectionID, r.cfg.BaseContext, serverMessage, err)
		if err != nil {
			if errors.Is(err, rawtopicreader.ErrUnexpectedMessageType) {
				trace.TopicOnReadUnknownGrpcMessage(r.cfg.Tracer, r.readConnectionID, r.cfg.BaseContext, err)
				// new messages can be added to protocol, it must be backward compatible to old programs
				// and skip message is safe
				continue
			}
			r.Close(ctx, err)
			return
		}

		status := serverMessage.StatusData()
		if !status.Status.IsSuccess() {
			// TODO: actualize error message
			r.Close(ctx, xerrors.WithStackTrace(fmt.Errorf("bad status from pq grpc stream: %v", status.Status)))
		}

		switch m := serverMessage.(type) {
		case *rawtopicreader.ReadResponse:
			if err := r.onReadResponse(m); err != nil {
				r.Close(ctx, err)
			}
		case *rawtopicreader.StartPartitionSessionRequest:
			if err = r.onStartPartitionSessionRequest(m); err != nil {
				r.Close(ctx, err)
				return
			}
		case *rawtopicreader.StopPartitionSessionRequest:
			if err = r.onStopPartitionSessionRequest(m); err != nil {
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

func (r *topicStreamReaderImpl) dataRequestLoop(ctx context.Context) {
	if r.ctx.Err() != nil {
		return
	}

	doneChan := ctx.Done()

	for {
		select {
		case <-doneChan:
			r.Close(ctx, r.ctx.Err())
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

			if err := r.sendDataRequest(sum); err != nil {
				return
			}
		}
	}
}

func (r *topicStreamReaderImpl) sendDataRequest(size int) error {
	return r.send(&rawtopicreader.ReadRequest{BytesSize: size})
}

func (r *topicStreamReaderImpl) freeBufferFromMessages(batch Batch) {
	size := 0
	for messageIndex := range batch.Messages {
		size += batch.Messages[messageIndex].bufferBytesAccount
	}
	select {
	case r.freeBytes <- size:
	case <-r.ctx.Done():
	}
}

func (r *topicStreamReaderImpl) updateTokenLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.CredUpdateInterval)
	defer ticker.Stop()

	readerCancel := ctx.Done()
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

		// normal way
		session, err := r.sessionController.Get(p.PartitionSessionID)
		if err != nil {
			return err
		}

		for bIndex := range p.Batches {
			if r.ctx.Err() != nil {
				return r.ctx.Err()
			}

			batch, err := newBatchFromStream(session, p.Batches[bIndex])
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
	isFirstClose := false
	// TODO: close error
	r.m.WithLock(func() {
		if r.closed {
			return
		}
		isFirstClose = true
		r.closed = true

		r.err = err
		r.cancel(err)
	})
	if !isFirstClose {
		// TODO: return error
		return
	}

	_ = r.committer.Close(ctx, err)

	// close stream strong after committer close - for flush commits buffer
	_ = r.stream.CloseSend()

	// close background workers after r.stream.CloseSend
	_ = r.backgroundWorkers.Close(ctx, err)
}

func (r *topicStreamReaderImpl) onCommitResponse(mess *rawtopicreader.CommitOffsetResponse) error {
	for i := range mess.PartitionsCommittedOffsets {
		commit := &mess.PartitionsCommittedOffsets[i]
		partition, err := r.sessionController.Get(commit.PartitionSessionID)
		if err != nil {
			return err
		}
		partition.setCommittedOffset(commit.CommittedOffset)
	}

	return nil
}

func (r *topicStreamReaderImpl) updateToken(ctx context.Context) error {
	token, err := r.cfg.Cred.Token(ctx)
	if err != nil {
		// TODO: log
		return xerrors.WithStackTrace(err)
	}

	err = r.send(&rawtopicreader.UpdateTokenRequest{UpdateTokenRequest: rawtopiccommon.UpdateTokenRequest{Token: token}})
	if err != nil {
		return err
	}
	return nil
}

func (r *topicStreamReaderImpl) onStartPartitionSessionRequest(m *rawtopicreader.StartPartitionSessionRequest) error {
	session := newPartitionSession(
		r.ctx,
		m.PartitionSession.Path,
		m.PartitionSession.PartitionID,
		m.PartitionSession.PartitionSessionID,
		m.CommittedOffset,
	)
	if err := r.sessionController.Add(session); err != nil {
		return err
	}
	return r.batcher.PushRawMessage(session, m)
}

func (r *topicStreamReaderImpl) onStartPartitionSessionRequestFromBuffer(
	m *rawtopicreader.StartPartitionSessionRequest,
) error {
	session, err := r.sessionController.Get(m.PartitionSession.PartitionSessionID)
	if err != nil {
		return err
	}

	respMessage := &rawtopicreader.StartPartitionSessionResponse{
		PartitionSessionID: session.partitionSessionID,
	}

	var forceOffset *int64
	if r.cfg.GetPartitionStartOffsetCallback != nil {
		req := GetPartitionStartOffsetRequest{
			Session: session,
		}
		resp, callbackErr := r.cfg.GetPartitionStartOffsetCallback(session.Context(), req)
		if callbackErr != nil {
			return callbackErr
		}
		if resp.startOffsetUsed {
			wantOffset := resp.startOffset.ToInt64()
			forceOffset = &wantOffset
		}
	}

	respMessage.ReadOffset.FromInt64Pointer(forceOffset)
	if r.cfg.CommitMode.commitsEnabled() {
		respMessage.CommitOffset.FromInt64Pointer(forceOffset)
	}

	if err = r.send(respMessage); err != nil {
		return err
	}

	trace.TopicOnPartitionReadStart(
		r.cfg.Tracer,
		r.readConnectionID,
		session.Context(),
		session.Topic,
		session.PartitionID,
		forceOffset,
		forceOffset,
	)

	return nil
}

func (r *topicStreamReaderImpl) onStopPartitionSessionRequest(m *rawtopicreader.StopPartitionSessionRequest) error {
	session, err := r.sessionController.Get(m.PartitionSessionID)
	if err != nil {
		return err
	}

	if !m.Graceful {
		session.close(ErrPartitionStopped)
	}

	return r.batcher.PushRawMessage(session, m)
}
