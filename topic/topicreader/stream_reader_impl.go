package topicreader

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	errGracefulShutdownPartition = xerrors.Wrap(errors.New("graceful shutdown partition"))
	errPartitionStopped          = xerrors.Wrap(errors.New("partition stopped"))
)

type partitionSessionID = rawtopicreader.PartitionSessionID

type topicStreamReaderImpl struct {
	cfg    topicStreamReaderConfig
	ctx    context.Context
	cancel xcontext.CancelErrFunc

	freeBytes         chan int
	stream            ReaderStream
	sessionController pumpSessionController

	readResponsesParseSignal chan struct{}
	batcher                  *batcher

	m             xsync.RWMutex
	err           error
	started       bool
	readResponses []*rawtopicreader.ReadResponse // use slice instead channel for guarantee read grpc stream without block
}

type topicStreamReaderConfig struct {
	BaseContext        context.Context
	ProtoBufferSize    int
	Cred               credentials.Credentials
	CredUpdateInterval time.Duration
	Consumer           string
	ReadSelectors      []ReadSelector
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
		BaseContext:        context.Background(),
		ProtoBufferSize:    1024 * 1024,
		CredUpdateInterval: time.Hour,
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
	res.freeBytes <- cfg.ProtoBufferSize
	err := res.start()
	if err == nil {
		return res, nil
	}
	return nil, err
}

func (r *topicStreamReaderImpl) ReadMessageBatch(ctx context.Context, opts readMessageBatchOptions) (*Batch, error) {
	ctx, cancel := xcontext.Merge(ctx, r.ctx)
	defer cancel(errors.New("ydb: topic stream read message batch competed"))

	batch, err := r.batcher.Get(ctx, opts.batcherGetOptions)
	if err == nil {
		return &batch, nil
	}
	return nil, err
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
	go r.dataParseLoop()
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
			r.onReadResponse(m)
		case *rawtopicreader.StartPartitionSessionRequest:
			if err = r.sessionController.onStartPartitionSessionRequest(m); err != nil {
				r.Close(nil, err)
				return
			}
		case *rawtopicreader.StopPartitionSessionRequest:
			if err = r.sessionController.onStopPartitionSessionRequest(m); err != nil {
				r.Close(nil, err)
				return
			}
		case *rawtopicreader.CommitOffsetResponse:
			if err = r.onCommitResponse(m); err != nil {
				r.Close(nil, err)
				return
			}

		case *rawtopicreader.PartitionSessionStatusResponse:
			if err = r.sessionController.onPartitionStatusResponse(m); err != nil {
				r.Close(nil, err)
			}
			return

		case *rawtopicreader.UpdateTokenResponse:
			// skip
		default:
			// TODO: remove before release
			r.Close(nil, xerrors.WithStackTrace(fmt.Errorf("receive unexpected message: %#v (%v)", m, reflect.TypeOf(m))))
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
			err := r.stream.Send(&rawtopicreader.ReadRequest{BytesSize: free})
			if err != nil {
				r.Close(nil, err)
			}
		}
	}
}

func (r *topicStreamReaderImpl) dataParseLoop() {
	for {
		select {
		case <-r.ctx.Done():
			r.Close(nil, r.ctx.Err())
			return

		case <-r.readResponsesParseSignal:
			// start work
		}

	consumeReadResponseBuffer:
		for {
			resp := r.getFirstReadResponse()
			if resp == nil {
				// buffer is empty, need wait new message
				break consumeReadResponseBuffer
			} else {
				r.readResponse(resp)
			}
		}
	}
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

func (r *topicStreamReaderImpl) getFirstReadResponse() (res *rawtopicreader.ReadResponse) {
	r.m.Lock()
	defer r.m.Unlock()

	if len(r.readResponses) > 0 {
		res = r.readResponses[0]

		copy(r.readResponses, r.readResponses[1:])
		r.readResponses = r.readResponses[:len(r.readResponses)-1]
	}

	return res
}

func (r *topicStreamReaderImpl) readResponse(mess *rawtopicreader.ReadResponse) {
	batchesCount := 0
	for i := range mess.PartitionData {
		batchesCount += len(mess.PartitionData[i].Batches)
	}

	for pIndex := range mess.PartitionData {
		p := &mess.PartitionData[pIndex]
		session := &PartitionSession{
			PartitionID: -1,
			ID:          p.PartitionSessionID,
		}

		for bIndex := range p.Batches {
			if r.ctx.Err() != nil {
				return
			}

			batch := NewBatchFromStream(context.TODO(), "topic-todo", session, p.Batches[bIndex])
			err := r.batcher.Add(batch)
			if err != nil {
				r.Close(context.Background(), err)
			}
		}
	}
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
		err := r.sessionController.sessionModify(commit.PartitionSessionID, func(p *partitionSessionData) {
			p.commitOffsetNotify(commit.CommittedOffset, nil)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *topicStreamReaderImpl) onReadResponse(mess *rawtopicreader.ReadResponse) {
	r.m.WithLock(func() {
		r.readResponses = append(r.readResponses, mess)
		select {
		case r.readResponsesParseSignal <- struct{}{}:
		default:
			// no blocking
		}
	})
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

type partitionSessionData struct {
	Topic       string
	PartitionID int64

	graceful       context.Context
	gracefulCancel xcontext.CancelErrFunc
	alive          context.Context
	aliveCancel    xcontext.CancelErrFunc
	commitWaiters  []commitWaiter
}

func newPartitionSessionData(readerCtx context.Context, mess *rawtopicreader.StartPartitionSessionRequest) *partitionSessionData {
	res := &partitionSessionData{
		Topic:       mess.PartitionSession.Path,
		PartitionID: mess.PartitionSession.PartitionID,
	}

	res.graceful, res.gracefulCancel = xcontext.WithErrCancel(context.Background())
	res.alive, res.aliveCancel = xcontext.WithErrCancel(readerCtx)
	return res
}

func (p *partitionSessionData) commitOffsetNotify(offset rawtopicreader.Offset, err error) {
	newWaiters := p.commitWaiters[:0]
	for i := range p.commitWaiters {
		waiter := &p.commitWaiters[i]
		if waiter.offset <= offset {
			waiter.notify(err)
		} else {
			newWaiters = append(newWaiters, *waiter)
		}
	}
	p.commitWaiters = newWaiters
}

func (p *partitionSessionData) nofityGraceful() {
	p.gracefulCancel(errGracefulShutdownPartition)
}

func (p *partitionSessionData) close(err error) {
	p.aliveCancel(err)
	for _, waiter := range p.commitWaiters {
		waiter.notify(err)
	}
}

func (p *partitionSessionData) onStatusResponse(m *rawtopicreader.PartitionSessionStatusResponse) {
	// TODO: response to status waiters?
}

type commitWaiter struct {
	offset rawtopicreader.Offset
	notify func(error)
}

type pumpSessionController struct {
	ctx context.Context
	r   *topicStreamReaderImpl

	m        sync.RWMutex
	sessions map[partitionSessionID]*partitionSessionData
}

func (c *pumpSessionController) init(ctx context.Context, reader *topicStreamReaderImpl) {
	c.ctx = ctx
	c.r = reader
	c.sessions = make(map[partitionSessionID]*partitionSessionData)
}

func (c *pumpSessionController) requestStatus(id partitionSessionID) error {
	if _, ok := c.sessions[id]; !ok {
		return xerrors.WithStackTrace(fmt.Errorf("unexpected session id: %v", id))
	}

	return c.r.send(&rawtopicreader.PartitionSessionStatusRequest{PartitionSessionID: id})
}

func (c *pumpSessionController) onStartPartitionSessionRequest(mess *rawtopicreader.StartPartitionSessionRequest) error {
	// TODO: improve handler
	// TODO: add user handler

	data := newPartitionSessionData(c.ctx, mess)

	if err := c.sessionAdd(mess.PartitionSession.PartitionSessionID, data); err != nil {
		return err
	}

	return c.r.send(&rawtopicreader.StartPartitionSessionResponse{PartitionSessionID: mess.PartitionSession.PartitionSessionID})
}

func (c *pumpSessionController) onStopPartitionSessionRequest(mess *rawtopicreader.StopPartitionSessionRequest) error {
	if mess.Graceful {
		err := c.sessionModify(mess.PartitionSessionID, func(p *partitionSessionData) {
			p.nofityGraceful()
		})
		return err
	}

	if data, err := c.sessionDel(mess.PartitionSessionID); err == nil {
		data.close(errPartitionStopped)
	} else {
		return err
	}

	return nil
}

func (c *pumpSessionController) sessionAdd(id partitionSessionID, data *partitionSessionData) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.sessions[id]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", id))
	}
	c.sessions[id] = data
	return nil
}

func (c *pumpSessionController) sessionDel(id partitionSessionID) (*partitionSessionData, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if data, ok := c.sessions[id]; ok {
		delete(c.sessions, id)
		return data, nil
	}
	return nil, xerrors.WithStackTrace(fmt.Errorf("delete undefined partition session: %v", id))
}

func (c *pumpSessionController) sessionModify(id partitionSessionID, callback func(p *partitionSessionData)) error {
	c.m.Lock()
	defer c.m.Unlock()

	if p, ok := c.sessions[id]; ok {
		callback(p)
		return nil
	}

	return xerrors.WithStackTrace(fmt.Errorf("modify unexpectet session id: %v", id))
}

func (c *pumpSessionController) onPartitionStatusResponse(m *rawtopicreader.PartitionSessionStatusResponse) error {
	return c.sessionModify(m.PartitionSessionID, func(p *partitionSessionData) {
		p.onStatusResponse(m)
	})
}

func TestCreatePump(ctx context.Context, stream ReaderStream, cred credentials.Credentials) (*topicStreamReaderImpl, error) {
	// TODO: remove
	cfg := newTopicStreamReaderConfig()
	cfg.BaseContext = ctx
	cfg.Cred = cred
	cfg.ReadSelectors = []ReadSelector{{
		Stream: "/local/asd",
	}}
	cfg.Consumer = "test"
	return newTopicStreamReader(stream, cfg)
}
