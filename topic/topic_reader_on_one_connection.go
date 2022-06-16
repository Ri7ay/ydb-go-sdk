package topic

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var (
	errGracefulShutdownPartition = xerrors.Wrap(errors.New("graceful shutdown partition"))
	errPartitionStopped          = xerrors.Wrap(errors.New("partition stopped"))
)

type partitionSessionID = pqstreamreader.PartitionSessionID

type topicStreamReaderImpl struct {
	cfg    topicStreamReaderConfig
	ctx    context.Context
	cancel xcontext.CancelErrFunc

	freeBytes         chan int
	stream            ReaderStream
	sessionController pumpSessionController

	readResponsesParseSignal chan struct{}
	messageBatches           chan *Batch

	m             xsync.RWMutex
	err           error
	started       bool
	readResponses []*pqstreamreader.ReadResponse // use slice instead channel for guarantee read grpc stream without block
}

type topicStreamReaderConfig struct {
	BaseContext        context.Context
	ProtoBufferSize    int
	Cred               credentials.Credentials
	CredUpdateInterval time.Duration
	Consumer           string
	ReadSelectors      []ReadSelector
}

func (cfg *topicStreamReaderConfig) initMessage() pqstreamreader.ClientMessage {
	// TODO improve
	res := &pqstreamreader.InitRequest{
		Consumer: cfg.Consumer,
	}

	res.TopicsReadSettings = make([]pqstreamreader.TopicReadSettings, len(cfg.ReadSelectors))
	for i, selector := range cfg.ReadSelectors {
		res.TopicsReadSettings[i] = pqstreamreader.TopicReadSettings{
			Topic:            selector.Stream.String(),
			PartitionsID:     selector.Partitions,
			StartFromWritten: selector.SkipMessagesBefore,
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
		messageBatches:           make(chan *Batch),
	}
	res.sessionController.init(res.ctx, res)
	res.freeBytes <- cfg.ProtoBufferSize
	err := res.start()
	if err == nil {
		return res, nil
	}
	return nil, err
}

func (r *topicStreamReaderImpl) ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*Batch, error) {
	// TODO: handle opts

	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if r.ctx.Err() != nil {
		return nil, r.ctx.Err()
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case <-r.ctx.Done():
		return nil, r.ctx.Err()

	case batch := <-r.messageBatches:
		r.freeBytes <- batch.sizeBytes
		return batch, nil
	}
}

func (r *topicStreamReaderImpl) Commit(ctx context.Context, offset CommitBatch) error {
	req := &pqstreamreader.CommitOffsetRequest{
		PartitionsOffsets: offset.toPartitionsOffsets(),
	}
	return r.stream.Send(req)
}

func (r *topicStreamReaderImpl) send(mess pqstreamreader.ClientMessage) error {
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

	if status := resp.StatusData(); status.Status != pqstreamreader.StatusSuccess {
		return xerrors.WithStackTrace(fmt.Errorf("bad status on initial error: %v (%v)", status.Status, status.Issues))
	}

	_, ok := resp.(*pqstreamreader.InitResponse)
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
		if status.Status != pqstreamreader.StatusSuccess {
			// TODO: actualize error message
			r.Close(nil, xerrors.WithStackTrace(fmt.Errorf("bad status from pq grpc stream: %v", status.Status)))
		}

		switch m := serverMessage.(type) {
		case *pqstreamreader.ReadResponse:
			r.onReadResponse(m)
		case *pqstreamreader.StartPartitionSessionRequest:
			if err = r.sessionController.onStartPartitionSessionRequest(m); err != nil {
				r.Close(nil, err)
				return
			}
		case *pqstreamreader.StopPartitionSessionRequest:
			if err = r.sessionController.onStopPartitionSessionRequest(m); err != nil {
				r.Close(nil, err)
				return
			}
		case *pqstreamreader.CommitOffsetResponse:
			if err = r.onCommitResponse(m); err != nil {
				r.Close(nil, err)
				return
			}

		case *pqstreamreader.PartitionSessionStatusResponse:
			if err = r.sessionController.onPartitionStatusResponse(m); err != nil {
				r.Close(nil, err)
			}
			return

		case *pqstreamreader.UpdateTokenResponse:
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
			err := r.stream.Send(&pqstreamreader.ReadRequest{BytesSize: free})
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
				r.dataParse(resp)
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

func (r *topicStreamReaderImpl) getFirstReadResponse() (res *pqstreamreader.ReadResponse) {
	r.m.Lock()
	defer r.m.Unlock()

	if len(r.readResponses) > 0 {
		res = r.readResponses[0]

		copy(r.readResponses, r.readResponses[1:])
		r.readResponses = r.readResponses[:len(r.readResponses)-1]
	}

	return res
}

func (r *topicStreamReaderImpl) dataParse(mess *pqstreamreader.ReadResponse) {
	batchesCount := 0
	for i := range mess.Partitions {
		batchesCount += len(mess.Partitions[i].Batches)
	}

	doneChannel := r.ctx.Done()
	for pIndex := range mess.Partitions {
		p := &mess.Partitions[pIndex]
		session := &PartitionSession{
			PartitionID: -1,
			ID:          p.PartitionSessionID,
		}
		for bIndex := range p.Batches {
			select {
			case r.messageBatches <- NewBatchFromStream(context.TODO(), "topic-todo", session, p.Batches[bIndex]):
				// pass
			case <-doneChannel:
				return
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

func (r *topicStreamReaderImpl) onCommitResponse(mess *pqstreamreader.CommitOffsetResponse) error {
	for i := range mess.Committed {
		commit := &mess.Committed[i]
		err := r.sessionController.sessionModify(commit.PartitionSessionID, func(p *partitionSessionData) {
			p.commitOffsetNotify(commit.Committed, nil)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (r *topicStreamReaderImpl) onReadResponse(mess *pqstreamreader.ReadResponse) {
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

	err = r.send(&pqstreamreader.UpdateTokenRequest{Token: token})
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

func newPartitionSessionData(readerCtx context.Context, mess *pqstreamreader.StartPartitionSessionRequest) *partitionSessionData {
	res := &partitionSessionData{
		Topic:       mess.PartitionSession.Topic,
		PartitionID: mess.PartitionSession.PartitionID,
	}

	res.graceful, res.gracefulCancel = xcontext.WithErrCancel(context.Background())
	res.alive, res.aliveCancel = xcontext.WithErrCancel(readerCtx)
	return res
}

func (p *partitionSessionData) commitOffsetNotify(offset pqstreamreader.Offset, err error) {
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

func (p *partitionSessionData) onStatusResponse(m *pqstreamreader.PartitionSessionStatusResponse) {
	// TODO: response to status waiters?
}

type commitWaiter struct {
	offset pqstreamreader.Offset
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

	return c.r.send(&pqstreamreader.PartitionSessionStatusRequest{PartitionSessionID: id})
}

func (c *pumpSessionController) onStartPartitionSessionRequest(mess *pqstreamreader.StartPartitionSessionRequest) error {
	// TODO: improve handler
	// TODO: add user handler

	data := newPartitionSessionData(c.ctx, mess)

	if err := c.sessionAdd(mess.PartitionSession.PartitionSessionID, data); err != nil {
		return err
	}

	return c.r.send(&pqstreamreader.StartPartitionSessionResponse{PartitionSessionID: mess.PartitionSession.PartitionSessionID})
}

func (c *pumpSessionController) onStopPartitionSessionRequest(mess *pqstreamreader.StopPartitionSessionRequest) error {
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

func (c *pumpSessionController) onPartitionStatusResponse(m *pqstreamreader.PartitionSessionStatusResponse) error {
	return c.sessionModify(m.PartitionSessionID, func(p *partitionSessionData) {
		p.onStatusResponse(m)
	})
}

func TestCreatePump(ctx context.Context, stream ReaderStream, cred credentials.Credentials) (*topicStreamReaderImpl, error) {
	cfg := newTopicStreamReaderConfig()
	cfg.BaseContext = ctx
	cfg.Cred = cred
	cfg.ReadSelectors = []ReadSelector{{
		Stream: "/local/asd",
	}}
	cfg.Consumer = "test"
	return newTopicStreamReader(stream, cfg)
}
