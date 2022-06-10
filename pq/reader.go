package pq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/locker"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ictx"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	// TODO: improve
	reconnectDuration     = time.Second / 10
	streamPollingInterval = time.Second / 10
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

type ReaderStreamConnector interface {
	Connect(ctx context.Context) (ReaderStream, error)
}

type Reader struct {
	connector     ReaderStreamConnector
	streamCreator func(stream ReaderStream, cfg readerPumpConfig) (readerPump, error)
	clock         clockwork.Clock
	cfg           readerPumpConfig

	ctx       context.Context
	ctxCancel ictx.CancelErrFunc

	backgrounds sync.WaitGroup

	needReconnectSignal chan struct{}
	batches             chan *Batch

	m               locker.RWLocker
	connectionError error
	pumpVal         readerPump
}

type readerPump interface {
	ReadMessageBatch(ctx context.Context) (*Batch, error)
	Commit(ctx context.Context, offset CommitBatch) error
	Close(err error)
}

func NewReader(connectCtx context.Context, connector ReaderStreamConnector, consumer string, readSelectors []ReadSelector, opts ...readerOption) *Reader {
	res := &Reader{
		connector: connector,
		clock:     clockwork.NewRealClock(),
		streamCreator: func(stream ReaderStream, cfg readerPumpConfig) (readerPump, error) {
			return newReaderPump(stream, cfg)
		},
		needReconnectSignal: make(chan struct{}, 1),
		cfg:                 readerPumpConfig{}, // TODO
	}

	res.start()

	return res
}

func (r *Reader) Close() error {
	r.m.Lock()
	defer r.m.Unlock()

	err := xerrors.WithStackTrace(errReaderClosed)
	r.ctxCancel(err)

	if r.pumpVal != nil {
		r.pumpVal.Close(err)
	}

	r.connectionError = err
	r.backgrounds.Wait()
	return nil
}

func (r *Reader) start() {
	r.backgrounds.Add(1)
	go r.reconnectLoop()
}

func (r *Reader) reconnectLoop() {
	defer r.backgrounds.Done()

	doneChan := r.ctx.Done()
	for {
		select {
		case <-doneChan:
			return
		case <-r.needReconnectSignal:
			pump, err := r.reconnect()
			r.m.WithLock(func() {
				r.pumpVal, r.connectionError = pump, err
			})
		}
	}
}

func (r *Reader) reconnect() (readerPump, error) {
	oldStream, _ := r.pump()
	oldStream.Close(xerrors.WithStackTrace(errors.New("reader need reconnect")))

	ctx, cancel := context.WithTimeout(r.ctx, reconnectDuration)
	defer cancel()

	streamReader, err := r.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	pump, err := r.streamCreator(streamReader, r.cfg)
	if err != nil {
		return nil, err
	}
	return pump, nil
}

func (r *Reader) fireReconnectOnRetryableError(err error) {
	if xerrors.RetryableError(err) == nil {
		return
	}

	select {
	case r.needReconnectSignal <- struct{}{}:
		// send signal
	default:
		// signal was send and not handled already
	}
}

func (r *Reader) pump() (readerPump, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	return r.pumpVal, r.connectionError
}

func (r *Reader) connectedPump(ctx context.Context) (readerPump, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if r.ctx.Err() != nil {
		return nil, r.ctx.Err()
	}

	var res readerPump
	r.m.WithLock(func() {
		if r.connectionError == nil {
			res = r.pumpVal
		}
	})
	if res != nil {
		return res, nil
	}

	// TODO: improve loop for work on events instead of timer
	ticker := r.clock.NewTicker(streamPollingInterval)
	for {
		select {
		case <-ticker.Chan():
			r.m.WithLock(func() {
				if r.connectionError == nil {
					res = r.pumpVal
				}
			})
			if res != nil {
				return res, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-r.ctx.Done():
			return nil, r.ctx.Err()
		}
	}
}

// ReadMessageBatch read batch of messages.
// Batch is collection of messages, which can be atomically committed
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
	var res *Batch
	var err error

	err = r.retryOnPump(ctx, func(ctx context.Context, pump readerPump) error {
		res, err = pump.ReadMessageBatch(ctx)
		return err
	})

	return res, err
}

func (r *Reader) retryOnPump(ctx context.Context, callback func(ctx context.Context, pump readerPump) error) error {
	for {
		pump, err := r.connectedPump(ctx)
		if err != nil {
			return err
		}

		err = callback(ctx, pump)
		if err == nil {
			return nil
		}

		if xerrors.RetryableError(err) == nil {
			return err
		}

		r.fireReconnectOnRetryableError(err)
		return err
	}
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	panic("not implemented")
}

func (r *Reader) Commit(ctx context.Context, offset CommitableByOffset) error {
	return r.CommitBatch(ctx, CommitBatchFromCommitableByOffset(offset))
}

func (r *Reader) CommitBatch(ctx context.Context, commitBatch CommitBatch) error {
	stream, err := r.pump()
	if err != nil {
		return err
	}

	return stream.Commit(ctx, commitBatch)
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...Message) error {
	return r.CommitBatch(ctx, CommitBatchFromMessages(messages...))
}
