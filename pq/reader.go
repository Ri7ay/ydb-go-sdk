package pq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"

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
	background backgroundworkers.BackgroundWorker

	connector     ReaderStreamConnector
	streamCreator func(stream ReaderStream, cfg readerPumpConfig) (readerPump, error)
	clock         clockwork.Clock
	cfg           readerPumpConfig

	needReconnectSignal chan struct{}
	batches             chan *Batch

	closeOnce sync.Once

	m               xsync.RWMutex
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
		batches:             make(chan *Batch),
	}

	res.start()

	return res
}

func (r *Reader) Close() error {
	r.closeOnce.Do(func() {
		if r.pumpVal != nil {
			r.pumpVal.Close(xerrors.WithStackTrace(errors.New("ydb: reader closed")))
		}

		_ = r.background.Close()
	})

	return nil
}

func (r *Reader) start() {
	r.background.Start(r.reconnectLoop)
}

func (r *Reader) reconnectLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.needReconnectSignal:
			pump, err := r.connectAndStartReceiveMessage(ctx)
			r.m.WithLock(func() {
				r.pumpVal, r.connectionError = pump, err
				if err == nil {
					// read duplicates
					select {
					case <-r.needReconnectSignal:
					default:
					}
				}
			})
		}
	}
}

func (r *Reader) connectAndStartReceiveMessage(ctx context.Context) (readerPump, error) {
	ctx, cancel := context.WithTimeout(ctx, reconnectDuration)
	defer cancel()

	streamReader, err := r.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	pump, err := r.streamCreator(streamReader, r.cfg)
	if err != nil {
		return nil, err
	}
	r.background.Start()
	return pump, nil
}

func (r *Reader) pumpMessages(ctx context.Context, pump readerPump) {
	for {
		batch, err := pump.ReadMessageBatch(ctx)
		if err != nil {
			return
		}
		r.batches <- batch
	}
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

func (r *Reader) pump() readerPump {
	r.m.RLock()
	defer r.m.RUnlock()

	return r.pumpVal
}

// ReadMessageBatch read batch of messages.
// Batch is collection of messages, which can be atomically committed
func (r *Reader) ReadMessageBatch(ctx context.Context, opts ...ReadBatchOption) (*Batch, error) {
forBatches:
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		select {
		case batch := <-r.batches:
			if batch.Context().Err() != nil {
				continue forBatches
			}
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-r.background.Done():
			return nil, xerrors.WithStackTrace(errReaderClosed)
		}
	}
}

func (r *Reader) ReadMessage(context.Context) (Message, error) {
	panic("not implemented")
}

func (r *Reader) Commit(ctx context.Context, offset CommitableByOffset) error {
	return r.CommitBatch(ctx, CommitBatchFromCommitableByOffset(offset))
}

func (r *Reader) CommitBatch(ctx context.Context, commitBatch CommitBatch) error {
	stream := r.pump()
	err := stream.Commit(ctx, commitBatch)
	r.fireReconnectOnRetryableError(err)
	return err
}

func (r *Reader) CommitMessages(ctx context.Context, messages ...Message) error {
	return r.CommitBatch(ctx, CommitBatchFromMessages(messages...))
}
