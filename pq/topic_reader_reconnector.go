package pq

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

const (
	// TODO: improve
	reconnectDuration      = time.Second * 60
	streamPollingInterval  = time.Second / 10
	forceReconnectInterval = time.Hour * 24 * 365 * 100 // never
)

type readerConnectFunc func(ctx context.Context) (topicStreamReader, error)

type readerReconnector struct {
	background backgroundworkers.BackgroundWorker

	readerConnect readerConnectFunc

	reconnectFromBadStream chan topicStreamReader
	nextBatch              chan *Batch

	closeOnce sync.Once

	m         xsync.RWMutex
	streamVal topicStreamReader
	streamErr error
	closedErr error
}

func newReaderReconnector(connectCtx context.Context, connector readerConnectFunc) *readerReconnector {
	res := &readerReconnector{
		readerConnect: connector,
		streamErr:     errUnconnected,
	}

	res.initChannels()
	res.start(connectCtx)

	return res
}

func (r *readerReconnector) ReadMessageBatch(ctx context.Context) (*Batch, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

readBatches:
	for {
		select {
		case batch := <-r.nextBatch:
			if batch.Context().Err() != nil {
				continue readBatches
			}
			return batch, nil

		case <-ctx.Done():
			return nil, ctx.Err()

		case <-r.background.Done():
			return nil, r.closedErr
		}
	}
}

func (r *readerReconnector) Commit(ctx context.Context, offset CommitBatch) error {
	stream, err := r.stream()
	if err != nil {
		return err
	}

	err = stream.Commit(ctx, offset)
	r.fireReconnectOnRetryableError(stream, err)
	return err
}

func (r *readerReconnector) Close(ctx context.Context, err error) {
	r.closeOnce.Do(func() {
		r.m.WithLock(func() {
			r.closedErr = err
		})

		_ = r.background.Close(ctx)

		if r.streamVal != nil {
			r.streamVal.Close(nil, xerrors.WithStackTrace(errReaderClosed))
		}
	})
}

func (r *readerReconnector) start(ctx context.Context) {
	r.background.Start(r.connectionLoop)

	// start first connection
	go func() { r.reconnectFromBadStream <- nil }()
}

func (r *readerReconnector) initChannels() {
	r.reconnectFromBadStream = make(chan topicStreamReader, 1)
	r.nextBatch = make(chan *Batch)
}

func (r *readerReconnector) connectionLoop(ctx context.Context) {
	defer r.handlePanic()

forConnect:
	// TODO: add delay for repeats
	for {
		select {
		case <-ctx.Done():
			return

		case oldStream := <-r.reconnectFromBadStream:
			if ctx.Err() != nil {
				return
			}

			stream, _ := r.stream()
			if oldStream != stream {
				continue forConnect
			}

			if oldStream != nil {
				oldStream.Close(ctx, xerrors.WithStackTrace(errors.New("ydb: reconnect to pq grpc stream")))
			}

			reconnectCtx, cancel := context.WithTimeout(ctx, reconnectDuration)
			newStream, err := r.connectAndStartReceiveMessage(reconnectCtx)
			cancel()

			if isRetryableError(err) {
				go func() {
					// guarantee write reconnect signal to channel
					r.reconnectFromBadStream <- newStream
				}()
			}

			r.m.WithLock(func() {
				r.streamVal, r.streamErr = newStream, err
			})
		}
	}
}

func (r *readerReconnector) connectAndStartReceiveMessage(ctx context.Context) (topicStreamReader, error) {
	streamReader, err := r.readerConnect(ctx)
	if err != nil {
		return nil, err
	}

	r.background.Start(func(ctx context.Context) {
		r.readMessagesFromStreamLoop(ctx, streamReader)
	})
	return streamReader, nil
}

func (r *readerReconnector) readMessagesFromStreamLoop(ctx context.Context, pump topicStreamReader) {
	defer r.handlePanic()

	for {
		batch, err := pump.ReadMessageBatch(ctx)
		r.fireReconnectOnRetryableError(pump, err)
		if err != nil {
			return
		}
		select {
		case r.nextBatch <- batch:
			// pass
		case <-ctx.Done():
			return
		}
	}
}

func (r *readerReconnector) fireReconnectOnRetryableError(stream topicStreamReader, err error) {
	if !isRetryableError(err) {
		return
	}

	select {
	case r.reconnectFromBadStream <- stream:
		// send signal
	default:
		// signal was send and not handled already
	}
}

func (r *readerReconnector) stream() (topicStreamReader, error) {
	r.m.RLock()
	defer r.m.RUnlock()

	if r.closedErr != nil {
		return nil, r.closedErr
	}

	return r.streamVal, r.streamErr
}

func (r *readerReconnector) handlePanic() {
	p := recover()

	if p != nil {
		r.Close(context.Background(), xerrors.WithStackTrace(fmt.Errorf("handled panic: %v", p)))
	}
}

func isRetryableError(err error) bool {
	return errors.Is(err, context.Canceled) || xerrors.RetryableError(err) != nil
}
