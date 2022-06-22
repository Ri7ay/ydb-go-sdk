package topicreader

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

const (
	// TODO: improve
	reconnectDuration      = time.Second * 60
	streamPollingInterval  = time.Second / 10
	forceReconnectInterval = time.Hour * 24 * 365 * 100 // never
)

type readerConnectFunc func(ctx context.Context) (batchedStreamReader, error)

type readerReconnector struct {
	background backgroundworkers.BackgroundWorker

	readerConnect readerConnectFunc

	reconnectFromBadStream chan batchedStreamReader

	closeOnce sync.Once

	m                          xsync.RWMutex
	streamConnectionInProgress chan struct{} // opened if connection in progress, closed if connection established
	streamVal                  batchedStreamReader
	streamErr                  error
	closedErr                  error
}

func newReaderReconnector(connectCtx context.Context, connector readerConnectFunc) *readerReconnector {
	res := &readerReconnector{
		readerConnect: connector,
		streamErr:     errUnconnected,
	}

	res.initChannels()
	res.start()

	return res
}

func (r *readerReconnector) ReadMessageBatch(ctx context.Context, opts readMessageBatchOptions) (Batch, error) {
	if ctx.Err() != nil {
		return Batch{}, ctx.Err()
	}

	for {
		stream, err := r.stream(ctx)
		switch {
		case isRetryableError(err):
			r.fireReconnectOnRetryableError(stream, err)
			runtime.Gosched()
			continue
		case err != nil:
			return Batch{}, err
		default:
			// pass
		}

		res, err := stream.ReadMessageBatch(ctx, opts)
		if isRetryableError(err) {
			r.fireReconnectOnRetryableError(stream, err)
			runtime.Gosched()
			continue
		}
		return res, err
	}
}

func (r *readerReconnector) Commit(ctx context.Context, commitRange CommitRange) error {
	stream, err := r.stream(ctx)
	if err != nil {
		return err
	}

	err = stream.Commit(ctx, commitRange)
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
			r.streamVal.Close(ctx, xerrors.WithStackTrace(errReaderClosed))
		}
	})
}

func (r *readerReconnector) start() {
	r.background.Start("reconnector-loop", r.reconnectionLoop)

	// start first connection
	r.reconnectFromBadStream <- nil
}

func (r *readerReconnector) initChannels() {
	r.reconnectFromBadStream = make(chan batchedStreamReader, 1)
	r.streamConnectionInProgress = make(chan struct{})
	close(r.streamConnectionInProgress) // no progress at start
}

func (r *readerReconnector) reconnectionLoop(ctx context.Context) {
	defer r.handlePanic()

	// TODO: add delay for repeats
	for {
		select {
		case <-ctx.Done():
			return

		case oldReader := <-r.reconnectFromBadStream:
			r.reconnect(ctx, oldReader)
		}
	}
}

func (r *readerReconnector) reconnect(ctx context.Context, oldReader batchedStreamReader) {
	if ctx.Err() != nil {
		return
	}

	stream, _ := r.stream(ctx)
	if oldReader != stream {
		return
	}

	connectionInProgress := make(chan struct{})
	defer close(connectionInProgress)

	r.m.WithLock(func() {
		r.streamConnectionInProgress = connectionInProgress
	})

	if oldReader != nil {
		oldReader.Close(ctx, xerrors.WithStackTrace(errors.New("ydb: reconnect to pq grpc stream")))
	}

	reconnectCtx, cancel := context.WithTimeout(ctx, reconnectDuration)
	newStream, err := r.readerConnect(reconnectCtx)
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

func (r *readerReconnector) fireReconnectOnRetryableError(stream batchedStreamReader, err error) {
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

func (r *readerReconnector) stream(ctx context.Context) (batchedStreamReader, error) {
	var err error
	var connectionChan chan struct{}
	r.m.WithRLock(func() {
		connectionChan = r.streamConnectionInProgress
		if r.closedErr != nil {
			err = r.closedErr
			return
		}
	})
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.background.Done():
		return nil, r.closedErr
	case <-connectionChan:
		var reader batchedStreamReader
		r.m.WithRLock(func() {
			reader = r.streamVal
			err = r.streamErr
		})
		r.fireReconnectOnRetryableError(reader, err)
		return reader, err
	}
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
