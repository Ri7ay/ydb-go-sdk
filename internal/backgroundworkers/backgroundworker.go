package backgroundworkers

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// TODO: improve name

// A BackgroundWorker must not be copied after first worker start.
type BackgroundWorker struct {
	ctx     context.Context
	workers sync.WaitGroup

	m      xsync.Mutex
	cancel context.CancelFunc
	closed bool
}

func New(parent context.Context) *BackgroundWorker {
	ctx, cancel := context.WithCancel(parent)

	return &BackgroundWorker{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (b *BackgroundWorker) Start(f func(ctx context.Context)) {
	b.m.Lock()
	defer b.m.Unlock()

	if b.closed {
		return
	}

	b.workers.Add(1)
	go func() {
		defer b.workers.Done()

		f(b.ctx)
	}()
}

func (b *BackgroundWorker) Done() <-chan struct{} {
	return b.ctx.Done()
}

func (b *BackgroundWorker) Close() error {
	b.m.WithLock(func() {
		b.closed = true
	})
	b.cancel()
	b.workers.Wait()

	return nil
}

func (b *BackgroundWorker) isClosed() bool {
	b.m.Lock()
	defer b.m.Unlock()

	return b.closed
}

func (b *BackgroundWorker) init() {
	if b.ctx == nil {
		b.ctx, b.cancel = context.WithCancel(context.Background())
	}
}
