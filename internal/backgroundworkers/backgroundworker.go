package backgroundworkers

import (
	"context"
	"runtime/pprof"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// TODO: improve name

// A BackgroundWorker must not be copied after first use
type BackgroundWorker struct {
	ctx     context.Context
	workers sync.WaitGroup

	onceInit sync.Once

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

func (b *BackgroundWorker) Start(name string, f func(ctx context.Context)) {
	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	if b.closed {
		return
	}

	b.workers.Add(1)
	go func() {
		defer b.workers.Done()

		pprof.Do(b.ctx, pprof.Labels("background", name), f)
	}()
}

func (b *BackgroundWorker) Done() <-chan struct{} {
	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	return b.ctx.Done()
}

func (b *BackgroundWorker) Close(ctx context.Context) error {
	b.init()

	b.m.WithLock(func() {
		b.closed = true
	})

	b.cancel()

	waitChan := make(chan struct{}, 1)

	go func() {
		b.workers.Wait()
		waitChan <- struct{}{}
	}()

	select {
	case <-waitChan:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *BackgroundWorker) init() {
	b.onceInit.Do(func() {
		if b.ctx == nil {
			b.ctx, b.cancel = context.WithCancel(context.Background())
		}
	})
}
