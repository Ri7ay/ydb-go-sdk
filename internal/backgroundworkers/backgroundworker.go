package backgroundworkers

import (
	"context"
	"runtime/pprof"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

// TODO: improve name

// A BackgroundWorker must not be copied after first use
type BackgroundWorker struct {
	ctx     context.Context
	workers sync.WaitGroup

	onceInit sync.Once

	m      xsync.Mutex
	cancel xcontext.CancelErrFunc
}

func New(parent context.Context) *BackgroundWorker {
	ctx, cancel := xcontext.WithErrCancel(parent)

	return &BackgroundWorker{
		ctx:    ctx,
		cancel: cancel,
	}
}

func (b *BackgroundWorker) Context() context.Context {
	b.init()

	return b.ctx
}

func (b *BackgroundWorker) Start(name string, f func(ctx context.Context)) {
	b.init()

	b.m.Lock()
	defer b.m.Unlock()

	if b.ctx.Err() != nil {
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

func (b *BackgroundWorker) Close(ctx context.Context, err error) error {
	b.init()

	b.cancel(err)

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
			b.ctx, b.cancel = xcontext.WithErrCancel(context.Background())
		}
	})
}
