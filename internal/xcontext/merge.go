package xcontext

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

func Merge(c ...context.Context) (context.Context, CancelErrFunc) {
	res := &mergeContext{
		contexts:        c,
		done:            make(chan struct{}),
		stopDoneWaiter:  make(chan struct{}),
		doneCloseLocker: new(int32),
	}

	// finalizer need for prevent goroutine leak if nobody cancel child process
	runtime.SetFinalizer(res, mergeContextFinalizer)

	doneChannels := make([]<-chan struct{}, len(c))
	for i := range c {
		doneChannels[i] = c[i].Done()
	}

	go waitFirstOfChannels(doneChannels, res.stopDoneWaiter, res.doneCloseLocker, res.done)

	return res, res.cancel
}

type mergeContext struct {
	contexts        []context.Context
	stopDoneWaiter  chan struct{}
	done            chan struct{}
	doneCloseLocker *int32

	m      sync.Mutex
	closed bool
	err    error
}

func (m *mergeContext) Deadline() (deadline time.Time, ok bool) {
	// find min deadline
	for _, c := range m.contexts {
		if d, localOk := c.Deadline(); localOk {
			if d.Before(deadline) || !ok {
				ok = true
				deadline = d
			}
		}
	}

	return deadline, ok
}

func (m *mergeContext) Done() <-chan struct{} {
	return m.done
}

func (m *mergeContext) Err() error {
	m.m.Lock()
	defer m.m.Unlock()

	return m.errUnderLock()
}

func (m *mergeContext) errUnderLock() error {
	if m.err == nil {
		for _, c := range m.contexts {
			if err := c.Err(); err != nil {
				m.err = err
				break
			}
		}
	}

	return m.err
}

func (m *mergeContext) Value(key interface{}) interface{} {
	for _, c := range m.contexts {
		if res := c.Value(key); res != nil {
			return res
		}
	}

	return nil
}

func (m *mergeContext) cancel(err error) {
	if err == nil {
		panic(errCancelWithNilError)
	}

	m.m.Lock()
	if m.err == nil {
		m.err = err
	}
	m.m.Unlock()

	if atomic.CompareAndSwapInt32(m.doneCloseLocker, 0, 1) {
		close(m.done)
	}

	m.close()
}

func (m *mergeContext) close() {
	m.m.Lock()
	defer m.m.Unlock()
	m.closeUnderLock()
}

func (m *mergeContext) closeUnderLock() {
	if m.closed {
		return
	}

	runtime.SetFinalizer(m, nil)
	close(m.stopDoneWaiter)
}

func mergeContextFinalizer(m *mergeContext) {
	m.close()
}

func waitFirstOfChannels(channels []<-chan struct{}, stopWait chan struct{}, closeLocker *int32, oneOfChannelsClosed chan struct{}) {
	if len(channels) == 0 {
		panic("can't wait nothing channels")
	}

	cases := make([]reflect.SelectCase, len(channels)+1)
	for i := range channels {
		cases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(channels[i]),
		}
	}
	cases[len(channels)] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(stopWait),
	}

	selected, _, _ := reflect.Select(cases)
	if selected < len(channels) && atomic.CompareAndSwapInt32(closeLocker, 0, 1) {
		close(oneOfChannelsClosed)
	}
	return
}
