package locker

import "sync"

type Locker struct {
	sync.Mutex
}

func (l *Locker) WithLock(f func()) {
	l.Lock()
	defer l.Unlock()

	f()
}

type RWLocker struct {
	sync.RWMutex
}

func (l *RWLocker) WithLock(f func()) {
	l.Lock()
	defer l.Unlock()

	f()
}

func (l *RWLocker) WithReadLock(f func()) {
	l.RLock()
	defer l.Unlock()

	f()
}
