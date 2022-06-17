package topicreader

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type topicPartitionSessionID struct {
	Topic string
	partitionSessionID
}

type batcher struct {
	m        xsync.Mutex
	messages batcherMessagesMap
	waiters  []getBatchOptions
}

type batcherMessagesMap map[*PartitionSession]Batch

func newBatcher() *batcher {
	return &batcher{
		messages: make(batcherMessagesMap),
	}
}

func (b *batcher) Add(batch *Batch) error {
	b.m.Lock()
	defer b.m.Unlock()

	var currentBatch Batch
	var ok bool
	if currentBatch, ok = b.messages[batch.partitionSession]; ok {
		if err := currentBatch.extendFromBatch(batch); err != nil {
			return err
		}
	} else {
		currentBatch = *batch
	}

	b.messages[batch.partitionSession] = currentBatch
	return nil
}

func (b *batcher) Get(ctx context.Context) (Batch, error) {
	res, ok := b.getNeedLock()
	if ok {
		return res, nil
	}

	resChan := b.createWaiter()
	select {
	case res = <-resChan:
		return res, nil
	case <-ctx.Done():
		return Batch{}, ctx.Err()
	}
}

func (b *batcher) get() (Batch, bool) {
	b.m.Lock()
	defer b.m.Unlock()

	return b.getNeedLock()
}

func (b *batcher) getNeedLock() (Batch, bool) {
	for k, v := range b.messages {
		delete(b.messages, k)
		return v, true
	}

	return Batch{}, false
}

func (b *batcher) createWaiter() <-chan Batch {
	waiter := batcherWaiter{
		Result: make(chan Batch),
	}

	b.m.WithLock(func() {
		b.waiters = append(b.waiters, waiter)
	})

	return waiter.Result
}

func (b *batcher) fireWaiters() {
	b.m.WithLock(func() {
		if len(b.waiters)
	})
}

type batcherWaiter struct {
	Result chan Batch
}
