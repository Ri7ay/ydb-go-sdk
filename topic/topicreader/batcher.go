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
	waiters  []batcherWaiter
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

	return b.addNeedLock(batch)
}

func (b *batcher) addNeedLock(batch *Batch) error {
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

	b.fireWaitersNeedLock()

	return nil
}

type batcherGetOptions struct {
	MaxCount int
}

func (b *batcher) Get(ctx context.Context, opts batcherGetOptions) (Batch, error) {
	res, ok := b.get(opts)
	if ok {
		return res, nil
	}

	resChan := b.createWaiter(opts)
	select {
	case res = <-resChan:
		return res, nil
	case <-ctx.Done():
		return Batch{}, ctx.Err()
	}
}

func (b *batcher) get(opts batcherGetOptions) (Batch, bool) {
	b.m.Lock()
	defer b.m.Unlock()

	return b.getNeedLock(opts)
}

func (b *batcher) getNeedLock(opts batcherGetOptions) (Batch, bool) {
	for k, v := range b.messages {
		if opts.MaxCount > 0 {
			head, rest := v.cutMessages(opts.MaxCount)
			if rest.isEmpty() {
				delete(b.messages, k)
			} else {
				b.messages[k] = rest
			}
			return head, true
		} else {
			delete(b.messages, k)
			return v, true
		}
	}

	return Batch{}, false
}

func (b *batcher) createWaiter(opts batcherGetOptions) <-chan Batch {
	waiter := batcherWaiter{
		Options: opts,
		Result:  make(chan Batch),
	}

	b.m.WithLock(func() {
		b.waiters = append(b.waiters, waiter)
	})

	return waiter.Result
}

func (b *batcher) fireWaitersNeedLock() {
	if len(b.waiters) == 0 {
		return
	}

	waiter := b.waiters[0]

	res, ok := b.get(waiter.Options)
	if !ok {
		return
	}

	copy(b.waiters, b.waiters[1:])
	b.waiters = b.waiters[:len(b.waiters)-1]

	select {
	case waiter.Result <- res:
		return
	default:
		_ = b.addNeedLock(&res)
	}
}

func (b *batcher) pubBatchNeedLock(batch Batch) {
	b.messages[batch.partitionSession] = batch
}

type batcherWaiter struct {
	Options batcherGetOptions
	Result  chan Batch
}
