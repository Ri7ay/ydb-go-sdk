package topicreader

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type batcher struct {
	m        xsync.Mutex
	messages batcherMessagesMap
	waiters  []batcherWaiter
}

type batcherMessagesMap map[*PartitionSession]batcherMessageOrderItem

type batcherMessageOrderItem struct {
	Batch      Batch
	RawMessage rawtopicreader.ServerMessage
}

func newBatcherItemBatch(b Batch) batcherMessageOrderItem {
	return batcherMessageOrderItem{Batch: b}
}

func newBatcherItemRawMessage(b rawtopicreader.ServerMessage) batcherMessageOrderItem {
	return batcherMessageOrderItem{RawMessage: b}
}

func (item *batcherMessageOrderItem) IsBatch() bool {
	return !item.Batch.isEmpty()
}

func (item *batcherMessageOrderItem) IsRawMessage() bool {
	return item.RawMessage != nil
}

func (item *batcherMessageOrderItem) IsEmpty() bool {
	return item.RawMessage == nil && item.Batch.isEmpty()
}

func newBatcher() *batcher {
	return &batcher{
		messages: make(batcherMessagesMap),
	}
}

func (b *batcher) AddBatch(batch Batch) error {
	b.m.Lock()
	defer b.m.Unlock()

	return b.addNeedLock(newBatcherItemBatch(batch))
}

func (b *batcher) addNeedLock(item batcherMessageOrderItem) error {
	var currentItem batcherMessageOrderItem
	var ok bool
	var err error
	if currentItem, ok = b.messages[item.Batch.partitionSession]; ok {
		if currentItem.Batch, err = currentItem.Batch.append(item.Batch); err != nil {
			return err
		}
	} else {
		currentItem = item
	}

	b.messages[item.Batch.partitionSession] = currentItem

	b.fireWaitersNeedLock()

	return nil
}

type batcherGetOptions struct {
	MinCount int
	MaxCount int
}

func (o batcherGetOptions) splitBatch(batch Batch) (head, rest Batch, ok bool) {
	notFound := func() (Batch, Batch, bool) {
		return Batch{}, Batch{}, false
	}

	if len(batch.Messages) < o.MinCount {
		return notFound()
	}

	if o.MaxCount == 0 {
		return batch, Batch{}, true
	}

	head, rest = batch.cutMessages(o.MaxCount)
	return head, rest, true
}

func (b *batcher) Get(ctx context.Context, opts batcherGetOptions) (batcherMessageOrderItem, error) {
	var findRes batcherResultCandidate
	b.m.WithLock(func() {
		findRes = b.findNeedLock(batcherWaiter{Options: opts})
		if !findRes.Ok {
			return
		}
		b.applyNeedLock(findRes)
	})
	if findRes.Ok {
		return findRes.Result, nil
	}

	resChan := b.createWaiter(opts)
	select {
	case batch := <-resChan:
		return batch, nil
	case <-ctx.Done():
		return batcherMessageOrderItem{}, ctx.Err()
	}
}

func (b *batcher) createWaiter(opts batcherGetOptions) <-chan batcherMessageOrderItem {
	waiter := batcherWaiter{
		Options: opts,
		Result:  make(chan batcherMessageOrderItem),
	}

	b.m.WithLock(func() {
		b.waiters = append(b.waiters, waiter)
	})

	return waiter.Result
}

func (b *batcher) fireWaitersNeedLock() {
	for {
		resCandidate := b.findNeedLock(b.waiters...)
		if !resCandidate.Ok {
			return
		}

		waiter := b.removeWaiterNeedLock(resCandidate.WaiterIndex)

		select {
		case waiter.Result <- resCandidate.Result:
			// waiter receive the result, commit it
			b.applyNeedLock(resCandidate)
			return
		default:
			// waiter cancelled, try with other waiter
		}
	}
}

func (b *batcher) removeWaiterNeedLock(index int) batcherWaiter {
	waiter := b.waiters[index]

	copy(b.waiters[index:], b.waiters[index+1:])
	b.waiters = b.waiters[:len(b.waiters)-1]

	return waiter
}

type batcherResultCandidate struct {
	Key         *PartitionSession
	Result      batcherMessageOrderItem
	Rest        batcherMessageOrderItem
	WaiterIndex int
	Ok          bool
}

func (b *batcher) findNeedLock(waiters ...batcherWaiter) batcherResultCandidate {
	if len(waiters) == 0 || len(b.messages) == 0 {
		return batcherResultCandidate{}
	}

	for k, item := range b.messages {
		for waiterIndex, waiter := range waiters {
			head, rest, ok := waiter.Options.splitBatch(item.Batch)
			if !ok {
				continue
			}
			return batcherResultCandidate{
				Key:         k,
				Result:      newBatcherItemBatch(head),
				Rest:        newBatcherItemBatch(rest),
				WaiterIndex: waiterIndex,
				Ok:          true,
			}
		}
	}

	return batcherResultCandidate{}
}

func (b *batcher) applyNeedLock(res batcherResultCandidate) {
	if res.Rest.IsEmpty() {
		delete(b.messages, res.Key)
	} else {
		b.messages[res.Key] = res.Rest
	}
}

type batcherWaiter struct {
	Options batcherGetOptions
	Result  chan batcherMessageOrderItem
}
