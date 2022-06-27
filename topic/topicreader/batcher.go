package topicreader

import (
	"context"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

type batcher struct {
	waiterId int64

	m        xsync.Mutex
	messages batcherMessagesMap
	waiters  []batcherWaiter
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

func (b *batcher) PushBatch(batch Batch) error {
	b.m.Lock()
	defer b.m.Unlock()

	return b.addNeedLock(batch.partitionSession, newBatcherItemBatch(batch))
}

func (b *batcher) PushRawMessage(session *partitionSession, m rawtopicreader.ServerMessage) error {
	b.m.Lock()
	defer b.m.Unlock()

	return b.addNeedLock(session, newBatcherItemRawMessage(m))
}

func (b *batcher) addNeedLock(session *partitionSession, item batcherMessageOrderItem) error {
	var currentItems batcherMessageOrderItems
	var ok bool
	var err error
	if currentItems, ok = b.messages[session]; ok {
		if currentItems, err = currentItems.Append(item); err != nil {
			return err
		}
	} else {
		currentItems = batcherMessageOrderItems{item}
	}

	b.messages[session] = currentItems

	b.fireWaitersNeedLock()

	return nil
}

type batcherGetOptions struct {
	MinCount        int
	MaxCount        int
	rawMessagesOnly bool
}

func newBatcherGetOptions() batcherGetOptions {
	return batcherGetOptions{}
}

func (o batcherGetOptions) cutBatchItemsHead(items batcherMessageOrderItems) (
	head batcherMessageOrderItem,
	rest batcherMessageOrderItems,
	ok bool,
) {
	notFound := func() (batcherMessageOrderItem, batcherMessageOrderItems, bool) {
		return batcherMessageOrderItem{}, batcherMessageOrderItems{}, false
	}
	if len(items) == 0 {
		return notFound()
	}

	if items[0].IsBatch() {
		if o.rawMessagesOnly {
			return notFound()
		}

		batchHead, batchRest, ok := o.splitBatch(items[0].Batch)

		if !ok {
			return notFound()
		}

		head = newBatcherItemBatch(batchHead)
		rest = items.ReplaceHeadItem(newBatcherItemBatch(batchRest))
		return head, rest, true
	}

	return items[0], items[1:], true
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

func (b *batcher) Pop(ctx context.Context, opts batcherGetOptions) (_ batcherMessageOrderItem, err error) {
	if err := ctx.Err(); err != nil {
		return batcherMessageOrderItem{}, err
	}
	var findRes batcherResultCandidate
	b.m.WithLock(func() {
		findRes = b.findNeedLock(0, []batcherWaiter{{Options: opts}})
		if !findRes.Ok {
			return
		}
		b.applyNeedLock(findRes)
	})
	if findRes.Ok {
		return findRes.Result, nil
	}

	waiter := b.createWaiter(opts)
	defer func() {
		removeWaiterErr := b.RemoveWaiter(waiter)
		if err == nil {
			err = removeWaiterErr
		}
	}()

	select {
	case batch := <-waiter.Result:
		return batch, nil
	case <-ctx.Done():
		return batcherMessageOrderItem{}, ctx.Err()
	}
}

func (b *batcher) createWaiter(opts batcherGetOptions) batcherWaiter {
	waiter := batcherWaiter{
		ID:               atomic.AddInt64(&b.waiterId, 1),
		Options:          opts,
		Result:           make(chan batcherMessageOrderItem),
		finishWaitSignal: make(emptyChan),
	}

	b.m.WithLock(func() {
		b.waiters = append(b.waiters, waiter)
	})

	return waiter
}

func (b *batcher) RemoveWaiter(waiter batcherWaiter) error {
	close(waiter.finishWaitSignal)

	return b.removeWaiterByID(waiter.ID)
}

func (b *batcher) removeWaiterByID(waiterID int64) error {
	b.m.Lock()
	defer b.m.Unlock()

	for i := 0; i < len(b.waiters); i++ {
		if b.waiters[i].ID == waiterID {
			b.removeWaiterByIndexNeedLock(i)
			return nil
		}
	}

	return xerrors.NewWithStackTrace("ydb: remove unexpected waiter")
}

func (b *batcher) fireWaitersNeedLock() {
	startIndex := 0
	for {
		resCandidate := b.findNeedLock(startIndex, b.waiters)
		if !resCandidate.Ok {
			return
		}

		waiter := b.waiters[resCandidate.WaiterIndex]

		select {
		case waiter.Result <- resCandidate.Result:
			// waiter receive the result, commit it
			b.applyNeedLock(resCandidate)
			return
		case <-waiter.finishWaitSignal:
			startIndex = resCandidate.WaiterIndex + 1
		}
	}
}

func (b *batcher) removeWaiterByIndexNeedLock(index int) {
	copy(b.waiters[index:], b.waiters[index+1:])
	b.waiters = b.waiters[:len(b.waiters)-1]
}

type batcherResultCandidate struct {
	Key         *partitionSession
	Result      batcherMessageOrderItem
	Rest        batcherMessageOrderItems
	WaiterIndex int
	Ok          bool
}

func newBatcherResultCandidate(
	key *partitionSession,
	result batcherMessageOrderItem,
	rest batcherMessageOrderItems,
	waiterIndex int,
	ok bool,
) batcherResultCandidate {
	return batcherResultCandidate{
		Key:         key,
		Result:      result,
		Rest:        rest,
		WaiterIndex: waiterIndex,
		Ok:          ok,
	}
}

func (b *batcher) findNeedLock(startIndex int, waiters []batcherWaiter) batcherResultCandidate {
	if len(waiters) == 0 || len(b.messages) == 0 {
		return batcherResultCandidate{}
	}

	rawMessageOpts := batcherGetOptions{rawMessagesOnly: true}

	var batchResult batcherResultCandidate
	needBatchResult := true

	for k, items := range b.messages {
		head, rest, ok := rawMessageOpts.cutBatchItemsHead(items)
		if ok {
			return newBatcherResultCandidate(k, head, rest, len(waiters)-1, true)
		}

		if needBatchResult {
			for waiterIndex, waiter := range waiters[startIndex:] {
				head, rest, ok = waiter.Options.cutBatchItemsHead(items)
				if !ok {
					continue
				}

				needBatchResult = false
				batchResult = newBatcherResultCandidate(k, head, rest, waiterIndex, true)
			}
		}
	}

	return batchResult
}

func (b *batcher) applyNeedLock(res batcherResultCandidate) {
	if res.Rest.IsEmpty() && res.WaiterIndex >= 0 {
		delete(b.messages, res.Key)
	} else {
		b.messages[res.Key] = res.Rest
	}
}

type batcherMessagesMap map[*partitionSession]batcherMessageOrderItems

type batcherMessageOrderItems []batcherMessageOrderItem

func (items batcherMessageOrderItems) Append(item batcherMessageOrderItem) (batcherMessageOrderItems, error) {
	if len(items) == 0 {
		return append(items, item), nil
	}

	lastItem := &items[len(items)-1]
	if item.IsBatch() && lastItem.IsBatch() {
		newItem, err := lastItem.Batch.append(item.Batch)
		if err != nil {
			return nil, err
		}
		lastItem.Batch = newItem
		return items, nil
	}

	return append(items, item), nil
}

func (items batcherMessageOrderItems) IsEmpty() bool {
	return len(items) == 0
}

func (items batcherMessageOrderItems) ReplaceHeadItem(item batcherMessageOrderItem) batcherMessageOrderItems {
	if item.IsEmpty() {
		return items[1:]
	}

	res := make(batcherMessageOrderItems, len(items))
	res[0] = item
	copy(res[1:], items[1:])
	return res
}

type batcherMessageOrderItem struct {
	Batch      Batch
	RawMessage rawtopicreader.ServerMessage
}

type batcherWaiter struct {
	ID               int64
	Options          batcherGetOptions
	Result           chan batcherMessageOrderItem
	finishWaitSignal emptyChan
}
