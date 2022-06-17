package topicreader

import (
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

type getBatchOptions struct{}
