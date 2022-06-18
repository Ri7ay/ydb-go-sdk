package topicreader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatcher_AddBatch(t *testing.T) {
	session1 := &PartitionSession{}
	session2 := &PartitionSession{}

	m11 := Message{
		WrittenAt:        testTime(1),
		PartitionSession: session1,
	}
	m12 := Message{
		WrittenAt:        testTime(2),
		PartitionSession: session1,
	}
	m21 := Message{
		WrittenAt:        testTime(3),
		PartitionSession: session2,
	}
	m22 := Message{
		WrittenAt:        testTime(4),
		PartitionSession: session2,
	}

	batch1 := mustNewBatch(session1, []Message{m11, m12})
	batch2 := mustNewBatch(session2, []Message{m21})
	batch3 := mustNewBatch(session2, []Message{m22})

	b := newBatcher()
	require.NoError(t, b.AddBatch(batch1))
	require.NoError(t, b.AddBatch(batch2))
	require.NoError(t, b.AddBatch(batch3))

	expectedSession1 := newBatcherItemBatch(mustNewBatch(session1, []Message{m11, m12}))
	expectedSession2 := newBatcherItemBatch(mustNewBatch(session2, []Message{m21, m22}))

	expected := batcherMessagesMap{
		session1: expectedSession1,
		session2: expectedSession2,
	}
	require.Equal(t, expected, b.messages)
}

func TestBatcher_GetBatch(t *testing.T) {
	t.Run("SimpleGet", func(t *testing.T) {
		ctx := context.Background()
		batch := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}})

		b := newBatcher()
		require.NoError(t, b.AddBatch(batch))

		res, err := b.Get(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
	})

	t.Run("SimpleOneOfTwo", func(t *testing.T) {
		ctx := context.Background()
		session1 := &PartitionSession{}
		session2 := &PartitionSession{}
		batch := mustNewBatch(session1, []Message{{WrittenAt: testTime(1), PartitionSession: session1}})
		batch2 := mustNewBatch(session2, []Message{{WrittenAt: testTime(2), PartitionSession: session2}})

		b := newBatcher()
		require.NoError(t, b.AddBatch(batch))
		require.NoError(t, b.AddBatch(batch2))

		possibleResults := []batcherMessageOrderItem{newBatcherItemBatch(batch), newBatcherItemBatch(batch2)}

		res, err := b.Get(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Contains(t, possibleResults, res)
		require.Len(t, b.messages, 1)

		res2, err := b.Get(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Contains(t, possibleResults, res2)
		require.NotEqual(t, res, res2)
		require.Empty(t, b.messages)
	})

	t.Run("GetAfterPut", func(t *testing.T) {
		ctx := context.Background()
		batch := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}})

		b := newBatcher()

		go func() {
			time.Sleep(time.Millisecond)
			_ = b.AddBatch(batch)
		}()

		res, err := b.Get(ctx, batcherGetOptions{})
		require.NoError(t, err)
		require.Equal(t, newBatcherItemBatch(batch), res)
		require.Empty(t, b.messages)
	})

	t.Run("GetMaxOne", func(t *testing.T) {
		ctx := context.Background()

		m1 := Message{WrittenAt: testTime(1)}
		m2 := Message{WrittenAt: testTime(2)}
		batch := mustNewBatch(nil, []Message{m1, m2})

		b := newBatcher()
		require.NoError(t, b.AddBatch(batch))

		res, err := b.Get(ctx, batcherGetOptions{MaxCount: 1})
		require.NoError(t, err)

		expectedResult := newBatcherItemBatch(mustNewBatch(nil, []Message{m1}))
		require.Equal(t, expectedResult, res)

		expectedMessages := batcherMessagesMap{
			nil: newBatcherItemBatch(mustNewBatch(nil, []Message{m2})),
		}
		require.Equal(t, expectedMessages, b.messages)
	})
}

func TestBatcher_Find(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		b := newBatcher()
		findRes := b.findNeedLock()
		require.False(t, findRes.Ok)
	})
	t.Run("FoundEmptyFilter", func(t *testing.T) {
		session := &PartitionSession{}
		batch := mustNewBatch(session, []Message{{WrittenAt: testTime(1)}})

		b := newBatcher()

		require.NoError(t, b.AddBatch(batch))

		findRes := b.findNeedLock(batcherWaiter{})
		expectedResult := batcherResultCandidate{
			Key:         session,
			Result:      newBatcherItemBatch(batch),
			Rest:        batcherMessageOrderItem{},
			WaiterIndex: 0,
			Ok:          true,
		}
		require.Equal(t, expectedResult, findRes)
	})

	t.Run("FoundPartialBatchFilter", func(t *testing.T) {
		session := &PartitionSession{}
		batch := mustNewBatch(session, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		b := newBatcher()

		require.NoError(t, b.AddBatch(batch))

		findRes := b.findNeedLock(batcherWaiter{Options: batcherGetOptions{MaxCount: 1}})

		expectedResult := newBatcherItemBatch(mustNewBatch(session, []Message{{WrittenAt: testTime(1)}}))
		expectedRestBatch := newBatcherItemBatch(mustNewBatch(session, []Message{{WrittenAt: testTime(2)}}))

		expectedCandidate := batcherResultCandidate{
			Key:         session,
			Result:      expectedResult,
			Rest:        expectedRestBatch,
			WaiterIndex: 0,
			Ok:          true,
		}
		require.Equal(t, expectedCandidate, findRes)
	})
}

func TestBatcher_Apply(t *testing.T) {
	t.Run("New", func(t *testing.T) {
		session := &PartitionSession{}
		b := newBatcher()

		batch := mustNewBatch(session, []Message{{WrittenAt: testTime(1)}})
		foundRes := batcherResultCandidate{
			Key:  session,
			Rest: newBatcherItemBatch(batch),
		}
		b.applyNeedLock(foundRes)

		expectedMap := batcherMessagesMap{session: newBatcherItemBatch(batch)}
		require.Equal(t, expectedMap, b.messages)
	})

	t.Run("Delete", func(t *testing.T) {
		session := &PartitionSession{}
		b := newBatcher()

		batch := mustNewBatch(session, []Message{{WrittenAt: testTime(1)}})

		foundRes := batcherResultCandidate{
			Key:  session,
			Rest: newBatcherItemBatch(Batch{}),
		}

		b.messages = batcherMessagesMap{session: newBatcherItemBatch(batch)}

		b.applyNeedLock(foundRes)

		require.Empty(t, b.messages)
	})
}

func TestBatcherGetOptions_Split(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		opts := batcherGetOptions{}
		batch := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})
		head, rest, ok := opts.splitBatch(batch)

		require.Equal(t, batch, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)
	})
	t.Run("MinCount", func(t *testing.T) {
		opts := batcherGetOptions{MinCount: 2}
		batch1 := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest, ok := opts.splitBatch(batch1)
		require.True(t, head.isEmpty())
		require.True(t, rest.isEmpty())
		require.False(t, ok)

		head, rest, ok = opts.splitBatch(batch2)
		require.Equal(t, batch2, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)
	})
	t.Run("MaxCount", func(t *testing.T) {
		opts := batcherGetOptions{MaxCount: 2}
		batch1 := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}})
		batch2 := mustNewBatch(nil, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})
		batch3 := mustNewBatch(nil, []Message{{WrittenAt: testTime(11)}, {WrittenAt: testTime(12)}, {WrittenAt: testTime(13)}, {WrittenAt: testTime(14)}})

		head, rest, ok := opts.splitBatch(batch1)
		require.Equal(t, batch1, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)

		head, rest, ok = opts.splitBatch(batch2)
		require.Equal(t, batch2, head)
		require.True(t, rest.isEmpty())
		require.True(t, ok)

		head, rest, ok = opts.splitBatch(batch3)
		expectedHead := mustNewBatch(nil, []Message{{WrittenAt: testTime(11)}, {WrittenAt: testTime(12)}})
		expectedRest := mustNewBatch(nil, []Message{{WrittenAt: testTime(13)}, {WrittenAt: testTime(14)}})
		require.Equal(t, expectedHead, head)
		require.Equal(t, expectedRest, rest)
		require.True(t, ok)

	})
}

func mustNewBatch(session *PartitionSession, messages []Message) Batch {
	batch, err := newBatch(session, messages)
	if err != nil {
		panic(err)
	}
	return batch
}
