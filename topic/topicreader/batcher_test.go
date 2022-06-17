package topicreader

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatcher_Add(t *testing.T) {
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

	batch1, err := newBatch(session1, []Message{m11, m12})
	batch2, err := newBatch(session2, []Message{m21})
	batch3, err := newBatch(session2, []Message{m22})
	require.NoError(t, err)

	b := newBatcher()
	require.NoError(t, b.Add(&batch1))
	require.NoError(t, b.Add(&batch2))
	require.NoError(t, b.Add(&batch3))

	expectedSession1, _ := newBatch(session1, []Message{m11, m12})
	expectedSession2, _ := newBatch(session2, []Message{m21, m22})

	expected := batcherMessagesMap{
		session1: expectedSession1,
		session2: expectedSession2,
	}
	require.Equal(t, expected, b.messages)
}

func TestBatcher_GetBatch(t *testing.T) {
	t.Run("SimpleGet", func(t *testing.T) {
		ctx := context.Background()
		batch, err := newBatch(nil, []Message{{WrittenAt: testTime(1)}})
		require.NoError(t, err)

		b := newBatcher()
		require.NoError(t, b.Add(&batch))

		res, err := b.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	t.Run("GetAfterPut", func(t *testing.T) {
		ctx := context.Background()
		batch, err := newBatch(nil, []Message{{WrittenAt: testTime(1)}})
		require.NoError(t, err)

		b := newBatcher()

		go func() {
			time.Sleep(time.Millisecond)
			_ = b.Add(&batch)
		}()

		res, err := b.Get(ctx)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})
}
