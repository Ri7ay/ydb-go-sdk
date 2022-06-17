package topicreader

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBatch_New(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		session := &PartitionSession{}
		m1 := Message{
			CommitOffset:     CommitOffset{Offset: 1, ToOffset: 2},
			PartitionSession: session,
		}
		m2 := Message{
			CommitOffset:     CommitOffset{Offset: 2, ToOffset: 3},
			PartitionSession: session,
		}
		batch, err := newBatch(session, []Message{m1, m2})
		require.NoError(t, err)

		expected := Batch{
			Messages:         []Message{m1, m2},
			CommitOffset:     CommitOffset{Offset: 1, ToOffset: 3},
			partitionSession: session,
		}
		require.Equal(t, expected, batch)
	})
}

func TestBatch_Extend(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		session := &PartitionSession{}
		m1 := Message{
			WrittenAt:    time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			CommitOffset: CommitOffset{Offset: 10, ToOffset: 11},
		}
		m2 := Message{
			WrittenAt:    time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			CommitOffset: CommitOffset{Offset: 11, ToOffset: 12},
		}

		b1 := Batch{
			Messages:         []Message{m1},
			CommitOffset:     m1.CommitOffset,
			partitionSession: session,
		}

		b2 := Batch{
			Messages:         []Message{m2},
			CommitOffset:     m2.CommitOffset,
			partitionSession: session,
		}
		require.NoError(t, b1.extendFromBatch(&b2))

		expected := Batch{
			Messages:         []Message{m1, m2},
			CommitOffset:     CommitOffset{Offset: 10, ToOffset: 12},
			partitionSession: session,
		}
		require.Equal(t, expected, b1)
	})
	t.Run("BadInterval", func(t *testing.T) {
		m1 := Message{
			WrittenAt:    time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			CommitOffset: CommitOffset{Offset: 10, ToOffset: 11},
		}
		m2 := Message{
			WrittenAt:    time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			CommitOffset: CommitOffset{Offset: 20, ToOffset: 30},
		}

		b1 := Batch{
			Messages:     []Message{m1},
			CommitOffset: m1.CommitOffset,
		}

		b2 := Batch{
			Messages:     []Message{m2},
			CommitOffset: m2.CommitOffset,
		}
		require.Error(t, b1.extendFromBatch(&b2))

		expected := Batch{
			Messages:     []Message{m1},
			CommitOffset: m1.CommitOffset,
		}
		require.Equal(t, expected, b1)
	})
	t.Run("BadSession", func(t *testing.T) {
		m1 := Message{
			WrittenAt:    time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			CommitOffset: CommitOffset{Offset: 10, ToOffset: 11},
		}
		m2 := Message{
			WrittenAt:    time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			CommitOffset: CommitOffset{Offset: 11, ToOffset: 12},
		}

		session1 := &PartitionSession{}
		session2 := &PartitionSession{}
		b1 := &Batch{
			Messages:         []Message{m1},
			CommitOffset:     m1.CommitOffset,
			partitionSession: session1,
		}

		b2 := &Batch{
			Messages:         []Message{m2},
			CommitOffset:     m2.CommitOffset,
			partitionSession: session2,
		}
		require.Error(t, b1.extendFromBatch(b2))

		expected := &Batch{
			Messages:         []Message{m1},
			CommitOffset:     m1.CommitOffset,
			partitionSession: session1,
		}
		require.Equal(t, expected, b1)
	})
}

func testTime(num int) time.Time {
	return time.Date(2022, 6, 17, 0, 0, 0, num, time.UTC)
}
