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
			CommitRange: CommitRange{Offset: 1, EndOffset: 2, partitionSession: session},
		}
		m2 := Message{
			CommitRange: CommitRange{Offset: 2, EndOffset: 3, partitionSession: session},
		}
		batch, err := newBatch(session, []Message{m1, m2})
		require.NoError(t, err)

		expected := Batch{
			Messages:         []Message{m1, m2},
			CommitRange:      CommitRange{Offset: 1, EndOffset: 3},
			partitionSession: session,
		}
		require.Equal(t, expected, batch)
	})
}

func TestBatch_Cut(t *testing.T) {
	t.Run("Full", func(t *testing.T) {
		session := &PartitionSession{}
		batch, _ := newBatch(session, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest := batch.cutMessages(100)

		require.Equal(t, batch, head)
		require.True(t, rest.isEmpty())
	})
	t.Run("Zero", func(t *testing.T) {
		session := &PartitionSession{}
		batch, _ := newBatch(session, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest := batch.cutMessages(0)

		require.Equal(t, batch, rest)
		require.True(t, head.isEmpty())
	})
	t.Run("Middle", func(t *testing.T) {
		session := &PartitionSession{}
		batch, _ := newBatch(session, []Message{{WrittenAt: testTime(1)}, {WrittenAt: testTime(2)}})

		head, rest := batch.cutMessages(1)

		expectedBatchHead, _ := newBatch(session, []Message{{WrittenAt: testTime(1)}})
		expectedBatchRest, _ := newBatch(session, []Message{{WrittenAt: testTime(2)}})
		require.Equal(t, expectedBatchHead, head)
		require.Equal(t, expectedBatchRest, rest)
	})
}

func TestBatch_Extend(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		session := &PartitionSession{}
		m1 := Message{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			CommitRange: CommitRange{Offset: 10, EndOffset: 11},
		}
		m2 := Message{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			CommitRange: CommitRange{Offset: 11, EndOffset: 12},
		}

		b1 := Batch{
			Messages:         []Message{m1},
			CommitRange:      m1.CommitRange,
			partitionSession: session,
		}

		b2 := Batch{
			Messages:         []Message{m2},
			CommitRange:      m2.CommitRange,
			partitionSession: session,
		}
		res, err := b1.append(b2)
		require.NoError(t, err)

		expected := Batch{
			Messages:         []Message{m1, m2},
			CommitRange:      CommitRange{Offset: 10, EndOffset: 12},
			partitionSession: session,
		}
		require.Equal(t, expected, res)
	})
	t.Run("BadInterval", func(t *testing.T) {
		m1 := Message{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			CommitRange: CommitRange{Offset: 10, EndOffset: 11},
		}
		m2 := Message{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			CommitRange: CommitRange{Offset: 20, EndOffset: 30},
		}

		b1 := Batch{
			Messages:    []Message{m1},
			CommitRange: m1.CommitRange,
		}

		b2 := Batch{
			Messages:    []Message{m2},
			CommitRange: m2.CommitRange,
		}
		res, err := b1.append(b2)
		require.Error(t, err)

		require.Equal(t, Batch{}, res)
	})
	t.Run("BadSession", func(t *testing.T) {
		m1 := Message{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 1, time.UTC),
			CommitRange: CommitRange{Offset: 10, EndOffset: 11},
		}
		m2 := Message{
			WrittenAt:   time.Date(2022, 6, 17, 15, 15, 0, 2, time.UTC),
			CommitRange: CommitRange{Offset: 11, EndOffset: 12},
		}

		session1 := &PartitionSession{}
		session2 := &PartitionSession{}
		b1 := Batch{
			Messages:         []Message{m1},
			CommitRange:      m1.CommitRange,
			partitionSession: session1,
		}

		b2 := Batch{
			Messages:         []Message{m2},
			CommitRange:      m2.CommitRange,
			partitionSession: session2,
		}
		res, err := b1.append(b2)
		require.Error(t, err)
		require.Equal(t, Batch{}, res)
	})
}

func TestSplitBytesByBatches(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		require.NoError(t, splitBytesByMessagesInBatches(nil, 0))
	})
	t.Run("BytesToNoMessages", func(t *testing.T) {
		require.Error(t, splitBytesByMessagesInBatches(nil, 10))
	})

	t.Run("MetadataOnlyEqually", func(t *testing.T) {
		batch, err := newBatch(nil, make([]Message, 3))
		require.NoError(t, err)
		require.NoError(t, splitBytesByMessagesInBatches([]Batch{batch}, 30))

		for _, mess := range batch.Messages {
			require.Equal(t, 10, mess.bufferBytesAccount)
		}
	})

	t.Run("MetadataOnlyWithReminder", func(t *testing.T) {
		batch, err := newBatch(nil, make([]Message, 3))
		require.NoError(t, err)
		require.NoError(t, splitBytesByMessagesInBatches([]Batch{batch}, 5))

		require.Equal(t, 2, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 2, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 1, batch.Messages[2].bufferBytesAccount)
	})

	t.Run("OnlyData", func(t *testing.T) {
		batch, err := newBatch(nil, make([]Message, 3))
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 10
		}

		require.NoError(t, splitBytesByMessagesInBatches([]Batch{batch}, 30))
		require.Equal(t, 10, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[2].bufferBytesAccount)
	})
	t.Run("DataAndMetadataEqually", func(t *testing.T) {
		batch, err := newBatch(nil, make([]Message, 3))
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 5
		}

		require.NoError(t, splitBytesByMessagesInBatches([]Batch{batch}, 30))
		require.Equal(t, 10, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[2].bufferBytesAccount)
	})
	t.Run("DataAndMetadataWithReminder", func(t *testing.T) {
		batch, err := newBatch(nil, make([]Message, 3))
		require.NoError(t, err)
		for i := range batch.Messages {
			batch.Messages[i].rawDataLen = 5
		}

		require.NoError(t, splitBytesByMessagesInBatches([]Batch{batch}, 32))
		require.Equal(t, 11, batch.Messages[0].bufferBytesAccount)
		require.Equal(t, 11, batch.Messages[1].bufferBytesAccount)
		require.Equal(t, 10, batch.Messages[2].bufferBytesAccount)
	})
}

func testTime(num int) time.Time {
	return time.Date(2022, 6, 17, 0, 0, 0, num, time.UTC)
}
