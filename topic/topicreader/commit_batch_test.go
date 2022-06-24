package topicreader

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

func TestCompressCommitsInplace(t *testing.T) {
	session1 := &PartitionSession{partitionSessionID: 1}
	session2 := &PartitionSession{partitionSessionID: 2}
	table := []struct {
		name     string
		source   []commitRange
		expected []commitRange
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []commitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
			},
			expected: []commitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressedToOne",
			source: []commitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
				{
					Offset:           2,
					EndOffset:        5,
					partitionSession: session1,
				},
				{
					Offset:           5,
					EndOffset:        10,
					partitionSession: session1,
				},
			},
			expected: []commitRange{
				{
					Offset:           1,
					EndOffset:        10,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressedUnordered",
			source: []commitRange{
				{
					Offset:           5,
					EndOffset:        10,
					partitionSession: session1,
				},
				{
					Offset:           2,
					EndOffset:        5,
					partitionSession: session1,
				},
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
			},
			expected: []commitRange{
				{
					Offset:           1,
					EndOffset:        10,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressDifferentSessionsSeparated",
			source: []commitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
				{
					Offset:           2,
					EndOffset:        3,
					partitionSession: session2,
				},
			},
			expected: []commitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
				{
					Offset:           2,
					EndOffset:        3,
					partitionSession: session2,
				},
			},
		},
		{
			name: "CompressTwoSessions",
			source: []commitRange{
				{
					Offset:           1,
					EndOffset:        1,
					partitionSession: session1,
				},
				{
					Offset:           2,
					EndOffset:        3,
					partitionSession: session2,
				},
				{
					Offset:           1,
					EndOffset:        3,
					partitionSession: session1,
				},
				{
					Offset:           3,
					EndOffset:        5,
					partitionSession: session2,
				},
			},
			expected: []commitRange{
				{
					Offset:           1,
					EndOffset:        3,
					partitionSession: session1,
				},
				{
					Offset:           2,
					EndOffset:        5,
					partitionSession: session2,
				},
			},
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var v CommitRages
			v.ranges = test.source
			v.optimize()
			require.Equal(t, test.expected, v.ranges)
		})
	}
}

func TestCommitsToRawPartitionCommitOffset(t *testing.T) {
	session1 := &PartitionSession{partitionSessionID: 1}
	session2 := &PartitionSession{partitionSessionID: 2}

	table := []struct {
		name     string
		source   []commitRange
		expected []rawtopicreader.PartitionCommitOffset
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []commitRange{
				{Offset: 1, EndOffset: 2, partitionSession: session1},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
					},
				},
			},
		},
		{
			name: "NeighboursWithOneSession",
			source: []commitRange{
				{Offset: 1, EndOffset: 2, partitionSession: session1},
				{Offset: 10, EndOffset: 20, partitionSession: session1},
				{Offset: 30, EndOffset: 40, partitionSession: session1},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
						{Start: 30, End: 40},
					},
				},
			},
		},
		{
			name: "TwoSessionsSameOffsets",
			source: []commitRange{
				{Offset: 1, EndOffset: 2, partitionSession: session1},
				{Offset: 10, EndOffset: 20, partitionSession: session1},
				{Offset: 1, EndOffset: 2, partitionSession: session2},
				{Offset: 10, EndOffset: 20, partitionSession: session2},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
				{
					PartitionSessionID: 2,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
			},
		},
		{
			name: "TwoSessionsWithDifferenceOffsets",
			source: []commitRange{
				{Offset: 1, EndOffset: 2, partitionSession: session1},
				{Offset: 10, EndOffset: 20, partitionSession: session1},
				{Offset: 1, EndOffset: 2, partitionSession: session2},
				{Offset: 3, EndOffset: 4, partitionSession: session2},
				{Offset: 5, EndOffset: 6, partitionSession: session2},
			},
			expected: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: 1,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 10, End: 20},
					},
				},
				{
					PartitionSessionID: 2,
					Offsets: []rawtopicreader.OffsetRange{
						{Start: 1, End: 2},
						{Start: 3, End: 4},
						{Start: 5, End: 6},
					},
				},
			},
		},
	}

	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var v CommitRages
			v.ranges = test.source
			res := v.toRawPartitionCommitOffset()
			require.Equal(t, test.expected, res)
		})
	}
}
