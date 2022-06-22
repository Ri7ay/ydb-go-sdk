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
		source   []CommitOffset
		expected []CommitOffset
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         2,
					partitionSession: session1,
				},
			},
			expected: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         2,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressedToOne",
			source: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         2,
					partitionSession: session1,
				},
				{
					Offset:           2,
					ToOffset:         5,
					partitionSession: session1,
				},
				{
					Offset:           5,
					ToOffset:         10,
					partitionSession: session1,
				},
			},
			expected: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         10,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressedUnordered",
			source: []CommitOffset{
				{
					Offset:           5,
					ToOffset:         10,
					partitionSession: session1,
				},
				{
					Offset:           2,
					ToOffset:         5,
					partitionSession: session1,
				},
				{
					Offset:           1,
					ToOffset:         2,
					partitionSession: session1,
				},
			},
			expected: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         10,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressDifferentSessionsSeparated",
			source: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         2,
					partitionSession: session1,
				},
				{
					Offset:           2,
					ToOffset:         3,
					partitionSession: session2,
				},
			},
			expected: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         2,
					partitionSession: session1,
				},
				{
					Offset:           2,
					ToOffset:         3,
					partitionSession: session2,
				},
			},
		},
		{
			name: "CompressTwoSessions",
			source: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         1,
					partitionSession: session1,
				},
				{
					Offset:           2,
					ToOffset:         3,
					partitionSession: session2,
				},
				{
					Offset:           1,
					ToOffset:         3,
					partitionSession: session1,
				},
				{
					Offset:           3,
					ToOffset:         5,
					partitionSession: session2,
				},
			},
			expected: []CommitOffset{
				{
					Offset:           1,
					ToOffset:         3,
					partitionSession: session1,
				},
				{
					Offset:           2,
					ToOffset:         5,
					partitionSession: session2,
				},
			},
		},
	}
	for _, test := range table {
		t.Run(test.name, func(t *testing.T) {
			var src []CommitOffset
			if test.source != nil {
				src = make([]CommitOffset, len(test.source))
				copy(src, test.source)
			}

			res := compressCommits(test.source)
			require.Equal(t, src, test.source) // check about doesn't touch source
			require.Equal(t, test.expected, res)
		})
	}
}

func TestCommitsToRawPartitionCommitOffset(t *testing.T) {
	session1 := &PartitionSession{partitionSessionID: 1}
	session2 := &PartitionSession{partitionSessionID: 2}

	table := []struct {
		name     string
		source   []CommitOffset
		expected []rawtopicreader.PartitionCommitOffset
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []CommitOffset{
				{Offset: 1, ToOffset: 2, partitionSession: session1},
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
			source: []CommitOffset{
				{Offset: 1, ToOffset: 2, partitionSession: session1},
				{Offset: 10, ToOffset: 20, partitionSession: session1},
				{Offset: 30, ToOffset: 40, partitionSession: session1},
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
			source: []CommitOffset{
				{Offset: 1, ToOffset: 2, partitionSession: session1},
				{Offset: 10, ToOffset: 20, partitionSession: session1},
				{Offset: 1, ToOffset: 2, partitionSession: session2},
				{Offset: 10, ToOffset: 20, partitionSession: session2},
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
			source: []CommitOffset{
				{Offset: 1, ToOffset: 2, partitionSession: session1},
				{Offset: 10, ToOffset: 20, partitionSession: session1},
				{Offset: 1, ToOffset: 2, partitionSession: session2},
				{Offset: 3, ToOffset: 4, partitionSession: session2},
				{Offset: 5, ToOffset: 6, partitionSession: session2},
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
			res := commitsToRawPartitionCommitOffset(test.source)
			require.Equal(t, test.expected, res)
		})
	}
}
