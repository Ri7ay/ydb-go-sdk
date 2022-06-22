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
		source   []CommitRange
		expected []CommitRange
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []CommitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
			},
			expected: []CommitRange{
				{
					Offset:           1,
					EndOffset:        2,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressedToOne",
			source: []CommitRange{
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
			expected: []CommitRange{
				{
					Offset:           1,
					EndOffset:        10,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressedUnordered",
			source: []CommitRange{
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
			expected: []CommitRange{
				{
					Offset:           1,
					EndOffset:        10,
					partitionSession: session1,
				},
			},
		},
		{
			name: "CompressDifferentSessionsSeparated",
			source: []CommitRange{
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
			expected: []CommitRange{
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
			source: []CommitRange{
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
			expected: []CommitRange{
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
			var src []CommitRange
			if test.source != nil {
				src = make([]CommitRange, len(test.source))
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
		source   []CommitRange
		expected []rawtopicreader.PartitionCommitOffset
	}{
		{
			name:     "Empty",
			source:   nil,
			expected: nil,
		},
		{
			name: "OneCommit",
			source: []CommitRange{
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
			source: []CommitRange{
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
			source: []CommitRange{
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
			source: []CommitRange{
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
			res := commitsToRawPartitionCommitOffset(test.source)
			require.Equal(t, test.expected, res)
		})
	}
}
