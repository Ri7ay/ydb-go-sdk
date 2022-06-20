package topicreader

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

type CommitableByOffset interface { // Интерфейс, который можно коммитить по оффсету
	GetCommitOffset() CommitOffset
}

type CommitBatch []CommitOffset

func CommitBatchFromMessages(messages ...Message) CommitBatch {
	var res CommitBatch
	res.AppendMessages(messages...)
	return res
}

func CommitBatchFromCommitableByOffset(commitable ...CommitableByOffset) CommitBatch {
	var res CommitBatch
	res.Append(commitable...)
	return res
}

func (b *CommitBatch) Append(messages ...CommitableByOffset) {
	for i := range messages {
		*b = append(*b, messages[i].GetCommitOffset())
	}
}

func (b *CommitBatch) AppendMessages(messages ...Message) {
	for i := range messages {
		*b = append(*b, messages[i].GetCommitOffset())
	}
}

func (b CommitBatch) toPartitionsOffsets() []rawtopicreader.PartitionCommitOffset {
	if len(b) == 0 {
		return nil
	}

	commits := make([]CommitOffset, len(b))
	copy(commits, b)

	commits = compressCommitsInplace(commits)
	return commitsToPartitions(commits)
}

func compressCommitsInplace(commits []CommitOffset) []CommitOffset {
	if len(commits) == 0 {
		return commits
	}

	sort.Slice(commits, func(i, j int) bool {
		cI, cJ := &commits[i], &commits[j]
		switch {
		case cI.partitionSessionID < cJ.partitionSessionID:
			return true
		case cJ.partitionSessionID < cI.partitionSessionID:
			return false
		case cI.Offset < cJ.Offset:
			return true
		default:
			return false
		}
	})

	newCommits := commits[:1]
	lastCommit := &newCommits[0]
	for i := range commits[1:] {
		commit := &commits[i]
		if lastCommit.partitionSessionID == commit.partitionSessionID && lastCommit.ToOffset == commit.Offset {
			lastCommit.ToOffset = commit.ToOffset
		} else {
			newCommits = append(newCommits, *commit)
			lastCommit = &newCommits[len(newCommits)-1]
		}
	}
	return newCommits
}

func commitsToPartitions(commits []CommitOffset) []rawtopicreader.PartitionCommitOffset {
	if len(commits) == 0 {
		return nil
	}

	newPartition := func(id rawtopicreader.PartitionSessionID) rawtopicreader.PartitionCommitOffset {
		return rawtopicreader.PartitionCommitOffset{
			PartitionSessionID: id,
		}
	}

	partitionOffsets := make([]rawtopicreader.PartitionCommitOffset, 0, len(commits))
	partitionOffsets = append(partitionOffsets, newPartition(commits[0].partitionSessionID))
	partition := &partitionOffsets[0]

	for i := range commits {
		commit := &commits[i]
		offsetsRange := rawtopicreader.OffsetRange{
			Start: commit.Offset,
			End:   commit.ToOffset,
		}
		if partition.PartitionSessionID != commit.partitionSessionID {
			partitionOffsets = append(partitionOffsets, newPartition(commit.partitionSessionID))
			partition = &partitionOffsets[len(partitionOffsets)-1]
		}
		partition.Offsets = append(partition.Offsets, offsetsRange)
	}
	return partitionOffsets
}
