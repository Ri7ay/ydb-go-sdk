package topicreader

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

type committedBySingleRange interface { // Интерфейс, который можно коммитить по оффсету
	getCommitRange() commitRange
}

type commitBatch []commitRange

func CommitBatchFromMessages(messages ...Message) commitBatch {
	var res commitBatch
	res.AppendMessages(messages...)
	return res
}

func CommitBatchFromCommitableByOffset(commitable ...committedBySingleRange) commitBatch {
	var res commitBatch
	res.Append(commitable...)
	return res
}

func (b *commitBatch) Append(messages ...committedBySingleRange) {
	for i := range messages {
		*b = append(*b, messages[i].getCommitRange())
	}
}

func (b *commitBatch) AppendMessages(messages ...Message) {
	for i := range messages {
		*b = append(*b, messages[i].getCommitRange())
	}
}

func (b *commitBatch) compress() commitBatch {
	return compressCommits(*b)
}

func (b commitBatch) toPartitionsOffsets() []rawtopicreader.PartitionCommitOffset {
	if len(b) == 0 {
		return nil
	}

	commits := compressCommits(b)
	return commitsToRawPartitionCommitOffset(commits)
}

func compressCommits(commitsOrig []commitRange) []commitRange {
	if len(commitsOrig) == 0 {
		return nil
	}

	// prevent broke argument
	sortedCommits := make([]commitRange, len(commitsOrig))
	copy(sortedCommits, commitsOrig)

	sort.Slice(sortedCommits, func(i, j int) bool {
		cI, cJ := &sortedCommits[i], &sortedCommits[j]
		switch {
		case cI.partitionSession.partitionSessionID < cJ.partitionSession.partitionSessionID:
			return true
		case cJ.partitionSession.partitionSessionID < cI.partitionSession.partitionSessionID:
			return false
		case cI.Offset < cJ.Offset:
			return true
		default:
			return false
		}
	})

	newCommits := sortedCommits[:1]
	lastCommit := &newCommits[0]
	for i := 1; i < len(sortedCommits); i++ {
		commit := &sortedCommits[i]
		if lastCommit.partitionSession.partitionSessionID == commit.partitionSession.partitionSessionID &&
			lastCommit.EndOffset == commit.Offset {
			lastCommit.EndOffset = commit.EndOffset
		} else {
			newCommits = append(newCommits, *commit)
			lastCommit = &newCommits[len(newCommits)-1]
		}
	}
	return newCommits
}

func commitsToRawPartitionCommitOffset(commits []commitRange) []rawtopicreader.PartitionCommitOffset {
	if len(commits) == 0 {
		return nil
	}

	newPartition := func(id rawtopicreader.PartitionSessionID) rawtopicreader.PartitionCommitOffset {
		return rawtopicreader.PartitionCommitOffset{
			PartitionSessionID: id,
		}
	}

	partitionOffsets := make([]rawtopicreader.PartitionCommitOffset, 0, len(commits))
	partitionOffsets = append(partitionOffsets, newPartition(commits[0].partitionSession.partitionSessionID))
	partition := &partitionOffsets[0]

	for i := range commits {
		commit := &commits[i]
		offsetsRange := rawtopicreader.OffsetRange{
			Start: commit.Offset,
			End:   commit.EndOffset,
		}
		if partition.PartitionSessionID != commit.partitionSession.partitionSessionID {
			partitionOffsets = append(partitionOffsets, newPartition(commit.partitionSession.partitionSessionID))
			partition = &partitionOffsets[len(partitionOffsets)-1]
		}
		partition.Offsets = append(partition.Offsets, offsetsRange)
	}
	return partitionOffsets
}
