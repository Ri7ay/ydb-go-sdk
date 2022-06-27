package topicreader

import (
	"sort"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

type committedBySingleRange interface { // Интерфейс, который можно коммитить по оффсету
	getCommitRange() commitRange
}

type CommitRages struct {
	ranges []commitRange
}

func (r *CommitRages) len() int {
	return len(r.ranges)
}

func NewCommitRangesWithCapacity(capacity int) CommitRages {
	return CommitRages{ranges: make([]commitRange, 0, capacity)}
}

func NewCommitRanges(commitable ...committedBySingleRange) CommitRages {
	var res CommitRages
	res.Append(commitable...)
	return res
}

func (r *CommitRages) Append(ranges ...committedBySingleRange) {
	converted := make([]commitRange, len(ranges))

	for i := range ranges {
		converted[i] = ranges[i].getCommitRange()
		r.ranges = append(r.ranges, converted...)
	}
}

func (r *CommitRages) AppendMessages(messages ...Message) {
	converted := make([]commitRange, len(messages))

	for i := range messages {
		converted[i] = messages[i].getCommitRange()
		r.ranges = append(r.ranges, converted...)
	}
}

func (r *CommitRages) appendCommitRange(cr commitRange) {
	r.ranges = append(r.ranges, cr)
}

func (r *CommitRages) appendCommitRanges(ranges []commitRange) {
	r.ranges = append(r.ranges, ranges...)
}

func (r *CommitRages) Reset() {
	r.ranges = r.ranges[:0]
}

func (r CommitRages) toPartitionsOffsets() []rawtopicreader.PartitionCommitOffset {
	if len(r.ranges) == 0 {
		return nil
	}

	r.optimize()
	return r.toRawPartitionCommitOffset()
}

func (r *CommitRages) optimize() {
	if r.len() == 0 {
		return
	}

	sort.Slice(r.ranges, func(i, j int) bool {
		cI, cJ := &r.ranges[i], &r.ranges[j]
		switch {
		case cI.partitionSession.partitionSessionID < cJ.partitionSession.partitionSessionID:
			return true
		case cJ.partitionSession.partitionSessionID < cI.partitionSession.partitionSessionID:
			return false
		case cI.commitOffsetStart < cJ.commitOffsetStart:
			return true
		default:
			return false
		}
	})

	newCommits := r.ranges[:1]
	lastCommit := &newCommits[0]
	for i := 1; i < len(r.ranges); i++ {
		commit := &r.ranges[i]
		if lastCommit.partitionSession.partitionSessionID == commit.partitionSession.partitionSessionID &&
			lastCommit.commitOffsetEnd == commit.commitOffsetStart {
			lastCommit.commitOffsetEnd = commit.commitOffsetEnd
		} else {
			newCommits = append(newCommits, *commit)
			lastCommit = &newCommits[len(newCommits)-1]
		}
	}

	r.ranges = newCommits
}

func (r *CommitRages) toRawPartitionCommitOffset() []rawtopicreader.PartitionCommitOffset {
	if len(r.ranges) == 0 {
		return nil
	}

	newPartition := func(id rawtopicreader.PartitionSessionID) rawtopicreader.PartitionCommitOffset {
		return rawtopicreader.PartitionCommitOffset{
			PartitionSessionID: id,
		}
	}

	partitionOffsets := make([]rawtopicreader.PartitionCommitOffset, 0, len(r.ranges))
	partitionOffsets = append(partitionOffsets, newPartition(r.ranges[0].partitionSession.partitionSessionID))
	partition := &partitionOffsets[0]

	for i := range r.ranges {
		commit := &r.ranges[i]
		offsetsRange := rawtopicreader.OffsetRange{
			Start: commit.commitOffsetStart,
			End:   commit.commitOffsetEnd,
		}
		if partition.PartitionSessionID != commit.partitionSession.partitionSessionID {
			partitionOffsets = append(partitionOffsets, newPartition(commit.partitionSession.partitionSessionID))
			partition = &partitionOffsets[len(partitionOffsets)-1]
		}
		partition.Offsets = append(partition.Offsets, offsetsRange)
	}
	return partitionOffsets
}

type commitRange struct {
	commitOffsetStart rawtopicreader.Offset
	commitOffsetEnd   rawtopicreader.Offset
	partitionSession  *partitionSession
}

func (c commitRange) getCommitRange() commitRange {
	return c
}

func (c commitRange) session() *partitionSession {
	return c.partitionSession
}
