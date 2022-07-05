package topicwriter

import (
	"bytes"
	"io"
	"time"
)

type Message struct {
	SeqNo        int64
	CreatedAt    time.Time
	Data         io.Reader
	Partitioning Partitioning

	dataContent *bytes.Reader // full copy of read data for send retries
}

type Partitioning struct {
	messageGroupID string
	partitionID    int64
	hasPartitionID bool
}

func NewPartitioningWithMessageGroupID(id string) Partitioning {
	return Partitioning{
		messageGroupID: id,
	}
}

func NewPartitioningWithPartitionID(id int64) Partitioning {
	return Partitioning{
		partitionID:    id,
		hasPartitionID: true,
	}
}
