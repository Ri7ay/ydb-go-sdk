package trace

import "context"

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// TopicReader specified trace of topic reader client activity.
	// gtrace:gen
	TopicReader struct {
		OnPartitionReadStart       func(OnPartitionReadStartInfo)
		OnPartitionReadStop        func(info OnPartitionReadStopInfo)
		OnPartitionCommittedNotify func(OnPartitionCommittedInfo)
	}

	OnPartitionReadStartInfo struct {
		PartitionContext context.Context
		Topic            string
		PartitionID      int64
		Offset           int64
	}
	OnPartitionReadStopInfo struct {
		PartitionContext context.Context
		Topic            string
		PartitionID      int64
		Graceful         bool
	}

	OnPartitionCommittedInfo struct {
		Topic           string
		PartitionID     int64
		CommittedOffset int64
	}
)
