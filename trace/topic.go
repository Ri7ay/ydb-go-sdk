package trace

import "context"

// tool gtrace used from ./internal/cmd/gtrace

//go:generate gtrace

type (
	// Topic specified trace of topic reader client activity.
	// gtrace:gen
	Topic struct {
		OnPartitionReadStart       func(OnPartitionReadStartInfo)
		OnPartitionReadStop        func(info OnPartitionReadStopInfo)
		OnPartitionCommittedNotify func(OnPartitionCommittedInfo)
		OnReadUnknownGrpcMessage   func(OnReadUnknownGrpcMessageInfo)
	}

	OnPartitionReadStartInfo struct {
		PartitionContext context.Context
		Topic            string
		PartitionID      int64
		ReadOffset       *int64
		CommitOffset     *int64
	}
	OnPartitionReadStopInfo struct {
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	OnPartitionCommittedInfo struct {
		Topic           string
		PartitionID     int64
		CommittedOffset int64
	}

	OnReadUnknownGrpcMessageInfo struct {
		BaseContext context.Context
		Error       error
	}
)
