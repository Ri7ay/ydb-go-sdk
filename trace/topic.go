package trace

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

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
		OnReadStreamRawReceived    func(OnReadStreamRawReceivedInfo)
		OnReadStreamRawSent        func(OnReadStreamRawSentInfo)
	}

	OnPartitionReadStartInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		ReadOffset         *int64
		CommitOffset       *int64
	}
	OnPartitionReadStopInfo struct {
		ReaderConnectionID string
		PartitionContext   context.Context
		Topic              string
		PartitionID        int64
		PartitionSessionID int64
		CommittedOffset    int64
		Graceful           bool
	}

	OnPartitionCommittedInfo struct {
		ReaderConnectionID string
		Topic              string
		PartitionID        int64
		CommittedOffset    int64
	}

	OnReadUnknownGrpcMessageInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		Error              error
	}

	OnReadStreamRawReceivedInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ServerMessage      rawtopicreader.ServerMessage
		Error              error
	}

	OnReadStreamRawSentInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ClientMessage      rawtopicreader.ClientMessage
		Error              error
	}
)
