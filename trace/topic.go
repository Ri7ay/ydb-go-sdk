package trace

import (
	"context"
	"io"
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
		OnReadStreamUpdateToken    func(OnReadStreamUpdateTokenInfo)
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
		ServerMessage      readStreamServerMessageDebugInfo // may be nil
		Error              error
	}

	readStreamServerMessageDebugInfo interface {
		Type() string
		JsonData() io.Reader
		IsReadStreamServerMessageDebugInfo()
	}

	OnReadStreamRawReceivedInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ServerMessage      readStreamServerMessageDebugInfo
		Error              error
	}

	readStreamClientMessageDebugInfo interface {
		Type() string
		JsonData() io.Reader
		IsReadStreamClientMessageDebugInfo()
	}

	OnReadStreamRawSentInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		ClientMessage      readStreamClientMessageDebugInfo
		Error              error
	}

	OnReadStreamUpdateTokenInfo struct {
		ReaderConnectionID string
		BaseContext        context.Context
		TokenLen           int
		Error              error
	}
)
