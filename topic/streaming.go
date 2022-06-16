package topic

import (
	"context"
	"io"
)

type ReadClient interface {
	ReadStream(context.Context) ReadStream
}

type WriteClient interface {
	WriteStream(context.Context) WriteStream
}

type WriteStream interface {
	Send(WriteSendMessage) error
	Recv() (WriteRecvMessage, error)
	CloseSend() error
	Close() error
}

// Common types for streaming

type EncodeReader interface {
	io.Reader
	Len() int
}

// Technical helper types

type readSendMark struct{}

func (readSendMark) isReadRequest() {}

type readRecvMark struct{}

func (readRecvMark) isReadStreamMessage() {}
