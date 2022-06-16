package topic

import "time"

// WriteSendMessage is group of types sutable for WriteStream.Send
type WriteSendMessage interface {
	isWriteRequest()
}

type WriteInitRequest struct {
	writeSendMark
	Stream            string
	MessageGroupID    string
	PartitionGroup    uint
	ExtraFields       map[string]string
	ConnectionAttempt int

	IdleTimeout time.Duration

	PreferredCluster string

	SessionID string // ???
}

type WriteRequest struct {
	writeSendMark
	Messages []WriteMessage
}

type WriteUpdateTokenRequest struct { // сам токен взять из dialer
	writeSendMark
}

// WriteRecvMessage is group of message types which can be received from WriteStream.Recv
type WriteRecvMessage interface {
	isWriteStreamMessage()
}

type WriteInitResponse struct {
	writeRecvMark
	LastSeqNo int
	SessionID string
	Stream    string
	Cluster   string
	Partition int
	Codecs    []Codec

	BlockFormatVersion int
	BlockSizeLimit     int

	FlushWindowLimit int
}

type WriteResponse struct {
	writeRecvMark

	Results    []WriteResult
	Partition  int
	Statistics WriteStatistics
}

type WriteTokenUpdated struct {
	writeRecvMark
}

// Technical helper types

type writeSendMark struct{}

func (writeSendMark) isWriteRequest() {}

type writeRecvMark struct{}

func (writeRecvMark) isWriteStreamMessage() {}

// Common types

type WriteMessage struct {
	SeqNo     int
	CreatedAt time.Time
	SentAt    time.Time
	Data      EncodeReader
}

type WriteResult struct {
	SeqNo          int
	Offset         uint
	AlreadyWritten bool
}

type WriteStatistics struct {
	Persist              time.Duration
	QueuedInPartition    time.Duration
	ThrottledOnPartition time.Duration
	ThrottledOnStream    time.Duration
}
