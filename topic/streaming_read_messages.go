package topic

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type ReadInitRequest struct {
	readSendMark

	ReadSettings []ReadSelector
	Consumer     string

	OnlyOriginal       bool
	MaxLag             time.Duration
	SkipMessagesBefore time.Time

	// MaxBlockFormatVersion int Set internally for data processing
	// MaxMetaCacheSize      int Set internally for data processing

	Session           SessionID
	ConnectionAttempt int
	State             ReadClientState
	IdleTimeout       time.Duration
}

type ReadMoreRequest struct {
	readSendMark
	UncompressedSize uint
}

type CreatePartitionStreamResponse struct {
	readSendMark
	StreamID PartitionStreamID

	ReadOffset   uint
	CommitOffset uint

	VerifyReadOffset bool
}

type DestroyPartitionStreamResponse struct {
	readSendMark
	StreamID PartitionStreamID
}

type ReadStopRequest struct {
	readSendMark
	StreamIDs []PartitionStreamID
}

type ReadResumeRequest struct {
	readSendMark
	Resumes []PartitionResumeInfo
}

type ReadCommitRequest struct {
	readSendMark
	Offsets []PartitionOffsetRange
}

type PartitionStreamStatusRequest struct {
	readSendMark
	StreamID PartitionStreamID
}

type ReadAddStreamRequest struct {
	readSendMark
	ReadSettings ReadSelector
}

type ReadRemoveStreamRequest struct {
	readSendMark
	Stream string
}

// ReadRecvMessage is group of message types which can be received from ReadStream.Recv
type ReadRecvMessage interface {
	isReadStreamMessage()
}

type ReadInitResponse struct {
	readRecvMark
	Session                    SessionID
	StreamsBlockFormatVersions map[string]int

	MaxMetaCacheSize int // Use internally for data processing
}

type CreatePartitionStreamRequest struct {
	readRecvMark
	PartitionStream

	CommitedOffset uint
	EndOffset      uint
}

type DestroyPartitionStreamRequest struct {
	readRecvMark
	StreamID PartitionStreamID

	Graceful       bool
	CommitedOffset uint
}

type ReadCommitResponse struct {
	readRecvMark
	Offsets []PartitionOffset
}

type ReadData struct {
	readRecvMark
	Skip       []PartitionOffsetRange
	Partitions []ReadPartitionData
}

type PartitionStreamStatusResponse struct {
	readRecvMark
	StreamID PartitionStreamID

	CommitedOffset uint
	EndOffset      uint
	WriteWatermark time.Time
}

type ReadStopResponse struct {
	readRecvMark
}

type ReadResumeResponse struct {
	readRecvMark
}

type ReadAddStreamResponse struct {
	readRecvMark
	BlockFormatVersion int
}

type ReadRemoveStreamResponse struct {
	readRecvMark
}

// Common types

type OffsetRange struct {
	StartOffset uint
	EndOffset   uint
}

type PartitionOffsetRange struct {
	StreamID PartitionStreamID
	OffsetRange
}

type PartitionOffset struct {
	StreamID   PartitionStreamID
	ReadOffset int
}

type ReadSelector struct {
	Stream             scheme.Path
	Partitions         []int64
	SkipMessagesBefore time.Time
}

func (s ReadSelector) clone() ReadSelector {
	dst := s

	dst.Partitions = make([]int64, len(s.Partitions))
	copy(dst.Partitions, s.Partitions)

	return dst
}

type ReadPartitionData struct {
	StreamID PartitionStreamID

	ResumeCookie
	ReadDataStatistics

	Messages []ReadMessages
}

type ReadMessages struct {
	Offset int
	SeqNo  int

	MessageGroupID string
	CreatedAt      time.Time
	WrittenAt      time.Time
	IP             string

	ExtraFields map[string]string

	Data EncodeReader
}

type ReadMessageData struct {
	Offset    uint
	SeqNo     uint
	CreatedAt time.Time

	PartitionKey string
	ExplicitHash string
}

type ReadClientState struct {
	PartitionStreamStates []PartitionStreamState
}

type PartitionStreamState struct {
	PartitionStream
	Status PartitionStreamStatus

	ReadOffset   int
	OffsetRanges []OffsetRange
}

type PartitionStream struct {
	StreamID PartitionStreamID

	Stream  string
	Cluster string

	Partition      int
	PartitionGroup int

	// FIXME: connection meta skipped or private
}

type PartitionResumeInfo struct {
	StreamID     PartitionStreamID
	ReadOffset   int
	ResumeCookie ResumeCookie
}

type ReadDataStatistics struct {
	BlobsFromCache int
	BlobsFromDisk  int
	BytesFromHead  int
	BytesFromCache int
	BytesFromDisk  int
	RepackDuration time.Duration
}

type (
	PartitionStreamID int
	ResumeCookie      int
	SessionID         string
)
