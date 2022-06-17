package topic

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiccodec"
)

type StreamSettings struct {
	// How many partitions in topic. Must less than database limit. Default limit - 10.
	PartitionsCount int
	// How long data in partition should be stored. Must be greater than 0 and less than limit for this database.
	// Default limit - 36 hours.
	RetentionPeriod time.Duration
	// How long last written seqno for message group should be stored. Must be greater then retention_period_ms
	// and less then limit for this database.  Default limit - 16 days.
	MessageGroupSeqnoRetentionPeriod time.Duration
	// How many last written seqno for various message groups should be stored per partition. Must be less than limit
	// for this database.  Default limit - 6*10^6 values.
	MaxPartitionMessageGroupsSeqnoStored int
	// List of allowed codecs for stream writes.
	// Writes with codec not from this list are forbidden.
	SupportedCodecs []topiccodec.Codec
	// Max storage usage for each topic's partition. Must be less than database limit. Default limit - 130 GB.
	MaxPartitionStorageSize int
	// Partition write speed in bytes per second. Must be less than database limit. Default limit - 1 MB/s.
	MaxPartitionWriteSpeed int
	// Burst size for write in partition, in bytes. Must be less than database limit. Default limit - 1 MB.
	MaxPartitionWriteBurst int

	// Disallows client writes. Used for mirrored topics in federation.
	ClientWriteDisabled bool
}

// CreateStreamOption stores additional options for stream modification requests
type StreamOption func(streamOptions)

func WithPartitionCount(count int) StreamOption {
	panic("not implemented")
}

func WithSupportedCodecs(codec topiccodec.Codec) StreamOption {
	panic("not implemented")
}

// WithReadRule add read rule setting to stream modification calls
func WithReadRule(r ReadRule) StreamOption {
	return func(o streamOptions) {
		o.AddReadRule(r)
	}
}

// WithRemoteMirrorRule add remote mirror settings to stream modification calls
func WithRemoteMirrorRule(r RemoteMirrorRule) StreamOption {
	return func(o streamOptions) {
		o.SetRemoteMirrorRule(r)
	}
}

type streamOptions interface {
	AddReadRule(r ReadRule)
	SetRemoteMirrorRule(r RemoteMirrorRule)
}

type StreamingWriteOption func()

type steramingWriteOption interface {
	SetCodec(topiccodec.Codec)
	// Block encoding settings
}
