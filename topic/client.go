package topic

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"
)

type Client interface {
	Close(context.Context) error

	DescribeStream(context.Context, scheme.Path) (StreamInfo, error)
	DropStream(context.Context, scheme.Path) error
	CreateStream(context.Context, scheme.Path, ...StreamOption) error
	AlterStream(context.Context, scheme.Path, ...StreamOption) error
	AddReadRule(context.Context, scheme.Path, ReadRule) error
	RemoveReadRule(context.Context, scheme.Path, Consumer) error

	Writer(ctx context.Context, opts ...writerOption) Writer
	Reader(ctx context.Context, opts ...readerOption) *Reader // TODO: add selector as explicit arg
}

type Consumer string

// Message for read rules description.
type ReadRule struct {
	// For what consumer this read rule is. Must be valid not empty consumer name.
	// Is key for read rules. There could be only one read rule with corresponding consumer name.
	Consumer Consumer
	// All messages with smaller timestamp of write will be skipped.
	StartingMessageTimestamp time.Time
	// Flag that this consumer is important.
	Important bool
	// Max format version that is supported by this consumer.
	// supported_format on topic must not be greater.
	SupportedFormat Format
	// List of supported codecs by this consumer.
	// supported_codecs on topic must be contained inside this list.
	Codecs []Codec

	// Read rule version. Any non-negative integer.
	Version int

	// Client service type. internal
	ServiceType string //
}

// Message for remote mirror rule description.
type RemoteMirrorRule struct {
	// Source cluster endpoint in format server:port.
	Endpoint string
	// Source topic that we want to mirror.
	SourceStream scheme.Path
	// Source consumer for reading source topic.
	Consumer Consumer
	// Credentials for reading source topic by source consumer.
	Credentials RemoteMirrorCredentials
	// All messages with smaller timestamp of write will be skipped.
	StartingMessageTimestamp time.Time
	// Database
	Database string
}

type RemoteMirrorCredentials interface {
	isRemoteMirrorCredentials()
}

func (OAuthTokenCredentials) isRemoteMirrorCredentials() {}
func (JWTCredentials) isRemoteMirrorCredentials()        {}
func (IAMCredentials) isRemoteMirrorCredentials()        {}

type OAuthTokenCredentials string

type JWTCredentials string // TODO: json + JWT token

type IAMCredentials struct {
	Endpoint          string
	ServiceAccountKey string
}

type StreamInfo struct {
	scheme.Entry
	StreamSettings

	// List of consumer read rules for this topic.
	ReadRules []ReadRule
	// remote mirror rule for this topic.
	RemoteMirrorRule RemoteMirrorRule
}

func (ss *StreamSettings) From(y *Ydb_PersQueue_V1.TopicSettings) {
	// TODO: fix

	*ss = StreamSettings{
		PartitionsCount:                      int(y.PartitionsCount),
		MessageGroupSeqnoRetentionPeriod:     time.Duration(y.MessageGroupSeqnoRetentionPeriodMs) * time.Millisecond,
		MaxPartitionMessageGroupsSeqnoStored: int(y.MaxPartitionMessageGroupsSeqnoStored),
		SupportedCodecs:                      decodeCodecs(y.SupportedCodecs),
		MaxPartitionStorageSize:              int(y.MaxPartitionStorageSize),
		MaxPartitionWriteSpeed:               int(y.MaxPartitionWriteSpeed),
		MaxPartitionWriteBurst:               int(y.MaxPartitionWriteBurst),
		SupportedFormat:                      decodeFormat(y.SupportedFormat),
		ClientWriteDisabled:                  y.ClientWriteDisabled,
	}
}

func (rr *ReadRule) From(y *Ydb_PersQueue_V1.TopicSettings_ReadRule) {
	if y == nil {
		*rr = ReadRule{}
		return
	}
	*rr = ReadRule{
		Consumer:                 Consumer(y.ConsumerName),
		StartingMessageTimestamp: time.UnixMilli(y.StartingMessageTimestampMs),
		Important:                y.Important,
		SupportedFormat:          decodeFormat(y.SupportedFormat),
		Codecs:                   decodeCodecs(y.SupportedCodecs),
		Version:                  int(y.Version),
		ServiceType:              y.ServiceType,
	}
}

func (rm *RemoteMirrorRule) From(y *Ydb_PersQueue_V1.TopicSettings_RemoteMirrorRule) {
	if y == nil {
		*rm = RemoteMirrorRule{}
		return
	}
	*rm = RemoteMirrorRule{
		Endpoint:                 y.Endpoint,
		SourceStream:             scheme.Path(y.TopicPath),
		Consumer:                 Consumer(y.ConsumerName),
		Credentials:              decodeCredentials(y.Credentials),
		StartingMessageTimestamp: time.UnixMilli(y.StartingMessageTimestampMs),
		Database:                 y.Database,
	}
}

func decodeCodecs(y []Ydb_PersQueue_V1.Codec) []Codec {
	codecs := make([]Codec, len(y))
	for i := range codecs {
		switch y[i] {
		case Ydb_PersQueue_V1.Codec_CODEC_RAW:
			codecs[i] = CodecRaw
		case Ydb_PersQueue_V1.Codec_CODEC_GZIP:
			codecs[i] = CodecGzip
		case Ydb_PersQueue_V1.Codec_CODEC_LZOP:
			codecs[i] = CodecLzop
		case Ydb_PersQueue_V1.Codec_CODEC_ZSTD:
			codecs[i] = CodecZstd
		default:
			codecs[i] = CodecUnspecified
		}
	}
	return codecs
}

func decodeFormat(y Ydb_PersQueue_V1.TopicSettings_Format) Format {
	switch y {
	case Ydb_PersQueue_V1.TopicSettings_FORMAT_BASE:
		return FormatBase
	default:
		return FormatUnspecified
	}
}

func decodeCredentials(y *Ydb_PersQueue_V1.Credentials) RemoteMirrorCredentials {
	if y == nil || y.Credentials == nil {
		return nil
	}
	switch c := y.Credentials.(type) {
	case *Ydb_PersQueue_V1.Credentials_Iam_:
		return IAMCredentials{
			Endpoint:          c.Iam.Endpoint,
			ServiceAccountKey: c.Iam.ServiceAccountKey,
		}
	case *Ydb_PersQueue_V1.Credentials_JwtParams:
		return JWTCredentials(c.JwtParams)
	case *Ydb_PersQueue_V1.Credentials_OauthToken:
		return OAuthTokenCredentials(c.OauthToken)
	default:
		panic(fmt.Sprintf("unknown credentials type %T", y))
	}
}
