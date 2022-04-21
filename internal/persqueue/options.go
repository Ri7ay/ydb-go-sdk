package persqueue

import (
	"fmt"

	pqproto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

	"github.com/ydb-platform/ydb-go-sdk/v3/persqueue"
)

type streamOptions struct {
	readRules    []*pqproto.TopicSettings_ReadRule
	remoteMirror *pqproto.TopicSettings_RemoteMirrorRule
}

func (o *streamOptions) AddReadRule(r persqueue.ReadRule) {
	o.readRules = append(o.readRules, encodeReadRule(r))
}

func (o *streamOptions) SetRemoteMirrorRule(r persqueue.RemoteMirrorRule) {
	o.remoteMirror = encodeRemoteMirrorRule(r)
}

func encodeTopicSettings(settings persqueue.StreamSettings, opts ...persqueue.StreamOption) *pqproto.TopicSettings {
	optValues := streamOptions{}
	for _, o := range opts {
		o(&optValues)
	}

	return &pqproto.TopicSettings{
		PartitionsCount:                      int32(settings.PartitionsCount),
		RetentionPeriodMs:                    settings.RetentionPeriod.Milliseconds(),
		MessageGroupSeqnoRetentionPeriodMs:   settings.MessageGroupSeqnoRetentionPeriod.Milliseconds(),
		MaxPartitionMessageGroupsSeqnoStored: int64(settings.MaxPartitionMessageGroupsSeqnoStored),
		SupportedFormat:                      encodeFormat(settings.SupportedFormat),
		SupportedCodecs:                      encodeCodecs(settings.SupportedCodecs),
		MaxPartitionStorageSize:              int64(settings.MaxPartitionStorageSize),
		MaxPartitionWriteSpeed:               int64(settings.MaxPartitionWriteSpeed),
		MaxPartitionWriteBurst:               int64(settings.MaxPartitionWriteBurst),
		ClientWriteDisabled:                  settings.ClientWriteDisabled,
		ReadRules:                            optValues.readRules,
		Attributes:                           settings.Attributes,
		RemoteMirrorRule:                     optValues.remoteMirror,
	}
}

func encodeReadRule(r persqueue.ReadRule) *pqproto.TopicSettings_ReadRule {
	return &pqproto.TopicSettings_ReadRule{
		ConsumerName:               string(r.Consumer),
		Important:                  r.Important,
		StartingMessageTimestampMs: r.StartingMessageTimestamp.UnixMilli(),
		SupportedFormat:            encodeFormat(r.SupportedFormat),
		SupportedCodecs:            encodeCodecs(r.Codecs),
		Version:                    int64(r.Version),
		ServiceType:                r.ServiceType,
	}
}

func encodeRemoteMirrorRule(r persqueue.RemoteMirrorRule) *pqproto.TopicSettings_RemoteMirrorRule {
	return &pqproto.TopicSettings_RemoteMirrorRule{
		Endpoint:                   r.Endpoint,
		TopicPath:                  string(r.SourceStream),
		ConsumerName:               string(r.Consumer),
		Credentials:                encodeRemoteMirrorCredentials(r.Credentials),
		StartingMessageTimestampMs: r.StartingMessageTimestamp.UnixMilli(),
		Database:                   r.Database,
	}
}

func encodeCodecs(v []persqueue.Codec) []pqproto.Codec {
	result := make([]pqproto.Codec, len(v))
	for i := range result {
		switch v[i] {
		case persqueue.CodecUnspecified:
			result[i] = pqproto.Codec_CODEC_UNSPECIFIED
		case persqueue.CodecRaw:
			result[i] = pqproto.Codec_CODEC_RAW
		case persqueue.CodecGzip:
			result[i] = pqproto.Codec_CODEC_GZIP
		case persqueue.CodecLzop:
			result[i] = pqproto.Codec_CODEC_LZOP
		case persqueue.CodecZstd:
			result[i] = pqproto.Codec_CODEC_ZSTD
		default:
			panic(fmt.Sprintf("unknown codec value %v", v))
		}
	}
	return result
}

func encodeFormat(v persqueue.Format) pqproto.TopicSettings_Format {
	switch v {
	case persqueue.FormatUnspecified:
		return pqproto.TopicSettings_FORMAT_UNSPECIFIED
	case persqueue.FormatBase:
		return pqproto.TopicSettings_FORMAT_BASE
	default:
		panic(fmt.Sprintf("unknown format value %v", v))
	}
}

func encodeRemoteMirrorCredentials(v persqueue.RemoteMirrorCredentials) *pqproto.Credentials {
	if v == nil {
		return nil
	}
	switch c := v.(type) {
	case persqueue.IAMCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_Iam_{
				Iam: &pqproto.Credentials_Iam{
					Endpoint:          c.Endpoint,
					ServiceAccountKey: c.ServiceAccountKey,
				},
			},
		}
	case persqueue.JWTCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_JwtParams{
				JwtParams: string(c),
			},
		}
	case persqueue.OAuthTokenCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_OauthToken{
				OauthToken: string(c),
			},
		}
	}
	panic(fmt.Sprintf("unknown credentials type %T", v))
}
