package topicstream

import (
	"fmt"

	pqproto "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topiccodec"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type streamOptions struct {
	readRules    []*pqproto.TopicSettings_ReadRule
	remoteMirror *pqproto.TopicSettings_RemoteMirrorRule
}

func (o *streamOptions) AddReadRule(r topic.ReadRule) {
	o.readRules = append(o.readRules, encodeReadRule(r))
}

func (o *streamOptions) SetRemoteMirrorRule(r topic.RemoteMirrorRule) {
	o.remoteMirror = encodeRemoteMirrorRule(r)
}

func encodeTopicSettings(settings topic.StreamSettings, opts ...topic.StreamOption) *pqproto.TopicSettings {
	optValues := streamOptions{}
	for _, o := range opts {
		o(&optValues)
	}

	// TODO: fix
	return &pqproto.TopicSettings{
		PartitionsCount: int32(settings.PartitionsCount),
		// RetentionPeriodMs:                    settings.RetentionPeriod.Milliseconds(),
		MessageGroupSeqnoRetentionPeriodMs:   settings.MessageGroupSeqnoRetentionPeriod.Milliseconds(),
		MaxPartitionMessageGroupsSeqnoStored: int64(settings.MaxPartitionMessageGroupsSeqnoStored),
		SupportedCodecs:                      encodeCodecs(settings.SupportedCodecs),
		MaxPartitionStorageSize:              int64(settings.MaxPartitionStorageSize),
		MaxPartitionWriteSpeed:               int64(settings.MaxPartitionWriteSpeed),
		MaxPartitionWriteBurst:               int64(settings.MaxPartitionWriteBurst),
		ClientWriteDisabled:                  settings.ClientWriteDisabled,
		ReadRules:                            optValues.readRules,
		RemoteMirrorRule:                     optValues.remoteMirror,
	}
}

func encodeReadRule(r topic.ReadRule) *pqproto.TopicSettings_ReadRule {
	return &pqproto.TopicSettings_ReadRule{
		ConsumerName:               string(r.Consumer),
		Important:                  r.Important,
		StartingMessageTimestampMs: r.StartingMessageTimestamp.UnixMilli(),
		SupportedCodecs:            encodeCodecs(r.Codecs),
		Version:                    int64(r.Version),
		ServiceType:                r.ServiceType,
	}
}

func encodeRemoteMirrorRule(r topic.RemoteMirrorRule) *pqproto.TopicSettings_RemoteMirrorRule {
	return &pqproto.TopicSettings_RemoteMirrorRule{
		Endpoint:                   r.Endpoint,
		TopicPath:                  string(r.SourceStream),
		ConsumerName:               string(r.Consumer),
		Credentials:                encodeRemoteMirrorCredentials(r.Credentials),
		StartingMessageTimestampMs: r.StartingMessageTimestamp.UnixMilli(),
		Database:                   r.Database,
	}
}

func encodeCodecs(v []topiccodec.Codec) []pqproto.Codec {
	result := make([]pqproto.Codec, len(v))
	for i := range result {
		switch v[i] {
		case topiccodec.CodecUnspecified:
			result[i] = pqproto.Codec_CODEC_UNSPECIFIED
		case topiccodec.CodecRaw:
			result[i] = pqproto.Codec_CODEC_RAW
		case topiccodec.CodecGzip:
			result[i] = pqproto.Codec_CODEC_GZIP
		case topiccodec.CodecLzop:
			result[i] = pqproto.Codec_CODEC_LZOP
		case topiccodec.CodecZstd:
			result[i] = pqproto.Codec_CODEC_ZSTD
		default:
			panic(fmt.Sprintf("unknown codec value %v", v))
		}
	}
	return result
}

func encodeRemoteMirrorCredentials(v topic.RemoteMirrorCredentials) *pqproto.Credentials {
	if v == nil {
		return nil
	}
	switch c := v.(type) {
	case topic.IAMCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_Iam_{
				Iam: &pqproto.Credentials_Iam{
					Endpoint:          c.Endpoint,
					ServiceAccountKey: c.ServiceAccountKey,
				},
			},
		}
	case topic.JWTCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_JwtParams{
				JwtParams: string(c),
			},
		}
	case topic.OAuthTokenCredentials:
		return &pqproto.Credentials{
			Credentials: &pqproto.Credentials_OauthToken{
				OauthToken: string(c),
			},
		}
	}
	panic(fmt.Sprintf("unknown credentials type %T", v))
}
