package rawtopic

import (
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"
)

type Codec int

const (
	CodecUNSPECIFIED Codec = iota
	CodecRaw               = Codec(Ydb_PersQueue_V1.Codec_CODEC_RAW)
	CodecGzip              = Codec(Ydb_PersQueue_V1.Codec_CODEC_GZIP)
)

const (
	CodecCustomerFirst = 10000
	CodecCustomerLast  = 19999
)

func (c Codec) IsCustomerCodec() bool {
	return c >= CodecCustomerFirst && c <= CodecCustomerLast
}

func (c *Codec) MustFromProto(codec Ydb_Topic.Codec) {
	*c = Codec(codec)
}

func (c Codec) ToProto() Ydb_Topic.Codec {
	return Ydb_Topic.Codec(c)
}

type SupportedCodecs []Codec

func (c SupportedCodecs) ToProto() *Ydb_Topic.SupportedCodecs {
	proto := &Ydb_Topic.SupportedCodecs{
		Codecs: make([]int32, len(c)),
	}
	for i := range c {
		proto.Codecs[i] = int32(c[i].ToProto().Number())
	}
	return proto
}

func (c *SupportedCodecs) MustFromProto(proto *Ydb_Topic.SupportedCodecs) {
	res := make([]Codec, len(proto.GetCodecs()))
	for i := range proto.GetCodecs() {
		res[i].MustFromProto(Ydb_Topic.Codec(proto.Codecs[i]))
	}
	*c = res
}
