package rawtopic

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

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

func (c *Codec) FromProto(codec Ydb_PersQueue_V1.Codec) {
	*c = Codec(codec)
}

func (c Codec) ToProto() Ydb_PersQueue_V1.Codec {
	return Ydb_PersQueue_V1.Codec(c)
}

type SupportedCodecs []Codec

func (c SupportedCodecs) ToProto() []Ydb_PersQueue_V1.Codec {
	proto := make([]Ydb_PersQueue_V1.Codec, len(c))
	for i := range c {
		proto[i] = c[i].ToProto()
	}
	return proto
}
