package rawtopic

import "github.com/ydb-platform/ydb-go-genproto/protos/Ydb_PersQueue_V1"

type Codec int

const (
	CodecUNSPECIFIED Codec = iota
	CodecRaw         Codec = 1
	CodecGzip        Codec = 2
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
