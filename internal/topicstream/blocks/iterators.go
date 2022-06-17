package blocks

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type MessageIterator interface {
	NextMessage() (data topic.EncodeReader, end bool)
}

type BlockIteratot interface {
	NextBlock() (block *Block, end bool) // Returned block can not be used after next call
}
