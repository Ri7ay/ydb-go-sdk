package topicwriter

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicwriter"
)

type RawTopicWriterStream interface {
	Recv() (rawtopicwriter.ServerMessage, error)
	Send(mess rawtopicwriter.ClientMessage) error
	CloseSend() error
}

type Writer struct{}

func (w *Writer) Write(message ...Message) error {
	panic("not implemented")
}
