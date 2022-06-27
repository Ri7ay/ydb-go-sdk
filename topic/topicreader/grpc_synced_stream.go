package topicreader

import (
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

var _ RawTopicReaderStream = &syncedStream{}

type syncedStream struct {
	m      sync.Mutex
	stream RawTopicReaderStream
}

func (s *syncedStream) Recv() (rawtopicreader.ServerMessage, error) {
	// not need synced
	return s.stream.Recv()
}

func (s *syncedStream) Send(mess rawtopicreader.ClientMessage) error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.stream.Send(mess)
}

func (s *syncedStream) CloseSend() error {
	s.m.Lock()
	defer s.m.Unlock()

	return s.stream.CloseSend()
}
