package topicreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnexpectedCodec          = errors.New("unexpected codec")
	ErrContextExplicitCancelled = errors.New("context explicit cancelled")
)

type SizeReader interface {
	// Важная часть чтобы можно было экономить память на байтиках.
	// За счет этого можно прочитанные сообщения лениво разжимать, а отправляемые при желании лениво формировать
	io.Reader
	Len() int
}

type MessageData struct { // Данные для записи. Так же эмбедятся в чтение
	SeqNo     int64
	CreatedAt time.Time
	WrittenAt time.Time

	Data io.Reader
}

type PartitionSession struct {
	Topic       string
	ID          rawtopicreader.PartitionSessionID
	PartitionID int64

	ctx context.Context

	m              sync.Mutex
	gracefulled    bool
	gracefulSignal chan struct{}
	commitedOffset rawtopicreader.Offset
}

func (s *PartitionSession) graceful(offset rawtopicreader.Offset) error {
	s.m.Lock()
	defer s.m.Unlock()

	if s.gracefulled {
		return xerrors.WithStackTrace(errors.New("partition send graceful signal already"))
	}

	s.gracefulled = true
	s.commitedOffset = offset
	close(s.gracefulSignal)
	return nil
}

func (s *PartitionSession) CommitedOffset() rawtopicreader.Offset {
	s.m.Lock()
	defer s.m.Unlock()

	return s.commitedOffset
}

func (s *PartitionSession) Context() context.Context {
	return context.TODO()
}

type Message struct {
	Topic            string
	PartitionSession *PartitionSession

	MessageData
	CommitOffset

	WrittenAt time.Time

	ctx context.Context // для отслеживания смерти assign
}

var (
	_ CommitableByOffset = Message{}
	_ CommitableByOffset = CommitOffset{}
)

type CommitOffset struct { // Кусочек, необходимый для коммита сообщения
	Offset   rawtopicreader.Offset
	ToOffset rawtopicreader.Offset

	partitionSessionID rawtopicreader.PartitionSessionID
}

func (c CommitOffset) GetCommitOffset() CommitOffset {
	return c
}

func (m Message) Context() context.Context {
	return m.ctx
}

var _ CommitableByOffset = Batch{}

func createReader(codec rawtopic.Codec, rawBytes []byte) io.Reader {
	switch codec {
	case rawtopic.CodecRaw:
		return bytes.NewReader(rawBytes)
	case rawtopic.CodecGzip:
		gzipReader, err := gzip.NewReader(bytes.NewReader(rawBytes))
		if err != nil {
			return errorReader{err: xerrors.WithStackTrace(fmt.Errorf("failed read gzip message: %w", err))}
		}

		gzipReader2, _ := gzip.NewReader(bytes.NewReader(rawBytes))
		content, _ := io.ReadAll(gzipReader2)
		contentS := string(content)
		_ = contentS
		return gzipReader
	default:
		return errorReader{err: xerrors.WithStackTrace(fmt.Errorf("received message with codec '%v': %w", codec, ErrUnexpectedCodec))}
	}
}

type errorReader struct {
	err error
}

func (u errorReader) Read(p []byte) (n int, err error) {
	return 0, u.err
}
