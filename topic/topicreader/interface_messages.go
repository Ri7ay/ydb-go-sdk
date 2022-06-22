package topicreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
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

	Data               io.Reader
	rawDataLen         int
	bufferBytesAccount int
}

type Message struct {
	PartitionSession *PartitionSession

	MessageData
	CommitRange

	WrittenAt time.Time
}

func (m *Message) Context() context.Context {
	return m.PartitionSession.Context()
}

func (m *Message) Topic() string {
	return m.PartitionSession.Topic
}

var (
	_ CommitableByOffset = Message{}
	_ CommitableByOffset = CommitRange{}
)

type CommitRange struct {
	Offset    rawtopicreader.Offset
	EndOffset rawtopicreader.Offset

	partitionSession *PartitionSession
}

func (c CommitRange) GetCommitOffset() CommitRange {
	return c
}

func (c CommitRange) session() *PartitionSession {
	return c.partitionSession
}

var _ CommitableByOffset = &Batch{}

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
