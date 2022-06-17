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

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topicstream/pqstreamreader"
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

	Data io.Reader
}

type PartitionSession struct {
	Topic       string
	ID          pqstreamreader.PartitionSessionID
	PartitionID int64

	ctx context.Context

	m              sync.Mutex
	gracefulled    bool
	gracefulSignal chan struct{}
	commitedOffset pqstreamreader.Offset
}

func (s *PartitionSession) graceful(offset pqstreamreader.Offset) error {
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

func (s *PartitionSession) CommitedOffset() pqstreamreader.Offset {
	s.m.Lock()
	defer s.m.Unlock()

	return s.commitedOffset
}

func (s *PartitionSession) Context() context.Context {
	return context.TODO()
}

type Message struct {
	Stream           string
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
	Offset   pqstreamreader.Offset
	ToOffset pqstreamreader.Offset

	partitionSessionID pqstreamreader.PartitionSessionID
}

func (c CommitOffset) GetCommitOffset() CommitOffset {
	return c
}

func (m Message) Context() context.Context {
	return m.ctx
}

type Batch struct {
	Messages       []Message
	WriteTimestamp time.Time

	CommitOffset // от всех сообщений батча

	sizeBytes                        int
	partitionContext                 context.Context // один на все сообщения
	partitionGracefulShutdownChannel <-chan struct{}
}

func NewBatchFromStream(batchContext context.Context, stream string, session *PartitionSession, sb pqstreamreader.Batch) *Batch {
	var res Batch
	res.sizeBytes = sb.SizeBytes
	res.WriteTimestamp = sb.WriteTimeStamp
	res.Messages = make([]Message, len(sb.Messages))
	res.partitionContext = batchContext

	if len(sb.Messages) > 0 {
		commitOffset := &res.CommitOffset
		commitOffset.partitionSessionID = session.ID
		commitOffset.Offset = sb.Messages[0].Offset
		commitOffset.ToOffset = sb.Messages[len(sb.Messages)-1].Offset + 1
	}

	for i := range sb.Messages {
		sMess := &sb.Messages[i]

		cMess := &res.Messages[i]
		cMess.Stream = stream
		cMess.PartitionSession = session
		cMess.ctx = batchContext

		messData := &cMess.MessageData
		messData.SeqNo = sMess.SeqNo
		messData.CreatedAt = sMess.Created
		messData.Data = createReader(sMess.Codec, sMess.Data)
		res.sizeBytes += len(sMess.Data)
	}

	return &res
}

func (m Batch) Context() context.Context {
	return m.partitionContext
}

func (m *Batch) PartitionSession() *PartitionSession {
	panic("not implemented")
}

var _ CommitableByOffset = Batch{}

func createReader(codec pqstreamreader.Codec, rawBytes []byte) io.Reader {
	switch codec {
	case pqstreamreader.CodecRaw:
		return bytes.NewReader(rawBytes)
	case pqstreamreader.CodecGzip:
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
