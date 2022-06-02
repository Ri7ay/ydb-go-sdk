package pq

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
)

var (
	ErrUnexpectedCodec = errors.New("unexpected codec")
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

type Message struct {
	Stream    string
	Partition int64

	MessageData
	CommitOffset

	Source    string
	WrittenAt time.Time
	IP        string

	ctx context.Context // для отслеживания смерти assign
}

var (
	_ CommitableByOffset = Message{}
	_ CommitableByOffset = CommitOffset{}
)

type CommitOffset struct { // Кусочек, необходимый для коммита сообщения
	Offset   pqstreamreader.Offset
	ToOffset pqstreamreader.Offset
	assignID pqstreamreader.PartitionSessionID
}

func (c CommitOffset) GetCommitOffset() CommitOffset {
	return c
}

func (m Message) Context() context.Context {
	return m.ctx
}

type Batch struct {
	Messages []Message

	CommitOffset // от всех сообщений батча

	size int
	ctx  context.Context // один на все сообщения
}

func NewBatchFromStream(batchContext context.Context, stream string, partitionNum int64, sessionID pqstreamreader.PartitionSessionID, sb pqstreamreader.Batch) *Batch {
	var res Batch
	res.Messages = make([]Message, 0, len(sb.Messages))

	if len(sb.Messages) > 0 {
		commitOffset := &res.CommitOffset
		commitOffset.assignID = sessionID
		commitOffset.Offset = sb.Messages[0].Offset
		commitOffset.ToOffset = sb.Messages[len(sb.Messages)-1].Offset + 1
	}

	for i := range sb.Messages {
		sMess := &sb.Messages[i]

		var cMess Message
		cMess.Stream = stream
		cMess.IP = sb.WriterIP
		cMess.Partition = partitionNum
		cMess.ctx = batchContext

		messData := &cMess.MessageData
		messData.SeqNo = sMess.SeqNo
		messData.CreatedAt = sMess.Created
		messData.Data = createReader(sMess.Codec, sMess.Data)
		res.size += len(sMess.Data)
	}

	return &res
}

func (m Batch) Context() context.Context {
	return m.ctx
}

var (
	_ CommitableByOffset = Batch{}
)

func createReader(codec pqstreamreader.Codec, rawBytes []byte) io.Reader {
	if codec != pqstreamreader.CodecRaw {
		return errorReader{err: ErrUnexpectedCodec}
	}

	return bytes.NewReader(rawBytes)
}

type errorReader struct {
	err error
}

func (u errorReader) Read(p []byte) (n int, err error) {
	return 0, u.err
}
