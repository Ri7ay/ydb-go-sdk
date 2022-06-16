package topic

import (
	"context"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backoff"
)

type Writer struct {
	stream *WriteStream

	sync  bool
	codec Codec

	autoGenerateSeqNo bool
}

type writerOption func()

func WithAutoSeq(enabled bool) writerOption {
	panic("not implemented")
}

func WithAsyncEnabled(enabled bool) writerOption {
	panic("not implemented")
}

func WithAsyncFlushSize(size int) writerOption {
	panic("not implemented")
}

func WithAsyncFlushInterval(interval time.Duration) writerOption {
	panic("not implemented")
}

func WithCodec(codec Codec) writerOption {
	panic("not implemented")
}

func WithMessageGroupID(groupID string) writerOption {
	panic("not implemented")
}

func WithRetrier(retrier backoff.Backoff) writerOption {
	panic("not implemented")
}

func WithSessionMetadata(meta map[string]string) writerOption {
	panic("not implemented")
}

func WithInflightMemoryLimit(size int) writerOption {
	panic("not implemented")
}

func WithInflightCountLimit(count int) writerOption {
	panic("not implemented")
}

func WithSeqnoValidation(enabled bool) writerOption {
	panic("not implemented")
}

func WithPartitionNum(num uint32) writerOption {
	panic("not implemented")
}

func NewWriter(stream, groupid string, opts ...writerOption) (*Writer, error) {
	return nil, nil
}

func (w *Writer) Close() error {
	// check alive
	// close send
	// wait acks with timeout
	return nil
}

func (w *Writer) Stats() WriterStats {
	return WriterStats{}
}

type WriterStats struct {
	Stream         string
	ClusterName    string
	Partition      int
	CommitedOffset int // rekby:check

	// buffers info
	// все остальное полезное
}

func (w *Writer) WriteMessages(ctx context.Context, msgs ...MessageData) error {
	// формируем и отправляем сообщения.
	// тут если не указано, то можно проставлять seqno сообщений и created at
	if w.sync {
		return w.waitAcks(ctx)
	}
	return nil
}

func (w *Writer) start(context.Context) error {
	// шлем init и ждем когда получим ответ
	// запускаем чтение из стрима не забывая параллельно чекать отмену контекста

	// Можно делать лениво или явно в конструкторе
	return nil
}

func (w *Writer) waitAcks(context.Context) error {
	// тут дожидаемся чтобы все сообщения в буфере были акнуты сервером

	return nil
}
