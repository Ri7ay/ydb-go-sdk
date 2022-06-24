package topicreader

import (
	"context"
)

//nolint:lll
//go:generate mockgen -source batched_stream_reader_interface.go -destination batched_stream_reader_mock_test.go -package topicreader -write_package_comment=false

type batchedStreamReader interface {
	ReadMessageBatch(ctx context.Context, opts readMessageBatchOptions) (Batch, error)
	Commit(ctx context.Context, commitRange commitRange) error
	Close(ctx context.Context, err error)
}
