package topicreader

import (
	"context"
)

//go:generate mockgen -source stream_reader_interface.go -destination stream_reader_mock_test.go -package topicreader -write_package_comment=false
type streamReader interface {
	ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*Batch, error)
	Commit(ctx context.Context, offset CommitBatch) error
	Close(ctx context.Context, err error)
}
