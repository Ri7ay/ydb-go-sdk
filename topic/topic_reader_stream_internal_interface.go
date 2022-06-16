package topic

import "context"

//go:generate mockgen -source topic_reader_stream_internal_interface.go -destination topic_reader_stream_internal_interface_mock_test.go -package topic -write_package_comment=false
type topicStreamReader interface {
	ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (*Batch, error)
	Commit(ctx context.Context, offset CommitBatch) error
	Close(ctx context.Context, err error)
}
