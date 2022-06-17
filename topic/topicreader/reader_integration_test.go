//go:build !fast
// +build !fast

package topicreader_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topicstream/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
	"google.golang.org/grpc"
)

func TestReaderWithLocalDB(t *testing.T) {
	ctx := context.Background()

	// TODO: Fix connection string to env
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")
	defer func() { _ = db.Close(ctx) }()
	require.NoError(t, err)

	var connector topicreader.TopicSteamReaderConnect = func(ctx context.Context) (topicreader.ReaderStream, error) {
		grpcConn := db.(grpc.ClientConnInterface)
		pqClient := Ydb_PersQueue_V12.NewPersQueueServiceClient(grpcConn)
		grpcStream, err := pqClient.StreamingRead(context.TODO())
		return pqstreamreader.StreamReader{Stream: grpcStream}, err
	}
	reader := topicreader.NewReader(ctx, connector, "test", []topicreader.ReadSelector{
		{
			Stream: "/local/asd",
		},
	})
	defer func() {
		_ = reader.Close()
	}()

	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}
