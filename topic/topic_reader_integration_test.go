//go:build !fast
// +build !fast

package topic_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topicstream/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"google.golang.org/grpc"
)

func TestReaderWithLocalDB(t *testing.T) {
	ctx := context.Background()

	// TODO: Fix connection string to env
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")
	defer func() { _ = db.Close(ctx) }()
	require.NoError(t, err)

	var connector topic.TopicSteamReaderConnect = func(ctx context.Context) (topic.ReaderStream, error) {
		grpcConn := db.(grpc.ClientConnInterface)
		pqClient := Ydb_PersQueue_V12.NewPersQueueServiceClient(grpcConn)
		grpcStream, err := pqClient.StreamingRead(context.TODO())
		return pqstreamreader.StreamReader{Stream: grpcStream}, err
	}
	reader := topic.NewReader(ctx, connector, "test", []topic.ReadSelector{
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
