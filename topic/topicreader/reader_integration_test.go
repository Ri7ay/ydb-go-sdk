//go:build !fast
// +build !fast

package topicreader_test

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"
	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicreader"
)

func TestReaderWithLocalDB(t *testing.T) {
	ctx := context.Background()
	defer pprof.SetGoroutineLabels(ctx)

	pprof.SetGoroutineLabels(pprof.WithLabels(ctx, pprof.Labels("test", "TestReaderWithLocalDB")))

	db, reader := createDBReader(ctx, t)
	defer func() {
		_ = reader.Close()
		_ = db.Close(ctx)
	}()

	mess, err := reader.ReadMessage(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, mess.CreatedAt)

	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}

func createDBReader(ctx context.Context, t *testing.T) (ydb.Connection, *topicreader.Reader) {
	// TODO: Fix connection string to env
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")

	require.NoError(t, err)

	var connector topicreader.TopicSteamReaderConnect = func(ctx context.Context) (topicreader.RawStreamReader, error) {
		grpcConn := db.(grpc.ClientConnInterface)
		pqClient := Ydb_PersQueue_V12.NewPersQueueServiceClient(grpcConn)
		grpcStream, err := pqClient.StreamingRead(context.TODO())
		return rawtopicreader.StreamReader{Stream: grpcStream}, err
	}

	return db, topicreader.NewReader(ctx, connector, "test", []topicreader.ReadSelector{
		{
			Stream: "/local/asd",
		},
	})
}
