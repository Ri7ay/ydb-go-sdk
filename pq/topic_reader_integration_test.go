//go:build !fast
// +build !fast

package pq_test

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/pq"
	"google.golang.org/grpc"
)

func TestReaderWithLocalDB(t *testing.T) {
	ctx := context.Background()

	// TODO: Fix connection string to env
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")
	defer func() { _ = db.Close(ctx) }()
	require.NoError(t, err)

	var connector pq.TopicSteamReaderConnect = func(ctx context.Context) (pq.ReaderStream, error) {
		grpcConn := db.(grpc.ClientConnInterface)
		pqClient := Ydb_PersQueue_V12.NewPersQueueServiceClient(grpcConn)
		grpcStream, err := pqClient.StreamingRead(context.TODO())
		return pqstreamreader.StreamReader{Stream: grpcStream}, err
	}
	reader := pq.NewReader(ctx, connector, "test", []pq.ReadSelector{
		{
			Stream: "/local/asd",
		},
	})

	for {
		batch, err := reader.ReadMessageBatch(ctx)
		if err != nil {
			// TODO
			panic(err)
		}
		for _, mess := range batch.Messages {
			data, err := io.ReadAll(mess.Data)
			if err != nil {
				panic(err)
			}
			fmt.Println(string(data))
		}
	}
}
