package pq_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	Ydb_PersQueue_V12 "github.com/ydb-platform/ydb-go-genproto/Ydb_PersQueue_V1"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/pq"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/pqstreamreader"

	"github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestOneThreadLocalDB(t *testing.T) {
	ctx := context.Background()
	// TODO: Fix connection string to env
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")
	defer func() { _ = db.Close(ctx) }()
	require.NoError(t, err)

	grpcConn := db.(grpc.ClientConnInterface)
	pqClient := Ydb_PersQueue_V12.NewPersQueueServiceClient(grpcConn)
	grpcStream, err := pqClient.StreamingRead(ctx)
	require.NoError(t, err)

	pump, err := pq.TestCreatePump(ctx, pqstreamreader.StreamReader{Stream: grpcStream}, credentials.NewAnonymousCredentials())
	require.NoError(t, err)
	batch, err := pump.ReadMessageBatch(ctx)
	require.NoError(t, err)
	for _, mess := range batch.Messages {
		data, err := io.ReadAll(mess.Data)
		require.NoError(t, err)
		t.Log(string(data))
	}
	require.NoError(t, pump.Commit(ctx, pq.CommitBatch{batch.GetCommitOffset()}))
	time.Sleep(time.Second)
}

func BenchmarkMassCommit(b *testing.B) {
	source := make([]pq.Message, 10000)
	for i := range source {
		source[i].Offset.FromInt64(int64(i))
		source[i].ToOffset.FromInt64(int64(i + 1))
	}

	first := source[0]
	res := source[:1]
	last := &source[0]
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		res = source[:1]
		last = &source[0]
		*last = first

		for i := 1; i < len(source); i++ {
			if last.ToOffset == source[i].Offset {
				last.ToOffset = source[i].ToOffset
			} else {
				res = append(res, source[i])
				last = &source[len(source)-1]
			}
		}
	}

	if len(res) != 1 {
		b.Error(len(res))
	}
	if res[0].ToOffset != source[len(source)-1].ToOffset {
		b.Error()
	}
}
