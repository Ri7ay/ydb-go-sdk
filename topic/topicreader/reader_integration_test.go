//go:build !fast
// +build !fast

package topicreader_test

import (
	"context"
	"io"
	"os"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/options"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
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
	t.Logf("mess: %#v", mess)

	content, err := io.ReadAll(mess.Data)
	require.NoError(t, err)
	t.Log("Content:", string(content))

	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}

func createDBReader(ctx context.Context, t *testing.T) (ydb.Connection, *topicreader.Reader) {
	// TODO: Fix connection string to env
	token := os.Getenv("YDB_TOKEN")

	connectionString := "grpc://localhost:2136?database=/local"
	if ecs := os.Getenv("YDB_CONNECTION_STRING"); ecs != "" {
		connectionString = ecs
	}

	db, err := ydb.Open(ctx, connectionString, ydb.WithAccessTokenCredentials(token))
	require.NoError(t, err)
	database := db.Name()
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		_ = s.ExecuteSchemeQuery(ctx, "DROP TABLE test")
		err = s.ExecuteSchemeQuery(ctx, `
CREATE TABLE
	test
(
	id Int64,
	val Utf8,
	PRIMARY KEY (id)
)
	`)
		require.NoError(t, err)

		err = s.ExecuteSchemeQuery(ctx, `
ALTER TABLE
	test
ADD CHANGEFEED
	feed
WITH (
	FORMAT = 'JSON',
	MODE = 'UPDATES'
)
	`)
		require.NoError(t, err)
		return nil
	})
	require.NoError(t, err)

	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		if _, err = tx.Execute(ctx, "INSERT INTO test (id, val) VALUES(1, 'asd')", nil); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)
	err = db.Table().DoTx(ctx, func(ctx context.Context, tx table.TransactionActor) error {
		if _, err = tx.Execute(ctx, "INSERT INTO test (id, val) VALUES(2, 'qwe')", nil); err != nil {
			return err
		}
		return nil
	})
	require.NoError(t, err)

	topicPath := scheme.Path(database + "/test/feed")
	// topicPath := scheme.Path(database + "/asd")

	require.NoError(t, err)

	err = db.Topic().AlterTopic(ctx, topicPath, options.WithAlterTopicAddConsumer(options.Consumer{Name: "test"}))
	require.NoError(t, err)

	tracer := trace.Topic{}

	//tracer.OnReadStreamRawReceived = func(info trace.OnReadStreamRawReceivedInfo) {
	//	t.Logf("time: %v, received: %#v, err: %v", time.Now(), info.ServerMessage, info.Error)
	//}
	//tracer.OnReadStreamRawSent = func(info trace.OnReadStreamRawSentInfo) {
	//	t.Logf("time: %v, sent: %#v, err: %v", time.Now(), info.ClientMessage, info.Error)
	//}

	reader, err := db.Topic().StreamRead("test", []topicreader.ReadSelector{
		{
			Stream: topicPath,
		},
	}, topicreader.WithTracer(tracer))
	require.NoError(t, err)

	return db, reader
}
