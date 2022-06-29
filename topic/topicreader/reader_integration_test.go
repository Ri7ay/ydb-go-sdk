//go:build !fast
// +build !fast

package topicreader_test

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/stretchr/testify/require"

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

	batch, err := reader.ReadMessageBatch(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, batch.Messages)
}

func createDBReader(ctx context.Context, t *testing.T) (ydb.Connection, *topicreader.Reader) {
	// TODO: Fix connection string to env
	db, err := ydb.Open(ctx, "grpc://localhost:2136?database=/local")
	err = db.Table().Do(ctx, func(ctx context.Context, s table.Session) error {
		return nil

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
		if err != nil {
			return err
		}

		return s.ExecuteSchemeQuery(ctx, `
ALTER TABLE
	test
ADD CHANGEFEED
	feed
WITH (
	FORMAT = 'JSON',
	MODE = 'UPDATES'
)
	`)
	})
	require.NoError(t, err)

	reader, err := db.Topic().StartRead("test", []topicreader.ReadSelector{
		{
			// Stream: "/local/test/feed",
			Stream: "/local/asd",
		},
	})
	require.NoError(t, err)

	return db, reader
}
