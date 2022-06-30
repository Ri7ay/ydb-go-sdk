package topic_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/topic"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic/options"

	"github.com/ydb-platform/ydb-go-sdk/v3/scheme"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

func TestClient_CreateDropTopic(t *testing.T) {
	ctx := context.Background()
	db := connect(t)
	topicPath := scheme.Path(db.Name() + "/testtopic")

	_ = db.Topic().DropTopic(ctx, topicPath)
	err := db.Topic().CreateTopic(ctx, topicPath,
		options.CreateTopicWithConsumer(
			options.Consumer{
				Name: "test",
			},
		),
	)
	require.NoError(t, err)

	_, err = db.Topic().(*topic.Client).DescribeTopic(ctx, topicPath)
	require.NoError(t, err)

	err = db.Topic().DropTopic(ctx, topicPath)
	require.NoError(t, err)
}

func connect(t *testing.T) ydb.Connection {
	connectionString := "grpc://localhost:2136/local"
	if cs := os.Getenv("YDB_CONNECTION_STRING"); cs != "" {
		connectionString = cs
	}
	db, err := ydb.Open(context.Background(), connectionString,
		ydb.WithDialTimeout(time.Second),
		ydb.WithAccessTokenCredentials(os.Getenv("YDB_TOKEN")),
	)
	require.NoError(t, err)
	return db
}
