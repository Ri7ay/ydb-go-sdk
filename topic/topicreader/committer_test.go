package topicreader

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

var (
	_ committer = committerDisabled{}
	_ committer = committerAsync{}
	_ committer = &committerSync{}
)

func TestCommitterDisabled(t *testing.T) {
	c := committerDisabled{}
	err := c.Commit(context.Background(), nil)
	require.ErrorIs(t, err, ErrCommitDisabled)
}

func TestCommitterAsync(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		session1 := &PartitionSession{partitionSessionID: 2}
		callback := func(mess rawtopicreader.ClientMessage) error {
			require.Equal(t, &rawtopicreader.CommitOffsetRequest{
				CommitOffsets: []rawtopicreader.PartitionCommitOffset{
					{
						PartitionSessionID: 2,
						Offsets: []rawtopicreader.OffsetRange{
							{
								Start: 3,
								End:   4,
							},
						},
					},
				},
			}, mess)

			return nil
		}
		c := newCommitterAsync(callback)
		require.NoError(t, c.Commit(context.Background(), []CommitOffset{
			{
				Offset:           3,
				ToOffset:         4,
				partitionSession: session1,
			},
		}))
	})

	t.Run("Error", func(t *testing.T) {
		testErr := errors.New("test error")
		c := newCommitterAsync(func(mess rawtopicreader.ClientMessage) error {
			return testErr
		})
		require.ErrorIs(t, c.Commit(context.Background(), nil), testErr)
	})

	t.Run("CancelledContext", func(t *testing.T) {
		c := newCommitterAsync(func(mess rawtopicreader.ClientMessage) error {
			t.Fatalf("must not called")
			return nil
		})

		testErr := errors.New("test error")
		ctx, cancel := xcontext.WithErrCancel(context.Background())
		cancel(testErr)
		err := c.Commit(ctx, nil)
		require.ErrorIs(t, err, testErr)
	})
}

func TestCommitSync(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		c := newCommitterSync(func(mess rawtopicreader.ClientMessage) error {
			t.Fatalf("must not call")
			return nil
		})

		err := c.Commit(context.Background(), CommitBatch{})
		require.NoError(t, err)
	})
	t.Run("StartWithCancelledContext", func(t *testing.T) {
		c := newCommitterSync(func(mess rawtopicreader.ClientMessage) error {
			t.Fatalf("must not call")
			return nil
		})

		testErr := errors.New("test error")
		ctx, cancel := xcontext.WithErrCancel(context.Background())
		cancel(testErr)

		err := c.Commit(ctx, CommitBatch{})
		require.ErrorIs(t, err, testErr)
	})
	t.Run("SuccessCommitWithNotifyAfterCommit", func(t *testing.T) {
		t.Skip("tmp")
		session := &PartitionSession{partitionSessionID: 1}

		commits := CommitBatch{
			{
				Offset:           1,
				ToOffset:         2,
				partitionSession: session,
			},
		}

		allowSendCommitNotify := make(chan bool)
		c := newCommitterSync(func(mess rawtopicreader.ClientMessage) error {
			close(allowSendCommitNotify)
			require.Equal(t,
				&rawtopicreader.CommitOffsetRequest{
					CommitOffsets: commits.toPartitionsOffsets(),
				},
				mess)
			return nil
		})

		go func() {
			<-allowSendCommitNotify
			c.OnCommitNotify(session, rawtopicreader.Offset(2))
		}()

		require.NoError(t, c.Commit(context.Background(), commits))
	})
}
