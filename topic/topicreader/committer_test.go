package topicreader

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

func TestCommitterCommit(t *testing.T) {
	t.Run("CommitWithCancelledContext", func(t *testing.T) {
		ctx := testContext(t)
		c := newTestCommitter(ctx, t)
		c.send = func(mess rawtopicreader.ClientMessage) error {
			t.Fatalf("must not call")
			return nil
		}

		testErr := errors.New("test error")
		ctx, cancel := xcontext.WithErrCancel(ctx)
		cancel(testErr)

		err := c.Commit(ctx, commitRange{})
		require.ErrorIs(t, err, testErr)
	})
}

func TestCommitterCommitDisabled(t *testing.T) {
	ctx := testContext(t)
	c := &committer{mode: CommitModeNone}
	err := c.Commit(ctx, commitRange{})
	require.ErrorIs(t, err, ErrCommitDisabled)
}

func TestCommitterCommitAsync(t *testing.T) {
	t.Run("SendCommit", func(t *testing.T) {
		ctx := testContext(t)
		session := &partitionSession{
			ctx:                context.Background(),
			partitionSessionID: 1,
		}

		commitRange := commitRange{
			commitOffsetStart: 1,
			commitOffsetEnd:   2,
			partitionSession:  session,
		}

		sendCalled := make(emptyChan)
		c := newTestCommitter(ctx, t)
		c.mode = CommitModeAsync
		c.send = func(mess rawtopicreader.ClientMessage) error {
			close(sendCalled)
			require.Equal(t,
				&rawtopicreader.CommitOffsetRequest{
					CommitOffsets: NewCommitRanges(commitRange).toPartitionsOffsets(),
				},
				mess)
			return nil
		}
		require.NoError(t, c.Commit(ctx, commitRange))
		<-sendCalled
	})
}

func TestCommitterCommitSync(t *testing.T) {
	t.Run("SendCommit", func(t *testing.T) {
		ctx := testContext(t)
		session := &partitionSession{
			ctx:                context.Background(),
			partitionSessionID: 1,
		}

		commitRange := commitRange{
			commitOffsetStart: 1,
			commitOffsetEnd:   2,
			partitionSession:  session,
		}

		sendCalled := false
		c := newTestCommitter(ctx, t)
		c.mode = CommitModeSync
		c.send = func(mess rawtopicreader.ClientMessage) error {
			sendCalled = true
			require.Equal(t,
				&rawtopicreader.CommitOffsetRequest{
					CommitOffsets: NewCommitRanges(commitRange).toPartitionsOffsets(),
				},
				mess)
			c.OnCommitNotify(session, commitRange.commitOffsetEnd)
			return nil
		}
		require.NoError(t, c.Commit(ctx, commitRange))
		require.True(t, sendCalled)
	})

	t.Run("SuccessCommitWithNotifyAfterCommit", func(t *testing.T) {
		ctx := testContext(t)
		session := &partitionSession{
			ctx:                context.Background(),
			partitionSessionID: 1,
		}

		commitRange := commitRange{
			commitOffsetStart: 1,
			commitOffsetEnd:   2,
			partitionSession:  session,
		}

		commitSended := make(emptyChan)
		c := newTestCommitter(ctx, t)
		c.mode = CommitModeSync
		c.send = func(mess rawtopicreader.ClientMessage) error {
			close(commitSended)
			return nil
		}

		commitCompleted := make(emptyChan)
		go func() {
			require.NoError(t, c.Commit(ctx, commitRange))
			close(commitCompleted)
		}()

		notifySended := false
		go func() {
			<-commitSended
			notifySended = true
			c.OnCommitNotify(session, rawtopicreader.Offset(2))
		}()

		<-commitCompleted
		require.True(t, notifySended)
	})

	t.Run("SuccessCommitPreviousCommitted", func(t *testing.T) {
		ctx := testContext(t)
		session := &partitionSession{
			ctx:                ctx,
			partitionSessionID: 1,
			committedOffsetVal: 2,
		}

		commitRange := commitRange{
			commitOffsetStart: 1,
			commitOffsetEnd:   2,
			partitionSession:  session,
		}

		c := newTestCommitter(ctx, t)
		require.NoError(t, c.Commit(ctx, commitRange))
	})
}

func TestCommitterBuffer(t *testing.T) {
	t.Run("SendZeroLag", func(t *testing.T) {
		ctx := testContext(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(emptyChan)
		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.send = func(mess rawtopicreader.ClientMessage) error {
			close(sendCalled)
			return nil
		}

		require.NoError(t, c.pushCommit(commitRange{partitionSession: &partitionSession{partitionSessionID: 2}}))
		<-sendCalled
	})
	t.Run("TimeLagTrigger", func(t *testing.T) {
		ctx := testContext(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(emptyChan)
		isSended := func() bool {
			select {
			case <-sendCalled:
				return true
			default:
				return false
			}
		}

		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.BufferTimeLagTrigger = time.Second
		c.send = func(mess rawtopicreader.ClientMessage) error {
			commitMess := mess.(*rawtopicreader.CommitOffsetRequest)
			require.Len(t, commitMess.CommitOffsets, 2)
			close(sendCalled)
			return nil
		}

		require.NoError(t, c.pushCommit(commitRange{partitionSession: &partitionSession{partitionSessionID: 1}}))
		require.NoError(t, c.pushCommit(commitRange{partitionSession: &partitionSession{partitionSessionID: 2}}))
		require.False(t, isSended())

		clock.BlockUntil(1)

		clock.Advance(time.Second - 1)
		time.Sleep(time.Millisecond)
		require.False(t, isSended())

		clock.Advance(1)
		<-sendCalled
	})
	t.Run("CountAndTimeFireCountMoreThenNeed", func(t *testing.T) {
		ctx := testContext(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(emptyChan)

		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.BufferTimeLagTrigger = time.Second // for prevent send
		c.BufferCountTrigger = 2
		c.send = func(mess rawtopicreader.ClientMessage) error {
			commitMess := mess.(*rawtopicreader.CommitOffsetRequest)
			require.Len(t, commitMess.CommitOffsets, 4)
			close(sendCalled)
			return nil
		}
		c.commits.appendCommitRanges([]commitRange{
			{partitionSession: &partitionSession{partitionSessionID: 1}},
			{partitionSession: &partitionSession{partitionSessionID: 2}},
			{partitionSession: &partitionSession{partitionSessionID: 3}},
		})

		require.NoError(t, c.pushCommit(commitRange{partitionSession: &partitionSession{partitionSessionID: 4}}))
		<-sendCalled
	})
	t.Run("CountAndTimeFireCountOnAdd", func(t *testing.T) {
		ctx := testContext(t)
		c := newTestCommitter(ctx, t)

		sendCalled := make(emptyChan)
		isSended := func() bool {
			select {
			case <-sendCalled:
				return true
			default:
				return false
			}
		}

		clock := clockwork.NewFakeClock()
		c.clock = clock
		c.BufferTimeLagTrigger = time.Second // for prevent send
		c.BufferCountTrigger = 4
		c.send = func(mess rawtopicreader.ClientMessage) error {
			commitMess := mess.(*rawtopicreader.CommitOffsetRequest)
			require.Len(t, commitMess.CommitOffsets, 4)
			close(sendCalled)
			return nil
		}

		for i := 0; i < 3; i++ {
			require.NoError(t, c.pushCommit(
				commitRange{
					partitionSession: &partitionSession{
						partitionSessionID: rawtopicreader.PartitionSessionID(i),
					},
				},
			))

			// wait notify consumed
			for len(c.commitLoopSignal) > 0 {
				runtime.Gosched()
			}
			require.False(t, isSended())
		}

		require.NoError(t, c.pushCommit(commitRange{partitionSession: &partitionSession{partitionSessionID: 3}}))
		<-sendCalled
	})
	t.Run("CountAndTimeFireTime", func(t *testing.T) {
		ctx := testContext(t)
		clock := clockwork.NewFakeClock()
		c := newTestCommitter(ctx, t)
		c.clock = clock
		c.BufferCountTrigger = 2
		c.BufferTimeLagTrigger = time.Second

		sendCalled := make(emptyChan)
		c.send = func(mess rawtopicreader.ClientMessage) error {
			close(sendCalled)
			return nil
		}
		require.NoError(t, c.pushCommit(commitRange{partitionSession: &partitionSession{}}))

		clock.BlockUntil(1)
		clock.Advance(time.Second)
		<-sendCalled
	})
	t.Run("FlushOnClose", func(t *testing.T) {
		ctx := testContext(t)
		c := newTestCommitter(ctx, t)

		sendCalled := false
		c.send = func(mess rawtopicreader.ClientMessage) error {
			sendCalled = true
			return nil
		}
		c.commits.appendCommitRange(commitRange{partitionSession: &partitionSession{}})
		require.NoError(t, c.Close(ctx, nil))
		require.True(t, sendCalled)
	})
}

func newTestCommitter(ctx context.Context, t *testing.T) *committer {
	res := newCommitter(ctx, CommitModeAsync, func(mess rawtopicreader.ClientMessage) error {
		return nil
	})
	t.Cleanup(func() {
		require.NoError(t, res.Close(ctx, errors.New("test comitter closed")))
	})
	return res
}
