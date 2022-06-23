package topicreader

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var ErrCommitDisabled = xerrors.Wrap(errors.New("ydb: commits disabled"))

type sendMessageToServerFunc func(mess rawtopicreader.ClientMessage) error

type committer struct {
	BufferTimeLagTrigger time.Duration // 0 mean no additional time lag
	BufferCountTrigger   int

	send sendMessageToServerFunc
	mode CommitMode

	clock            clockwork.Clock
	commitLoopSignal chan struct{}
	backgroundWorker backgroundworkers.BackgroundWorker

	m       xsync.Mutex
	waiters []commitWaiter
	commits []CommitRange
}

func newCommitter(lifeContext context.Context, mode CommitMode, send sendMessageToServerFunc) *committer {
	res := &committer{
		mode:             mode,
		clock:            clockwork.NewRealClock(),
		send:             send,
		backgroundWorker: *backgroundworkers.New(lifeContext),
	}
	res.initChannels()
	res.start()
	return res
}

func (c *committer) initChannels() {
	c.commitLoopSignal = make(chan struct{}, 1)
}

func (c *committer) start() {
	c.backgroundWorker.Start("commit pusher", c.pushCommitsLoop)
}

func (c *committer) Close(ctx context.Context, err error) error {
	return c.backgroundWorker.Close(ctx, err)
}

func (c *committer) Commit(ctx context.Context, commitRange CommitRange) error {
	if !c.mode.commitsEnabled() {
		return ErrCommitDisabled
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err := c.pushCommit(commitRange); err != nil {
		return err
	}

	if c.mode == CommitModeSync {
		return c.waitCommitAck(ctx, commitRange)
	}

	return nil
}

func (c *committer) pushCommit(commitRange CommitRange) error {
	var res error
	c.m.WithLock(func() {
		if err := c.backgroundWorker.Context().Err(); err != nil {
			res = err
			return
		}

		c.commits = append(c.commits, commitRange)
	})

	select {
	case c.commitLoopSignal <- struct{}{}:
	default:
	}

	return res
}

func (c *committer) pushCommitsLoop(ctx context.Context) {
	for {
		c.waitSendTrigger(ctx)

		var commits CommitBatch
		c.m.WithLock(func() {
			commits = c.commits
			c.commits = make([]CommitRange, 0, len(commits))
		})

		if len(commits) == 0 && c.backgroundWorker.Context().Err() != nil {
			// committer closed with empty buffer - target close state
			return
		}

		commits = commits.compress()
		if err := sendCommitMessage(c.send, commits); err != nil {
			_ = c.backgroundWorker.Close(ctx, err)
		}
	}
}

func (c *committer) waitSendTrigger(ctx context.Context) {
	ctxDone := ctx.Done()
	select {
	case <-ctxDone:
		return
	case <-c.commitLoopSignal:
	}

	if c.BufferTimeLagTrigger == 0 {
		return
	}

	finish := c.clock.After(c.BufferTimeLagTrigger)
	if c.BufferCountTrigger == 0 {
		select {
		case <-ctxDone:
		case <-finish:
		}
		return
	}

	for {
		var commitsLen int
		c.m.WithLock(func() {
			commitsLen = len(c.commits)
		})
		if commitsLen >= c.BufferCountTrigger {
			return
		}

		select {
		case <-ctxDone:
			return
		case <-finish:
			return
		case <-c.commitLoopSignal:
			// check count on next loop iteration
		}
	}
}

func (c *committer) waitCommitAck(ctx context.Context, commitRange CommitRange) error {
	session := commitRange.partitionSession

	waiter := newCommitWaiter(session, commitRange.EndOffset)

	defer c.m.WithLock(func() {
		c.removeWaiterByIDNeedLock(waiter.ID)
	})

	var fastOk bool
	c.m.WithLock(func() {
		// need check atomically with add to waiters for prevent race conditions
		fastOk = waiter.checkCondition(session, session.committedOffset())

		if !fastOk {
			c.addWaiterNeedLock(waiter)
		}
	})
	if fastOk {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-session.Context().Done():
		return session.Context().Err()
	case <-waiter.Committed:
		return nil
	}
}

func (c *committer) OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset) {
	c.m.WithLock(func() {
		for i := range c.waiters {
			waiter := c.waiters[i]
			if waiter.checkCondition(session, offset) {
				select {
				case waiter.Committed <- struct{}{}:
				default:
				}
			}
		}
	})
}

func (c *committer) addWaiterNeedLock(waiter commitWaiter) {
	c.waiters = append(c.waiters, waiter)
}

func (c *committer) removeWaiterByIDNeedLock(id int64) {
	newWaiters := c.waiters[:0]
	for i := range c.waiters {
		if c.waiters[i].ID == id {
			continue
		}

		newWaiters = append(newWaiters, c.waiters[i])
	}
	c.waiters = newWaiters
}

type commitWaiter struct {
	ID        int64
	Session   *PartitionSession
	EndOffset rawtopicreader.Offset
	Committed chan struct{}
}

func (w *commitWaiter) checkCondition(session *PartitionSession, offset rawtopicreader.Offset) (finished bool) {
	return session == w.Session && offset >= w.EndOffset
}

var commitWaiterLastID int64

func newCommitWaiter(session *PartitionSession, endOffset rawtopicreader.Offset) commitWaiter {
	id := atomic.AddInt64(&commitWaiterLastID, 1)
	return commitWaiter{
		ID:        id,
		Session:   session,
		EndOffset: endOffset,
		Committed: make(chan struct{}, 1),
	}
}

func sendCommitMessage(send sendMessageToServerFunc, batch CommitBatch) error {
	req := &rawtopicreader.CommitOffsetRequest{
		CommitOffsets: batch.toPartitionsOffsets(),
	}
	return send(req)
}
