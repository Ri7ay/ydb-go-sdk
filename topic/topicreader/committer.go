package topicreader

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var ErrCommitDisabled = xerrors.Wrap(errors.New("ydb: commits disabled"))

type committer interface {
	Commit(ctx context.Context, commitRange CommitRange) error
	OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset)
}

type committerDisabled struct{}

func (c committerDisabled) Commit(ctx context.Context, commitRange CommitRange) error {
	return ErrCommitDisabled
}

func (c committerDisabled) OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset) {
	// skip
}

type sendMessageToServerFunc func(mess rawtopicreader.ClientMessage) error

type committerAsync struct {
	send sendMessageToServerFunc
}

func newCommitterAsync(send sendMessageToServerFunc) committerAsync {
	return committerAsync{send: send}
}

func (c committerAsync) Commit(ctx context.Context, commitRange CommitRange) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// TODO: buffer
	batch := CommitBatch{commitRange.GetCommitOffset()}
	return sendCommitMessage(c.send, batch)
}

func (c committerAsync) OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset) {
	// skip
}

type committerSync struct {
	send sendMessageToServerFunc

	m       xsync.Mutex
	waiters []commitWaiter
}

func newCommitterSync(send sendMessageToServerFunc) *committerSync {
	return &committerSync{send: send}
}

func (c *committerSync) Commit(ctx context.Context, commitRange CommitRange) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	session := commitRange.partitionSession

	waiter := newCommitWaiter(session, commitRange.EndOffset)

	defer c.m.WithLock(func() {
		c.removeWaiterByIdNeedLock(waiter.Id)
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

	batch := CommitBatch{commitRange}
	if err := sendCommitMessage(c.send, batch); err != nil {
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

func (c *committerSync) OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset) {
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

func (c *committerSync) addWaiterNeedLock(waiter commitWaiter) {
	c.waiters = append(c.waiters, waiter)
}

func (c *committerSync) removeWaiterByIdNeedLock(id int64) {
	newWaiters := c.waiters[:0]
	for i := range c.waiters {
		if c.waiters[i].Id == id {
			continue
		}

		newWaiters = append(newWaiters, c.waiters[i])
	}
	c.waiters = newWaiters
}

type commitWaiter struct {
	Id        int64
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
		Id:        id,
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
