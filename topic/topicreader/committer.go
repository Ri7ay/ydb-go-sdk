package topicreader

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"
)

var ErrCommitDisabled = xerrors.Wrap(errors.New("ydb: commits disabled"))

type committer interface {
	Commit(ctx context.Context, offset CommitBatch) error
	OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset)
}

type committerDisabled struct{}

func (c committerDisabled) Commit(ctx context.Context, offset CommitBatch) error {
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

func (c committerAsync) Commit(ctx context.Context, offsets CommitBatch) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return sendCommitMessage(c.send, offsets)
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

func (c *committerSync) Commit(ctx context.Context, commitBatch CommitBatch) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if len(commitBatch) == 0 {
		return nil
	}

	waitConditions := make([]commitWaiterCondition, len(commitBatch))
	for i := range commitBatch {
		waitConditions[i].Session = commitBatch[i].partitionSession
		waitConditions[i].NeedOffset = commitBatch[i].ToOffset
	}

	waiter := newCommitWaiter(waitConditions)
	c.m.WithLock(func() {
		for i := range waiter.Conditions {
			waiter.CommitNotify(waiter.Conditions[i].Session, waiter.Conditions[i].NeedOffset)
		}

		c.waiters = append(c.waiters, waiter)
	})

	if err := sendCommitMessage(c.send, commitBatch); err != nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-waiter.Committed:
		return err
	}
}

func (c *committerSync) OnCommitNotify(session *PartitionSession, offset rawtopicreader.Offset) {
	// TODO implement me
	panic("implement me")
}

func (c *committerSync) examineWaitersNeedLock() {
	newWaiters := c.waiters[:0]
forWaiters:
	for i := range c.waiters {
		waiter := &c.waiters[i]

		if len(waiter.Conditions) == 0 {
			waiter.Committed <- nil
			continue forWaiters
		}

		for condIndex := range waiter.Conditions {
			if err := waiter.Conditions[condIndex].Session.Context().Err(); err != nil {
				waiter.Committed <- err
				continue forWaiters
			}
		}

		newWaiters = append(newWaiters, *waiter)
	}
	c.waiters = newWaiters
}

type commitWaiter struct {
	Conditions []commitWaiterCondition
	Committed  chan error
}

func newCommitWaiter(needs []commitWaiterCondition) commitWaiter {
	return commitWaiter{
		Conditions: needs,
		Committed:  make(chan error, 1),
	}
}

func (w *commitWaiter) CommitNotify(session *PartitionSession, offset rawtopicreader.Offset) {
	newConditions := w.Conditions[:0]
	for srcIndex := range w.Conditions {
		need := &w.Conditions[srcIndex]
		if need.Session == session && offset >= need.NeedOffset {
			// needs completed by notification
			continue
		}
		newConditions = append(newConditions, *need)
	}
	w.Conditions = newConditions
}

type commitWaiterCondition struct {
	Session    *PartitionSession
	NeedOffset rawtopicreader.Offset
}

func sendCommitMessage(send sendMessageToServerFunc, batch CommitBatch) error {
	req := &rawtopicreader.CommitOffsetRequest{
		CommitOffsets: batch.toPartitionsOffsets(),
	}
	return send(req)
}
