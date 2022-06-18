package topicreader

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type PartitionSession struct {
	Topic       string
	PartitionID int64

	ctx                context.Context
	ctxCancel          xcontext.CancelErrFunc
	partitionSessionID rawtopicreader.PartitionSessionID
	committedOffset    int64
}

func newPartitionSession(partitionContext context.Context, topic string, partitionID int64, partitionSessionID rawtopicreader.PartitionSessionID, committedOffset int64) *PartitionSession {
	partitionContext, cancel := xcontext.WithErrCancel(partitionContext)

	return &PartitionSession{
		Topic:              topic,
		PartitionID:        partitionID,
		ctx:                partitionContext,
		ctxCancel:          cancel,
		partitionSessionID: partitionSessionID,
		committedOffset:    committedOffset,
	}
}

func (s *PartitionSession) CommitedOffset() int64 {
	return atomic.LoadInt64(&s.committedOffset)
}

func (s *PartitionSession) Context() context.Context {
	return s.ctx
}

func (s *PartitionSession) close(err error) {
	s.ctxCancel(err)
}

func (s *PartitionSession) setCommittedOffset(v int64) {
	atomic.StoreInt64(&s.committedOffset, v)
}

type partitionSessionStorage struct {
	ctx context.Context
	r   *topicStreamReaderImpl

	m        sync.RWMutex
	sessions map[partitionSessionID]*PartitionSession
}

func (c *partitionSessionStorage) init(ctx context.Context, reader *topicStreamReaderImpl) {
	c.ctx = ctx
	c.r = reader
	c.sessions = make(map[partitionSessionID]*PartitionSession)
}

func (c *partitionSessionStorage) requestStatus(id partitionSessionID) error {
	if _, ok := c.sessions[id]; !ok {
		return xerrors.WithStackTrace(fmt.Errorf("unexpected session id: %v", id))
	}

	return c.r.send(&rawtopicreader.PartitionSessionStatusRequest{PartitionSessionID: id})
}

func (c *partitionSessionStorage) onStartPartitionSessionRequest(mess *rawtopicreader.StartPartitionSessionRequest) error {
	// TODO: improve handler
	// TODO: add user handler

	data := newPartitionSession(c.ctx, mess.PartitionSession.Path, mess.PartitionSession.PartitionID, mess.PartitionSession.PartitionSessionID, mess.CommittedOffset.ToInt64())

	if err := c.Add(data); err != nil {
		return err
	}

	return c.r.send(&rawtopicreader.StartPartitionSessionResponse{PartitionSessionID: mess.PartitionSession.PartitionSessionID})
}

func (c *partitionSessionStorage) onStopPartitionSessionRequest(mess *rawtopicreader.StopPartitionSessionRequest) error {
	if mess.Graceful {
		// TODO: implement
		return nil
	}

	if data, err := c.Remove(mess.PartitionSessionID); err == nil {
		data.close(errPartitionStopped)
	} else {
		return err
	}

	return nil
}

func (c *partitionSessionStorage) Add(session *PartitionSession) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, ok := c.sessions[session.partitionSessionID]; ok {
		return xerrors.WithStackTrace(fmt.Errorf("session id already existed: %v", session.partitionSessionID))
	}
	c.sessions[session.partitionSessionID] = session
	return nil
}

func (c *partitionSessionStorage) Get(id partitionSessionID) (*PartitionSession, error) {
	c.m.RLock()
	defer c.m.RUnlock()

	partition := c.sessions[id]
	if partition == nil {
		return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: read undefined partition with id: %v", id))
	}

	return partition, nil
}

func (c *partitionSessionStorage) Remove(id partitionSessionID) (*PartitionSession, error) {
	c.m.Lock()
	defer c.m.Unlock()

	if data, ok := c.sessions[id]; ok {
		delete(c.sessions, id)
		return data, nil
	}

	return nil, xerrors.WithStackTrace(fmt.Errorf("ydb: delete undefined partition session with id: %v", id))
}
