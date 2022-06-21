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

	committedOffset int64
}

func newPartitionSession(partitionContext context.Context, topic string, partitionID int64, partitionSessionID rawtopicreader.PartitionSessionID, committedOffset rawtopicreader.Offset) *PartitionSession {
	partitionContext, cancel := xcontext.WithErrCancel(partitionContext)

	return &PartitionSession{
		Topic:              topic,
		PartitionID:        partitionID,
		ctx:                partitionContext,
		ctxCancel:          cancel,
		partitionSessionID: partitionSessionID,
		committedOffset:    committedOffset.ToInt64(),
	}
}

func (s *PartitionSession) Context() context.Context {
	return s.ctx
}

func (s *PartitionSession) close(err error) {
	s.ctxCancel(err)
}

func (s *PartitionSession) setCommittedOffset(v rawtopicreader.Offset) {
	atomic.StoreInt64(&s.committedOffset, v.ToInt64())
}

type partitionSessionStorage struct {
	m        sync.RWMutex
	sessions map[partitionSessionID]*PartitionSession
}

func (c *partitionSessionStorage) init() {
	c.sessions = make(map[partitionSessionID]*PartitionSession)
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
