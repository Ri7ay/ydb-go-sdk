package topicreader

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Batch struct {
	Messages []Message

	CommitOffset // от всех сообщений батча

	partitionSession *PartitionSession
	partitionContext context.Context // один на все сообщения
}

func newBatch(session *PartitionSession, messages []Message) (Batch, error) {
	for i := 0; i < len(messages); i++ {
		mess := messages[i]

		if i == 0 {
			continue
		}

		prev := messages[i-1]
		if prev.ToOffset != mess.Offset {
			return Batch{}, xerrors.NewWithStackTrace("ydb: bad message offset while messages batch create")
		}

		if session != mess.PartitionSession {
			return Batch{}, xerrors.NewWithStackTrace("ydb: bad session while messages batch create")
		}
	}

	offset := CommitOffset{}
	if len(messages) > 0 {
		offset.Offset = messages[0].Offset
		offset.ToOffset = messages[len(messages)-1].ToOffset
	}

	return Batch{
		partitionSession: session,
		Messages:         messages,
		CommitOffset:     offset,
	}, nil
}

func NewBatchFromStream(batchContext context.Context, stream string, session *PartitionSession, sb rawtopicreader.Batch) *Batch {
	var res Batch
	res.Messages = make([]Message, len(sb.MessageData))
	res.partitionContext = batchContext

	if len(sb.MessageData) > 0 {
		commitOffset := &res.CommitOffset
		commitOffset.partitionSessionID = session.ID
		commitOffset.Offset = sb.MessageData[0].Offset
		commitOffset.ToOffset = sb.MessageData[len(sb.MessageData)-1].Offset + 1
	}

	for i := range sb.MessageData {
		sMess := &sb.MessageData[i]

		cMess := &res.Messages[i]
		cMess.Stream = stream
		cMess.PartitionSession = session
		cMess.ctx = batchContext

		messData := &cMess.MessageData
		messData.SeqNo = sMess.SeqNo
		messData.CreatedAt = sMess.CreatedAt
		messData.Data = createReader(sb.Codec, sMess.Data)
		messData.WrittenAt = sb.WrittenAt
	}

	return &res
}

func (m Batch) Context() context.Context {
	return m.partitionContext
}

func (m *Batch) PartitionSession() *PartitionSession {
	panic("not implemented")
}

func (m *Batch) extendFromBatch(b *Batch) error {
	if m.partitionSession != b.partitionSession {
		return xerrors.WithStackTrace(errors.New("ydb: bad partition session for merge"))
	}

	if m.ToOffset != b.Offset {
		return xerrors.WithStackTrace(errors.New("ydb: bad offset interval for merge"))
	}

	m.Messages = append(m.Messages, b.Messages...)
	m.ToOffset = b.ToOffset
	return nil
}

func (m *Batch) cutMessages(count int) (head, rest Batch) {
	if count >= len(m.Messages) {
		return *m, Batch{}
	}

	head, _ = newBatch(m.partitionSession, m.Messages[:count])
	rest, _ = newBatch(m.partitionSession, m.Messages[count:])
	return head, rest
}

func (m *Batch) isEmpty() bool {
	return len(m.Messages) == 0
}
