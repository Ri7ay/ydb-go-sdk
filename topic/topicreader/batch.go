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

		if mess.PartitionSession == nil {
			mess.PartitionSession = session
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

func NewBatchFromStream(session *PartitionSession, sb rawtopicreader.Batch) (Batch, error) {
	messages := make([]Message, len(sb.MessageData))

	for i := range sb.MessageData {
		sMess := &sb.MessageData[i]

		cMess := &messages[i]
		cMess.PartitionSession = session
		cMess.Offset = sMess.Offset
		cMess.ToOffset = sMess.Offset + 1

		messData := &cMess.MessageData
		messData.SeqNo = sMess.SeqNo
		messData.CreatedAt = sMess.CreatedAt
		messData.rawDataLen = len(sMess.Data)
		messData.Data = createReader(sb.Codec, sMess.Data)
		messData.WrittenAt = sb.WrittenAt
	}

	return newBatch(session, messages)
}

func (m Batch) Context() context.Context {
	return m.partitionSession.Context()
}

func (m Batch) PartitionSession() *PartitionSession {
	return m.partitionSession
}

func (m Batch) append(b Batch) (Batch, error) {
	if m.partitionSession != b.partitionSession {
		return Batch{}, xerrors.WithStackTrace(errors.New("ydb: bad partition session for merge"))
	}

	if m.ToOffset != b.Offset {
		return Batch{}, xerrors.WithStackTrace(errors.New("ydb: bad offset interval for merge"))
	}

	res := m
	res.Messages = append(res.Messages, b.Messages...)
	res.ToOffset = b.ToOffset
	return res, nil
}

func (m Batch) cutMessages(count int) (head, rest Batch) {
	switch {
	case count == 0:
		return Batch{}, m
	case count >= len(m.Messages):
		return m, Batch{}
	default:
		head, _ = newBatch(m.partitionSession, m.Messages[:count])
		rest, _ = newBatch(m.partitionSession, m.Messages[count:])
		return head, rest
	}
}

func (m Batch) isEmpty() bool {
	return len(m.Messages) == 0
}

func splitBytesByMessagesInBatches(batches []Batch, bytesCount int) error {
	if bytesCount == 0 {
		return nil
	}

	messageSumSizes := 0
	messagesCount := 0
	for batchIndex := range batches {
		messagesCount += len(batches[batchIndex].Messages)
		for messageIndex := range batches[batchIndex].Messages {
			messageSumSizes += batches[batchIndex].Messages[messageIndex].rawDataLen
		}
	}

	if messagesCount == 0 {
		return xerrors.NewWithIssues("ydb: can't split bytes to zero length messages count")
	}

	overheadBytes := bytesCount - messageSumSizes
	overheadPerMessage := overheadBytes / messagesCount
	overheadRemind := overheadBytes % messagesCount

	for batchIndex := range batches {
		for messageIndex := range batches[batchIndex].Messages {
			mess := &batches[batchIndex].Messages[messageIndex]

			mess.bufferBytesAccount = mess.rawDataLen + overheadPerMessage
			if overheadRemind > 0 {
				mess.bufferBytesAccount++
				overheadRemind--
			}
		}
	}

	return nil
}
