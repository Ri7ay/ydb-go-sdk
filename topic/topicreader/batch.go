package topicreader

import (
	"context"
	"errors"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type Batch struct {
	Messages []Message

	commitRange // от всех сообщений батча
}

func newBatch(session *partitionSession, messages []Message) (Batch, error) {
	for i := 0; i < len(messages); i++ {
		mess := messages[i]

		if i == 0 {
			continue
		}

		prev := messages[i-1]
		if prev.commitOffsetEnd != mess.commitOffsetStart {
			return Batch{}, xerrors.NewWithStackTrace("ydb: bad message offset while messages batch create")
		}

		if mess.session() == nil {
			mess.partitionSession = session
		}
		if session != mess.session() {
			return Batch{}, xerrors.NewWithStackTrace("ydb: bad session while messages batch create")
		}
	}

	offset := commitRange{
		partitionSession: session,
	}
	if len(messages) > 0 {
		offset.commitOffsetStart = messages[0].commitOffsetStart
		offset.commitOffsetEnd = messages[len(messages)-1].commitOffsetEnd
	}

	return Batch{
		Messages:    messages,
		commitRange: offset,
	}, nil
}

func newBatchFromStream(session *partitionSession, sb rawtopicreader.Batch) (Batch, error) {
	messages := make([]Message, len(sb.MessageData))
	prevOffset := session.lastReceivedMessageOffset()
	for i := range sb.MessageData {
		sMess := &sb.MessageData[i]

		dstMess := &messages[i]
		dstMess.CreatedAt = sMess.CreatedAt
		dstMess.MessageGroupID = sMess.MessageGroupID
		dstMess.Offset = sMess.Offset.ToInt64()
		dstMess.SeqNo = sMess.SeqNo
		dstMess.WrittenAt = sb.WrittenAt
		dstMess.WriteSessionMetadata = sb.WriteSessionMeta

		dstMess.rawDataLen = len(sMess.Data)
		dstMess.Data = createReader(sb.Codec, sMess.Data)
		dstMess.UncompressedSize = int(sMess.UncompressedSize)

		dstMess.commitRange.partitionSession = session
		dstMess.commitRange.commitOffsetStart = prevOffset + 1
		dstMess.commitRange.commitOffsetEnd = sMess.Offset + 1

		prevOffset = sMess.Offset
	}

	session.setLastReceivedMessageOffset(prevOffset)

	return newBatch(session, messages)
}

func (m Batch) Context() context.Context {
	return m.partitionSession.Context()
}

func (m Batch) PartitionSession() *partitionSession {
	return m.partitionSession
}

func (m Batch) EndOffset() int64 {
	if len(m.Messages) == 0 {
		return -1
	}
	return m.Messages[len(m.Messages)-1].Offset
}

func (m Batch) append(b Batch) (Batch, error) {
	if m.partitionSession != b.partitionSession {
		return Batch{}, xerrors.WithStackTrace(errors.New("ydb: bad partition session for merge"))
	}

	if m.commitOffsetEnd != b.commitOffsetStart {
		return Batch{}, xerrors.WithStackTrace(errors.New("ydb: bad offset interval for merge"))
	}

	res := m
	res.Messages = append(res.Messages, b.Messages...)
	res.commitOffsetEnd = b.commitOffsetEnd
	return res, nil
}

func (m Batch) cutMessages(count int) (head, rest Batch) {
	switch {
	case count == 0:
		return Batch{}, m
	case count >= len(m.Messages):
		return m, Batch{}
	default:
		// slice[0:count:count] - limit slice capacity and prevent overwrite rest by append messages to head
		// explicit 0 need for prevent typos, when type slice[count:count] instead of slice[:count:count]
		head, _ = newBatch(m.partitionSession, m.Messages[0:count:count])
		rest, _ = newBatch(m.partitionSession, m.Messages[count:])
		return head, rest
	}
}

func (m Batch) isEmpty() bool {
	return len(m.Messages) == 0
}

func splitBytesByMessagesInBatches(batches []Batch, totalBytesCount int) error {
	restBytes := totalBytesCount

	cutBytes := func(want int) int {
		switch {
		case restBytes == 0:
			return 0
		case want >= restBytes:
			res := restBytes
			restBytes = 0
			return res
		default:
			restBytes -= want
			return want
		}
	}

	messagesCount := 0
	for batchIndex := range batches {
		messagesCount += len(batches[batchIndex].Messages)
		for messageIndex := range batches[batchIndex].Messages {
			message := &batches[batchIndex].Messages[messageIndex]
			message.bufferBytesAccount = cutBytes(batches[batchIndex].Messages[messageIndex].rawDataLen)
		}
	}

	if messagesCount == 0 {
		if totalBytesCount == 0 {
			return nil
		}

		return xerrors.NewWithIssues("ydb: can't split bytes to zero length messages count")
	}

	overheadPerMessage := restBytes / messagesCount
	overheadRemind := restBytes % messagesCount

	for batchIndex := range batches {
		for messageIndex := range batches[batchIndex].Messages {
			mess := &batches[batchIndex].Messages[messageIndex]

			mess.bufferBytesAccount += cutBytes(overheadPerMessage)
			if overheadRemind > 0 {
				mess.bufferBytesAccount += cutBytes(1)
			}
		}
	}

	return nil
}
