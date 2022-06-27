package topicreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

func TestTopicStreamReaderImpl_CommitStoles(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	e.Start()

	lastOffset := e.partitionSession.lastReceivedMessageOffset()
	const dataSize = 4

	// request new data portion
	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: dataSize * 2})

	// Expect commit message with stole
	e.stream.EXPECT().Send(
		&rawtopicreader.CommitOffsetRequest{
			CommitOffsets: []rawtopicreader.PartitionCommitOffset{
				{
					PartitionSessionID: e.partitionSessionID,
					Offsets: []rawtopicreader.OffsetRange{
						{
							Start: lastOffset + 1,
							End:   lastOffset + 16,
						},
					},
				},
			},
		},
	)

	// send message with stole offsets
	//
	e.SendFromServer(&rawtopicreader.ReadResponse{
		BytesSize: dataSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: e.partitionSessionID,
				Batches: []rawtopicreader.Batch{
					{
						Codec:          rawtopic.CodecRaw,
						MessageGroupID: "1",
						MessageData: []rawtopicreader.MessageData{
							{
								Offset: lastOffset + 10,
							},
						},
					},
				},
			},
		},
	})
	e.SendFromServer(&rawtopicreader.ReadResponse{
		BytesSize: dataSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: e.partitionSessionID,
				Batches: []rawtopicreader.Batch{
					{
						Codec:          rawtopic.CodecRaw,
						MessageGroupID: "1",
						MessageData: []rawtopicreader.MessageData{
							{
								Offset: lastOffset + 15,
							},
						},
					},
				},
			},
		},
	})

	opts := newReadMessageBatchOptions()
	opts.MinCount = 2
	batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
	_ = batch
	require.NoError(t, err)
	require.NoError(t, e.reader.Commit(e.ctx, batch.getCommitRange()))
}

func TestTopicStreamReaderImpl_Create(t *testing.T) {
	t.Run("BadSessionInitialization", func(t *testing.T) {
		mc := gomock.NewController(t)
		stream := NewMockRawStreamReader(mc)
		stream.EXPECT().Send(gomock.Any()).Return(nil)
		stream.EXPECT().Recv().Return(&rawtopicreader.StartPartitionSessionRequest{
			ServerMessageMetadata: rawtopicreader.ServerMessageMetadata{Status: rawydb.StatusInternalError},
		}, nil)
		stream.EXPECT().CloseSend().Return(nil)

		reader, err := newTopicStreamReader(stream, newTopicStreamReaderConfig())
		require.Error(t, err)
		require.Nil(t, reader)
	})
}

func TestStreamReaderImpl_OnPartitionCloseHandle(t *testing.T) {
	t.Run("GracefulFalseCancelPartitionContext", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)
		e.Start()

		require.NoError(t, e.partitionSession.Context().Err())

		// stop partition
		e.SendFromServerAndSetNextCallback(
			&rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: e.partitionSessionID},
			func() {
				require.Error(t, e.partitionSession.Context().Err())
			})
		e.WaitProcessed()
	})
	t.Run("TraceGracefulTrue", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)

		readMessagesCtx, readMessagesCtxCancel := xcontext.WithErrCancel(context.Background())
		committedOffset := int64(222)

		e.reader.cfg.Tracer.OnPartitionReadStop = func(info trace.OnPartitionReadStopInfo) {
			expected := trace.OnPartitionReadStopInfo{
				PartitionContext:   e.partitionSession.ctx,
				Topic:              e.partitionSession.Topic,
				PartitionID:        e.partitionSession.PartitionID,
				PartitionSessionID: e.partitionSession.partitionSessionID.ToInt64(),
				CommittedOffset:    committedOffset,
				Graceful:           true,
			}
			require.Equal(t, expected, info)

			require.NoError(t, info.PartitionContext.Err())

			readMessagesCtxCancel(errors.New("test tracer finished"))
		}

		e.Start()

		e.stream.EXPECT().Send(&rawtopicreader.StopPartitionSessionResponse{
			PartitionSessionID: e.partitionSessionID,
		}).Return(nil)

		e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: e.partitionSessionID,
			Graceful:           true,
			CommittedOffset:    rawtopicreader.NewOffset(committedOffset),
		})

		_, err := e.reader.ReadMessageBatch(readMessagesCtx, newReadMessageBatchOptions())
		require.Error(t, err)
		require.Error(t, readMessagesCtx.Err())
	})
	t.Run("TraceGracefulFalse", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)

		readMessagesCtx, readMessagesCtxCancel := xcontext.WithErrCancel(context.Background())
		committedOffset := int64(222)

		e.reader.cfg.Tracer.OnPartitionReadStop = func(info trace.OnPartitionReadStopInfo) {
			expected := trace.OnPartitionReadStopInfo{
				PartitionContext:   e.partitionSession.ctx,
				Topic:              e.partitionSession.Topic,
				PartitionID:        e.partitionSession.PartitionID,
				PartitionSessionID: e.partitionSession.partitionSessionID.ToInt64(),
				CommittedOffset:    committedOffset,
				Graceful:           false,
			}
			require.Equal(t, expected, info)
			require.Error(t, info.PartitionContext.Err())

			readMessagesCtxCancel(errors.New("test tracer finished"))
		}

		e.Start()

		e.SendFromServer(&rawtopicreader.StopPartitionSessionRequest{
			PartitionSessionID: e.partitionSessionID,
			Graceful:           false,
			CommittedOffset:    rawtopicreader.NewOffset(committedOffset),
		})

		_, err := e.reader.ReadMessageBatch(readMessagesCtx, newReadMessageBatchOptions())
		require.Error(t, err)
		require.Error(t, readMessagesCtx.Err())
	})
}

func TestTopicStreamReaderImpl_ReadMessages(t *testing.T) {
	e := newTopicReaderTestEnv(t)
	e.Start()

	compress := func(mess string) []byte {
		b := &bytes.Buffer{}
		writer := gzip.NewWriter(b)
		_, err := writer.Write([]byte(mess))
		require.NoError(t, writer.Close())
		require.NoError(t, err)
		return b.Bytes()
	}

	prevOffset := e.partitionSession.lastReceivedMessageOffset()

	dataSize := 4
	e.stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: dataSize})
	e.SendFromServer(&rawtopicreader.ReadResponse{
		BytesSize: dataSize,
		PartitionData: []rawtopicreader.PartitionData{
			{
				PartitionSessionID: e.partitionSessionID,
				Batches: []rawtopicreader.Batch{
					{
						Codec:            rawtopic.CodecRaw,
						WriteSessionMeta: map[string]string{"a": "b", "c": "d"},
						WrittenAt:        testTime(5),
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           prevOffset + 1,
								SeqNo:            1,
								CreatedAt:        testTime(1),
								Data:             []byte("123"),
								UncompressedSize: 3,
								MessageGroupID:   "1",
							},
							{
								Offset:           prevOffset + 2,
								SeqNo:            2,
								CreatedAt:        testTime(2),
								Data:             []byte("4567"),
								UncompressedSize: 4,
								MessageGroupID:   "1",
							},
						},
					},
					{
						Codec:            rawtopic.CodecGzip,
						WriteSessionMeta: map[string]string{"e": "f", "g": "h"},
						WrittenAt:        testTime(6),
						MessageData: []rawtopicreader.MessageData{
							{
								Offset:           prevOffset + 10,
								SeqNo:            3,
								CreatedAt:        testTime(3),
								Data:             compress("098"),
								UncompressedSize: 3,
								MessageGroupID:   "2",
							},
							{
								Offset:           prevOffset + 20,
								SeqNo:            4,
								CreatedAt:        testTime(4),
								Data:             compress("0987"),
								UncompressedSize: 4,
								MessageGroupID:   "2",
							},
						},
					},
				},
			},
		},
	},
	)

	expectedData := [][]byte{[]byte("123"), []byte("4567"), []byte("098"), []byte("0987")}
	expectedBatch := Batch{
		commitRange: commitRange{
			commitOffsetStart: prevOffset + 1,
			commitOffsetEnd:   prevOffset + 21,
			partitionSession:  e.partitionSession,
		},
		Messages: []Message{
			{
				SeqNo:                1,
				CreatedAt:            testTime(1),
				MessageGroupID:       "1",
				Offset:               prevOffset.ToInt64() + 1,
				WrittenAt:            testTime(5),
				WriteSessionMetadata: map[string]string{"a": "b", "c": "d"},
				rawDataLen:           3,
				commitRange: commitRange{
					commitOffsetStart: prevOffset + 1,
					commitOffsetEnd:   prevOffset + 2,
					partitionSession:  e.partitionSession,
				},
			},
			{
				SeqNo:                2,
				CreatedAt:            testTime(2),
				MessageGroupID:       "1",
				Offset:               prevOffset.ToInt64() + 2,
				WrittenAt:            testTime(5),
				WriteSessionMetadata: map[string]string{"a": "b", "c": "d"},
				rawDataLen:           4,
				commitRange: commitRange{
					commitOffsetStart: prevOffset + 2,
					commitOffsetEnd:   prevOffset + 3,
					partitionSession:  e.partitionSession,
				},
			},
			{
				SeqNo:                3,
				CreatedAt:            testTime(3),
				MessageGroupID:       "2",
				Offset:               prevOffset.ToInt64() + 10,
				WrittenAt:            testTime(6),
				WriteSessionMetadata: map[string]string{"e": "f", "g": "h"},
				rawDataLen:           len(compress("098")),
				commitRange: commitRange{
					commitOffsetStart: prevOffset + 3,
					commitOffsetEnd:   prevOffset + 11,
					partitionSession:  e.partitionSession,
				},
			},
			{
				SeqNo:                4,
				CreatedAt:            testTime(4),
				MessageGroupID:       "2",
				Offset:               prevOffset.ToInt64() + 20,
				WrittenAt:            testTime(6),
				WriteSessionMetadata: map[string]string{"e": "f", "g": "h"},
				rawDataLen:           len(compress("0987")),
				commitRange: commitRange{
					commitOffsetStart: prevOffset + 11,
					commitOffsetEnd:   prevOffset + 21,
					partitionSession:  e.partitionSession,
				},
			},
		},
	}

	opts := newReadMessageBatchOptions()
	opts.MinCount = 4
	batch, err := e.reader.ReadMessageBatch(e.ctx, opts)
	require.NoError(t, err)

	var data [][]byte
	for i := range batch.Messages {
		content, err := ioutil.ReadAll(batch.Messages[i].Data)
		require.NoError(t, err)
		data = append(data, content)
		batch.Messages[i].Data = nil
		batch.Messages[i].bufferBytesAccount = 0
	}

	require.Equal(t, expectedData, data)
	require.Equal(t, expectedBatch, batch)
}

type streamEnv struct {
	ctx                context.Context
	t                  testing.TB
	reader             *topicStreamReaderImpl
	stopReadEvents     emptyChan
	stream             *MockRawStreamReader
	partitionSessionID partitionSessionID
	mc                 *gomock.Controller
	partitionSession   *partitionSession

	m                          xsync.Mutex
	messagesFromServerToClient chan testStreamResult
	nextMessageNeedCallback    func()
}

type testStreamResult struct {
	nextMessageCallback func()
	mess                rawtopicreader.ServerMessage
	err                 error
	waitOnly            bool
}

func newTopicReaderTestEnv(t testing.TB) streamEnv {
	ctx := testContext(t)

	mc := gomock.NewController(t)

	stream := NewMockRawStreamReader(mc)

	cfg := newTopicStreamReaderConfig()
	cfg.BaseContext = ctx

	reader, err := newTopicStreamReaderStopped(stream, cfg)
	require.NoError(t, err)
	// reader.initSession() - skip stream level initialization

	const testPartitionID = 5
	const testSessionID = 15
	const testSessionComitted = 20
	session := newPartitionSession(ctx, "/test", testPartitionID, testSessionID, testSessionComitted)
	require.NoError(t, reader.sessionController.Add(session))

	env := streamEnv{
		ctx:                        ctx,
		t:                          t,
		reader:                     reader,
		stopReadEvents:             make(emptyChan),
		stream:                     stream,
		messagesFromServerToClient: make(chan testStreamResult),
		partitionSession:           session,
		partitionSessionID:         session.partitionSessionID,
		mc:                         mc,
	}

	stream.EXPECT().Recv().AnyTimes().DoAndReturn(env.receiveMessageHandler)
	stream.EXPECT().Send(&rawtopicreader.ReadRequest{BytesSize: 0}).AnyTimes() // allow in test send data without explicit sizes
	stream.EXPECT().CloseSend().Return(nil)

	t.Cleanup(func() {
		cleanupTimeout, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		close(env.stopReadEvents)
		env.reader.Close(ctx, errors.New("test finished"))
		require.NoError(t, cleanupTimeout.Err())
	})

	t.Cleanup(func() {
		if messLen := len(env.messagesFromServerToClient); messLen != 0 {
			t.Fatalf("not all messages consumed from server: %v", messLen)
		}
	})

	return env
}

func (e *streamEnv) Start() {
	require.NoError(e.t, e.reader.startLoops())
}

func (e *streamEnv) readerReceiveWaitClose(callback func()) {
	e.stream.EXPECT().Recv().Do(func() {
		if callback != nil {
			callback()
		}
		<-e.ctx.Done()
	}).Return(nil, errors.New("test reader closed"))
}

func (e *streamEnv) SendFromServer(mess rawtopicreader.ServerMessage) {
	e.SendFromServerAndSetNextCallback(mess, nil)
}

func (e *streamEnv) SendFromServerAndSetNextCallback(mess rawtopicreader.ServerMessage, callback func()) {
	if mess.StatusData().Status == 0 {
		mess.SetStatus(rawydb.StatusSuccess)
	}
	e.messagesFromServerToClient <- testStreamResult{mess: mess, nextMessageCallback: callback}
}

func (e *streamEnv) WaitProcessed() {
	e.messagesFromServerToClient <- testStreamResult{waitOnly: true}
}

func (e *streamEnv) receiveMessageHandler() (rawtopicreader.ServerMessage, error) {
	if e.ctx.Err() != nil {
		return nil, e.ctx.Err()
	}

	var callback func()
	e.m.WithLock(func() {
		callback = e.nextMessageNeedCallback
		e.nextMessageNeedCallback = nil
	})

	if callback != nil {
		callback()
	}

readMessages:
	for {
		select {
		case <-e.ctx.Done():
			return nil, e.ctx.Err()
		case <-e.stopReadEvents:
			return nil, xerrors.NewWithStackTrace("mock reader closed")
		case res := <-e.messagesFromServerToClient:
			if res.waitOnly {
				continue readMessages
			}
			e.m.WithLock(func() {
				e.nextMessageNeedCallback = res.nextMessageCallback
			})
			return res.mess, res.err
		}
	}
}

func testContext(t testing.TB) context.Context {
	ctx, cancel := xcontext.WithErrCancel(context.Background())
	ctx = pprof.WithLabels(ctx, pprof.Labels("test", t.Name()))
	pprof.SetGoroutineLabels(ctx)

	t.Cleanup(func() {
		pprof.SetGoroutineLabels(ctx)
		cancel(fmt.Errorf("test context finished: %v", t.Name()))
	})
	return ctx
}
