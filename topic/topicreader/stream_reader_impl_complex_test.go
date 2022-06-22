package topicreader

import (
	"context"
	"errors"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xcontext"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xsync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawydb"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopicreader"
)

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

type streamEnv struct {
	ctx                context.Context
	t                  testing.TB
	reader             *topicStreamReaderImpl
	stream             *MockRawStreamReader
	partitionSessionID partitionSessionID
	mc                 *gomock.Controller
	partitionSession   *PartitionSession

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
	cleanCtx := context.Background()
	t.Cleanup(func() {
		pprof.SetGoroutineLabels(cleanCtx)
	})

	ctx := pprof.WithLabels(cleanCtx, pprof.Labels("test", t.Name()))
	pprof.SetGoroutineLabels(ctx)
	ctx, ctxCancel := xcontext.WithErrCancel(ctx)
	// call cleanup in defer for cancel test context before start other cleanup functions
	defer t.Cleanup(func() {
		ctxCancel(errors.New("test cleanup"))
	})

	mc := gomock.NewController(t)

	stream := NewMockRawStreamReader(mc)

	reader, err := newTopicStreamReaderStopped(stream, newTopicStreamReaderConfig())
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
		stream:                     stream,
		messagesFromServerToClient: make(chan testStreamResult),
		partitionSession:           session,
		partitionSessionID:         session.partitionSessionID,
		mc:                         mc,
	}

	stream.EXPECT().Recv().AnyTimes().DoAndReturn(env.receiveMessageHandler)
	stream.EXPECT().CloseSend().Return(nil)

	t.Cleanup(func() {
		cleanupTimeout, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		env.reader.Close(cleanupTimeout, errors.New("test finished"))
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
