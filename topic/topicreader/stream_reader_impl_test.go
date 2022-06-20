package topicreader

import (
	"context"
	"runtime/pprof"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

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
	t.Run("GracefulFalse", func(t *testing.T) {
		e := newTopicReaderTestEnv(t)

		// stop partition
		needStartPartitionMessage := make(chan bool)
		sendStopPartition := make(chan bool)
		e.stream.EXPECT().Recv().Do(func() {
			close(needStartPartitionMessage)
			<-sendStopPartition
		}).Return(&rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: e.partitionSessionID}, nil)

		needNextMessage := make(chan bool)
		e.stream.EXPECT().Recv().Do(func() {
			close(needNextMessage)
			<-e.ctx.Done()
		})

		e.start()

		<-needStartPartitionMessage
		require.NoError(t, e.partitionSession.Context().Err())

		close(sendStopPartition)
		<-needNextMessage
		require.Error(t, e.partitionSession.Context().Err())
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
}

func newTopicReaderTestEnv(t testing.TB) streamEnv {
	cleanCtx := context.Background()
	t.Cleanup(func() {
		pprof.SetGoroutineLabels(cleanCtx)
	})

	ctx := pprof.WithLabels(cleanCtx, pprof.Labels("test", t.Name()))
	pprof.SetGoroutineLabels(ctx)

	mc := gomock.NewController(t)

	stream := NewMockRawStreamReader(mc)
	stream.EXPECT().CloseSend().MinTimes(0).MaxTimes(1)

	reader := newTopicStreamReaderStopped(stream, newTopicStreamReaderConfig())
	// reader.initSession() - skip stream level initialization

	const testPartitionID = 5
	const testSessionID = 15
	const testSessionComitted = 20
	session := newPartitionSession(ctx, "/test", testPartitionID, testSessionID, testSessionComitted)
	require.NoError(t, reader.sessionController.Add(session))

	return streamEnv{
		ctx:                ctx,
		t:                  t,
		reader:             reader,
		stream:             stream,
		partitionSession:   session,
		partitionSessionID: session.partitionSessionID,
		mc:                 mc,
	}
}

func (e *streamEnv) start() {
	require.NoError(e.t, e.reader.startLoops())
}
