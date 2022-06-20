package topicreader

import (
	"errors"
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
		mc := gomock.NewController(t)
		stream := NewMockRawStreamReader(mc)

		// Initialize
		stream.EXPECT().Send(gomock.Any()).Return(nil)
		stream.EXPECT().Recv().Return(&rawtopicreader.InitResponse{
			ServerMessageMetadata: rawtopicreader.ServerMessageMetadata{Status: rawydb.StatusSuccess},
			SessionID:             "test",
		}, nil)

		// Start partition
		stream.EXPECT().Recv().Return(&rawtopicreader.StartPartitionSessionRequest{
			ServerMessageMetadata: rawtopicreader.ServerMessageMetadata{Status: rawydb.StatusSuccess},
			PartitionSession:      rawtopicreader.PartitionSession{PartitionSessionID: 1},
		}, nil)

		stream.EXPECT().Send(gomock.Any()).Return(nil) // start partition ack
		stream.EXPECT().Send(gomock.Any()).Return(nil) // read request

		// stop partition
		needMessage2 := make(chan bool)
		sendStopPartition := make(chan bool)
		stream.EXPECT().Recv().Do(func() {
			close(needMessage2)
			<-sendStopPartition
		}).Return(&rawtopicreader.StopPartitionSessionRequest{PartitionSessionID: 1}, nil)

		// close stream
		needMessage3 := make(chan bool)
		streamClosed := make(chan bool)
		stream.EXPECT().Recv().Do(func() {
			close(needMessage3)
			<-streamClosed
		}).Return(nil, errors.New("test stream closed"))

		stream.EXPECT().CloseSend().Do(func() {
			close(streamClosed)
		})

		reader, err := newTopicStreamReader(stream, newTopicStreamReaderConfig())
		require.NoError(t, err)

		<-needMessage2
		session, err := reader.sessionController.Get(1)
		require.NoError(t, err)
		require.NoError(t, session.Context().Err())

		close(sendStopPartition)
		<-needMessage3

		_, err = reader.sessionController.Get(1)
		require.Error(t, err)
		require.Error(t, session.Context().Err())
	})
}
