package topicreader

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var _ batchedStreamReader = &readerReconnector{} // check interface implementation

func TestTopicReaderReconnectorReadMessageBatch(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		baseReader := NewMockbatchedStreamReader(mc)

		opts := readMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 10}}
		batch := Batch{
			Messages: []Message{{WrittenAt: time.Date(2022, 0o6, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		reader := &readerReconnector{
			streamVal: baseReader,
		}
		reader.initChannels()
		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	t.Run("WithConnect", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		baseReader := NewMockbatchedStreamReader(mc)
		opts := readMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 10}}
		batch := Batch{
			Messages: []Message{{WrittenAt: time.Date(2022, 0o6, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		connectCalled := 0
		reader := &readerReconnector{
			readerConnect: func(ctx context.Context) (batchedStreamReader, error) {
				connectCalled++
				if connectCalled > 1 {
					return nil, errors.New("unexpected call test connect function")
				}
				return baseReader, nil
			},
			streamErr: errUnconnected,
		}
		reader.initChannels()
		reader.background.Start("test-reconnectionLoop", reader.reconnectionLoop)

		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	t.Run("WithReConnect", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		opts := readMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 10}}

		baseReader1 := NewMockbatchedStreamReader(mc)
		baseReader1.EXPECT().ReadMessageBatch(gomock.Any(), opts).MinTimes(1).Return(Batch{}, xerrors.Retryable(errors.New("test1")))
		baseReader1.EXPECT().Close(gomock.Any(), gomock.Any()).Return()

		baseReader2 := NewMockbatchedStreamReader(mc)
		baseReader2.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(Batch{}, xerrors.Retryable(errors.New("test2")))
		baseReader2.EXPECT().Close(gomock.Any(), gomock.Any()).Return()

		baseReader3 := NewMockbatchedStreamReader(mc)
		batch := Batch{
			Messages: []Message{{WrittenAt: time.Date(2022, 0o6, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader3.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		readers := []batchedStreamReader{
			baseReader1, baseReader2, baseReader3,
		}
		connectCalled := 0
		reader := &readerReconnector{
			readerConnect: func(ctx context.Context) (batchedStreamReader, error) {
				connectCalled++
				return readers[connectCalled-1], nil
			},
			streamErr: errUnconnected,
		}
		reader.initChannels()
		reader.background.Start("test-reconnectionLoop", reader.reconnectionLoop)

		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	t.Run("StartWithCancelledContext", func(t *testing.T) {
		cancelledCtx, cancelledCtxCancel := context.WithCancel(context.Background())
		cancelledCtxCancel()

		for i := 0; i < 100; i++ {
			reconnector := &readerReconnector{}
			reconnector.initChannels()

			_, err := reconnector.ReadMessageBatch(cancelledCtx, readMessageBatchOptions{})
			require.ErrorIs(t, err, context.Canceled)
		}
	})

	t.Run("OnClose", func(t *testing.T) {
		reconnector := &readerReconnector{}
		testErr := errors.New("test'")

		go func() {
			reconnector.Close(context.Background(), testErr)
		}()

		_, err := reconnector.ReadMessageBatch(context.Background(), readMessageBatchOptions{})
		require.ErrorIs(t, err, testErr)
	})
}

func TestTopicReaderReconnectorCommit(t *testing.T) {
	ctx := context.WithValue(context.Background(), "k", "v")
	expectedCommitRange := commitRange{commitOffsetStart: 1, commitOffsetEnd: 2}
	testErr := errors.New("test")
	testErr2 := errors.New("test2")
	t.Run("AllOk", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		stream := NewMockbatchedStreamReader(mc)
		stream.EXPECT().Commit(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, offset commitRange) {
			require.Equal(t, "v", ctx.Value("k"))
			require.Equal(t, expectedCommitRange, offset)
		})
		reconnector := &readerReconnector{streamVal: stream}
		reconnector.initChannels()
		require.NoError(t, reconnector.Commit(ctx, expectedCommitRange))
	})
	t.Run("StreamOkCommitErr", func(t *testing.T) {
		mc := gomock.NewController(t)
		stream := NewMockbatchedStreamReader(mc)
		stream.EXPECT().Commit(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, offset commitRange) {
			require.Equal(t, "v", ctx.Value("k"))
			require.Equal(t, expectedCommitRange, offset)
		}).Return(testErr)
		reconnector := &readerReconnector{streamVal: stream}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
	t.Run("StreamErr", func(t *testing.T) {
		reconnector := &readerReconnector{streamErr: testErr}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
	t.Run("CloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{closedErr: testErr}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
	t.Run("StreamAndCloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{closedErr: testErr, streamErr: testErr2}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, expectedCommitRange), testErr)
	})
}

func TestTopicReaderReconnectorConnectionLoop(t *testing.T) {
	t.Run("Reconnect", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		newStream1 := NewMockbatchedStreamReader(mc)
		newStream1.EXPECT().Close(gomock.Any(), gomock.Any())
		newStream2 := NewMockbatchedStreamReader(mc)
		newStream2.EXPECT().Close(gomock.Any(), gomock.Any())

		reconnector := &readerReconnector{}
		reconnector.initChannels()

		stream1Ready := make(emptyChan)
		stream2Ready := make(emptyChan)
		reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
			{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					close(stream1Ready)
					return newStream1, nil
				},
			},
			{
				err: xerrors.Retryable(errors.New("test reconnect error")),
			},
			{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					close(stream2Ready)
					return newStream2, nil
				},
			},
			{
				callback: func(ctx context.Context) (batchedStreamReader, error) {
					t.Error()
					return nil, errors.New("unexpected call")
				},
			},
		}...)

		reconnector.background.Start("test-reconnectionLoop", reconnector.reconnectionLoop)
		reconnector.reconnectFromBadStream <- nil

		<-stream1Ready

		// skip bad (old) stream
		reconnector.reconnectFromBadStream <- NewMockbatchedStreamReader(mc)

		reconnector.reconnectFromBadStream <- newStream1

		<-stream2Ready

		reconnector.Close(context.Background(), errReaderClosed)
	})

	t.Run("StartWithCancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reconnector := &readerReconnector{}
		reconnector.reconnectionLoop(ctx) // must return
	})
}

func TestTopicReaderReconnectorStart(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	ctx := context.Background()

	reconnector := &readerReconnector{}
	reconnector.initChannels()

	stream := NewMockbatchedStreamReader(mc)
	stream.EXPECT().Close(gomock.Any(), gomock.Any()).Do(func(_ context.Context, err error) {
		require.Error(t, err)
	})

	connectionRequested := make(emptyChan)
	reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
		{callback: func(ctx context.Context) (batchedStreamReader, error) {
			close(connectionRequested)
			return stream, nil
		}},
		{callback: func(ctx context.Context) (batchedStreamReader, error) {
			t.Error()
			return nil, errors.New("unexpected call")
		}},
	}...)

	reconnector.start()

	<-connectionRequested
	reconnector.Close(ctx, nil)
}

func TestTopicReaderReconnectorFireReconnectOnRetryableError(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		mc := gomock.NewController(t)
		reconnector := &readerReconnector{}

		stream := NewMockbatchedStreamReader(mc)
		reconnector.initChannels()

		reconnector.fireReconnectOnRetryableError(stream, nil)
		select {
		case <-reconnector.reconnectFromBadStream:
			t.Fatal()
		default:
			// OK
		}

		reconnector.fireReconnectOnRetryableError(stream, xerrors.Wrap(errors.New("test")))
		select {
		case <-reconnector.reconnectFromBadStream:
			t.Fatal()
		default:
			// OK
		}

		reconnector.fireReconnectOnRetryableError(stream, xerrors.Retryable(errors.New("test")))
		res := <-reconnector.reconnectFromBadStream
		require.Equal(t, stream, res)
	})

	t.Run("SkipWriteOnFullChannel", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		reconnector := &readerReconnector{}
		stream := NewMockbatchedStreamReader(mc)
		reconnector.initChannels()

	fillChannel:
		for {
			select {
			case reconnector.reconnectFromBadStream <- nil:
				// repeat
			default:
				break fillChannel
			}
		}

		// write skipped
		reconnector.fireReconnectOnRetryableError(stream, xerrors.Retryable(errors.New("test")))
		res := <-reconnector.reconnectFromBadStream
		require.Nil(t, res)
	})
}

type readerConnectFuncAnswer struct {
	callback readerConnectFunc
	stream   batchedStreamReader
	err      error
}

func readerConnectFuncMock(answers ...readerConnectFuncAnswer) readerConnectFunc {
	return func(ctx context.Context) (batchedStreamReader, error) {
		res := answers[0]
		if len(answers) > 1 {
			answers = answers[1:]
		}

		if res.callback == nil {
			return res.stream, res.err
		}

		return res.callback(ctx)
	}
}
