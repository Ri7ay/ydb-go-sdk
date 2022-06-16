package pq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	_ topicStreamReader = &readerReconnector{} // check interface implementation
)

func TestTopicReaderReconnectorReadMessageBatch(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		baseReader := NewMocktopicStreamReader(mc)

		opts := ReadMessageBatchOptions{maxMessages: 10}
		batch := &Batch{
			Messages: []Message{{WrittenAt: time.Date(2022, 06, 15, 17, 56, 0, 0, time.UTC)}},
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

		baseReader := NewMocktopicStreamReader(mc)
		opts := ReadMessageBatchOptions{maxMessages: 10}
		batch := &Batch{
			Messages: []Message{{WrittenAt: time.Date(2022, 06, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		connectCalled := 0
		reader := &readerReconnector{
			readerConnect: func(ctx context.Context) (topicStreamReader, error) {
				connectCalled++
				if connectCalled > 1 {
					return nil, errors.New("unexpected call test connect function")
				}
				return baseReader, nil
			},
			streamErr: errUnconnected,
		}
		reader.initChannels()
		reader.background.Start(reader.reconnectionLoop)

		res, err := reader.ReadMessageBatch(context.Background(), opts)
		require.NoError(t, err)
		require.Equal(t, batch, res)
	})

	t.Run("WithReConnect", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		opts := ReadMessageBatchOptions{maxMessages: 10}

		baseReader1 := NewMocktopicStreamReader(mc)
		baseReader1.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(nil, xerrors.Retryable(errors.New("test1")))
		baseReader1.EXPECT().Close(gomock.Any(), gomock.Any()).Return()

		baseReader2 := NewMocktopicStreamReader(mc)
		baseReader2.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(nil, xerrors.Retryable(errors.New("test2")))
		baseReader2.EXPECT().Close(gomock.Any(), gomock.Any()).Return()

		baseReader3 := NewMocktopicStreamReader(mc)
		batch := &Batch{
			Messages: []Message{{WrittenAt: time.Date(2022, 06, 15, 17, 56, 0, 0, time.UTC)}},
		}
		baseReader3.EXPECT().ReadMessageBatch(gomock.Any(), opts).Return(batch, nil)

		readers := []topicStreamReader{
			baseReader1, baseReader2, baseReader3,
		}
		connectCalled := 0
		reader := &readerReconnector{
			readerConnect: func(ctx context.Context) (topicStreamReader, error) {
				connectCalled++
				return readers[connectCalled-1], nil
			},
			streamErr: errUnconnected,
		}
		reader.initChannels()
		reader.background.Start(reader.reconnectionLoop)

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

			_, err := reconnector.ReadMessageBatch(cancelledCtx, ReadMessageBatchOptions{})
			require.ErrorIs(t, err, context.Canceled)
		}
	})

	t.Run("OnClose", func(t *testing.T) {
		reconnector := &readerReconnector{}
		testErr := errors.New("test'")

		go func() {
			reconnector.Close(context.Background(), testErr)
		}()

		_, err := reconnector.ReadMessageBatch(context.Background(), ReadMessageBatchOptions{})
		require.ErrorIs(t, err, testErr)
	})
}

func TestTopicReaderReconnectorCommit(t *testing.T) {
	ctx := context.WithValue(context.Background(), "k", "v")
	commitBatch := CommitBatch{CommitOffset{Offset: 1, ToOffset: 2}}
	testErr := errors.New("test")
	testErr2 := errors.New("test2")
	t.Run("AllOk", func(t *testing.T) {
		mc := gomock.NewController(t)
		defer mc.Finish()

		stream := NewMocktopicStreamReader(mc)
		stream.EXPECT().Commit(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, offset CommitBatch) {
			require.Equal(t, "v", ctx.Value("k"))
			require.Equal(t, commitBatch, offset)
		})
		reconnector := &readerReconnector{streamVal: stream}
		reconnector.initChannels()
		require.NoError(t, reconnector.Commit(ctx, commitBatch))
	})
	t.Run("StreamOkCommitErr", func(t *testing.T) {
		mc := gomock.NewController(t)
		stream := NewMocktopicStreamReader(mc)
		stream.EXPECT().Commit(gomock.Any(), gomock.Any()).Do(func(ctx context.Context, offset CommitBatch) {
			require.Equal(t, "v", ctx.Value("k"))
			require.Equal(t, commitBatch, offset)
		}).Return(testErr)
		reconnector := &readerReconnector{streamVal: stream}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
	t.Run("StreamErr", func(t *testing.T) {
		reconnector := &readerReconnector{streamErr: testErr}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
	t.Run("CloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{closedErr: testErr}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
	t.Run("StreamAndCloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{closedErr: testErr, streamErr: testErr2}
		reconnector.initChannels()
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
}

func TestTopicReaderReconnectorConnectionLoop(t *testing.T) {
	t.Run("Reconnect", func(t *testing.T) {
		newStream1 := &topicStreamReaderMock{}
		newStream1.On("Close", mock.Anything, mock.Anything)
		newStream2 := &topicStreamReaderMock{}
		newStream2.On("Close", mock.Anything, mock.Anything)

		reconnector := &readerReconnector{}
		reconnector.initChannels()

		stream1Ready := make(chan struct{})
		stream2Ready := make(chan struct{})
		reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
			{
				callback: func(ctx context.Context) (topicStreamReader, error) {
					close(stream1Ready)
					return newStream1, nil
				},
			},
			{
				err: xerrors.Retryable(errors.New("test reconnect error")),
			},
			{
				callback: func(ctx context.Context) (topicStreamReader, error) {
					close(stream2Ready)
					return newStream2, nil
				},
			},
			{
				callback: func(ctx context.Context) (topicStreamReader, error) {
					t.Error()
					return nil, errors.New("unexpected call")
				},
			},
		}...)

		reconnector.background.Start(reconnector.reconnectionLoop)
		reconnector.reconnectFromBadStream <- nil

		<-stream1Ready

		// skip bad (old) stream
		reconnector.reconnectFromBadStream <- &topicStreamReaderMock{}

		reconnector.reconnectFromBadStream <- newStream1

		<-stream2Ready

		reconnector.Close(context.Background(), errReaderClosed)

		newStream1.AssertExpectations(t)
		newStream2.AssertExpectations(t)
	})

	t.Run("StartWithCancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reconnector := &readerReconnector{}
		reconnector.reconnectionLoop(ctx) // must return
	})
}

func TestTopicReaderReconnectorStart(t *testing.T) {
	ctx := context.Background()

	reconnector := &readerReconnector{}
	reconnector.initChannels()

	stream := &topicStreamReaderMock{}
	stream.On("Close", mock.Anything, mock.Anything).Once().Run(func(args mock.Arguments) {
		require.Error(t, args.Error(1))
	})

	connectionRequested := make(chan struct{})
	reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
		{callback: func(ctx context.Context) (topicStreamReader, error) {
			close(connectionRequested)
			return stream, nil
		}},
		{callback: func(ctx context.Context) (topicStreamReader, error) {
			t.Error()
			return nil, errors.New("unexpected call")
		}},
	}...)

	reconnector.start(ctx)

	<-connectionRequested
	reconnector.Close(ctx, nil)

	stream.AssertExpectations(t)
}

func TestTopicReaderReconnectorFireReconnectOnRetryableError(t *testing.T) {
	t.Run("Ok", func(t *testing.T) {
		reconnector := &readerReconnector{}
		stream := &topicStreamReaderMock{}
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
		reconnector := &readerReconnector{}
		stream := &topicStreamReaderMock{}
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
	stream   topicStreamReader
	err      error
}

func readerConnectFuncMock(answers ...readerConnectFuncAnswer) readerConnectFunc {
	return func(ctx context.Context) (topicStreamReader, error) {
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

type topicStreamReaderMock struct {
	mock.Mock
}

func (t *topicStreamReaderMock) ReadMessageBatch(ctx context.Context, opts ReadMessageBatchOptions) (batch *Batch, _ error) {
	res := t.Called(ctx, opts)
	if v := res.Get(0); v != nil {
		batch = v.(*Batch)
	}
	return batch, res.Error(1)
}

func (t *topicStreamReaderMock) Commit(ctx context.Context, offset CommitBatch) error {
	res := t.Called(ctx, offset)
	return res.Error(0)
}

func (t *topicStreamReaderMock) Close(ctx context.Context, err error) {
	t.Called(ctx, err)
}
