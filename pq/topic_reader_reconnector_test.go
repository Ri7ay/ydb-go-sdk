package pq

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	_ topicStreamReader = &readerReconnector{} // check interface implementation
)

func TestTopicReaderReconnectorReadMessageBatch(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		cancelledCtx, cancelledCtxCancel := context.WithCancel(context.Background())
		cancelledCtxCancel()

		batch1 := &Batch{partitionContext: context.Background(), Messages: []Message{{WrittenAt: time.Date(2022, 6, 10, 21, 21, 0, 1, time.UTC)}}}
		batch2 := &Batch{partitionContext: cancelledCtx, Messages: []Message{{WrittenAt: time.Date(2022, 6, 10, 21, 21, 0, 2, time.UTC)}}}
		batch3 := &Batch{partitionContext: context.Background(), Messages: []Message{{WrittenAt: time.Date(2022, 6, 10, 21, 21, 0, 3, time.UTC)}}}

		reconnector := &readerReconnector{}
		reconnector.initChannels()

		go func() {
			reconnector.nextBatch <- batch1
			reconnector.nextBatch <- batch2
			reconnector.nextBatch <- batch3
		}()

		res, err := reconnector.ReadMessageBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, batch1, res)

		res, err = reconnector.ReadMessageBatch(ctx)
		require.NoError(t, err)
		require.Equal(t, batch3, res)

		go func() { cancel() }()
		_, err = reconnector.ReadMessageBatch(ctx)
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("StartWithCancelledContext", func(t *testing.T) {
		cancelledCtx, cancelledCtxCancel := context.WithCancel(context.Background())
		cancelledCtxCancel()

		for i := 0; i < 100; i++ {
			reconnector := &readerReconnector{}
			reconnector.initChannels()
			go func() { reconnector.nextBatch <- &Batch{} }()

			_, err := reconnector.ReadMessageBatch(cancelledCtx)
			require.ErrorIs(t, err, context.Canceled)
		}
	})

	t.Run("OnClose", func(t *testing.T) {
		reconnector := &readerReconnector{}
		testErr := errors.New("test'")

		go func() {
			reconnector.Close(context.Background(), testErr)
		}()

		_, err := reconnector.ReadMessageBatch(context.Background())
		require.ErrorIs(t, err, testErr)
	})
}

func TestTopicReaderReconnectorCommit(t *testing.T) {
	ctx := context.WithValue(context.Background(), "k", "v")
	commitBatch := CommitBatch{CommitOffset{Offset: 1, ToOffset: 2}}
	testErr := errors.New("test")
	testErr2 := errors.New("test2")
	t.Run("AllOk", func(t *testing.T) {
		stream := &topicStreamReaderMock{}
		stream.On("Commit", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			localCtx := args.Get(0).(context.Context)
			require.Equal(t, "v", localCtx.Value("k"))
			require.Equal(t, commitBatch, args.Get(1))
		}).Return(nil)
		reconnector := &readerReconnector{streamVal: stream}
		require.NoError(t, reconnector.Commit(ctx, commitBatch))
	})
	t.Run("StreamOkCommitErr", func(t *testing.T) {
		stream := &topicStreamReaderMock{}
		stream.On("Commit", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			localCtx := args.Get(0).(context.Context)
			require.Equal(t, "v", localCtx.Value("k"))
			require.Equal(t, commitBatch, args.Get(1))
		}).Return(testErr)
		reconnector := &readerReconnector{streamVal: stream}
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
	t.Run("StreamErr", func(t *testing.T) {
		reconnector := &readerReconnector{streamErr: testErr}
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
	t.Run("CloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{closedErr: testErr}
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
	t.Run("StreamAndCloseErr", func(t *testing.T) {
		reconnector := &readerReconnector{closedErr: testErr, streamErr: testErr2}
		require.ErrorIs(t, reconnector.Commit(ctx, commitBatch), testErr)
	})
}

func TestTopicReaderReconnectorConnectionLoop(t *testing.T) {
	t.Run("Reconnect", func(t *testing.T) {

		newStream1ReadStarted := make(chan bool)
		newStream1 := &topicStreamReaderMock{}
		newStream1.On("Close", mock.Anything, mock.Anything).Once().Run(func(args mock.Arguments) {
			require.Error(t, args.Error(1))
		})
		newStream1.On("ReadMessageBatch", mock.Anything).Once().Run(func(args mock.Arguments) {
			close(newStream1ReadStarted)
			<-args.Get(0).(context.Context).Done()
		}).Return(nil, context.Canceled)

		newStream2ReadStarted := make(chan bool)
		newStream2 := &topicStreamReaderMock{}
		newStream2.On("Close", mock.Anything, mock.Anything).Once().Run(func(args mock.Arguments) {
			require.Error(t, args.Error(1))
		})
		newStream2.On("ReadMessageBatch", mock.Anything).Once().Run(func(args mock.Arguments) {
			close(newStream2ReadStarted)
			<-args.Get(0).(context.Context).Done()
		}).Return(nil, context.Canceled)

		reconnector := &readerReconnector{}
		reconnector.initChannels()

		reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
			{
				stream: newStream1,
			},
			{
				err: xerrors.Retryable(errors.New("test reconnect error")),
			},
			{
				stream: newStream2,
			},
			{
				callback: func(ctx context.Context) (topicStreamReader, error) {
					t.Error()
					return nil, errors.New("unexpected call")
				},
			},
		}...)

		reconnector.background.Start(reconnector.connectionLoop)
		reconnector.reconnectFromBadStream <- nil

		<-newStream1ReadStarted

		// skip bad (old) stream
		reconnector.reconnectFromBadStream <- &topicStreamReaderMock{}

		reconnector.reconnectFromBadStream <- newStream1

		<-newStream2ReadStarted

		reconnector.Close(context.Background(), errReaderClosed)

		newStream1.AssertExpectations(t)
		newStream2.AssertExpectations(t)
	})

	t.Run("StartWithCancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		reconnector := &readerReconnector{}
		reconnector.connectionLoop(ctx) // must return
	})
}

func TestTopicReaderReconnectorStart(t *testing.T) {
	ctx := context.Background()

	reconnector := &readerReconnector{}
	reconnector.initChannels()

	streamStartRead := make(chan bool)
	stream := &topicStreamReaderMock{}
	stream.On("Close", mock.Anything, mock.Anything).Once().Run(func(args mock.Arguments) {
		require.Error(t, args.Error(1))
	})
	stream.On("ReadMessageBatch", mock.Anything).Once().Run(func(args mock.Arguments) {
		close(streamStartRead)
		<-args.Get(0).(context.Context).Done()
	}).Return(nil, context.Canceled)

	reconnector.readerConnect = readerConnectFuncMock([]readerConnectFuncAnswer{
		{stream: stream},
		{callback: func(ctx context.Context) (topicStreamReader, error) {
			t.Error()
			return nil, errors.New("unexpected call")
		}},
	}...)

	reconnector.start(ctx)

	<-streamStartRead
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

func (t *topicStreamReaderMock) ReadMessageBatch(ctx context.Context) (batch *Batch, _ error) {
	res := t.Called(ctx)
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
