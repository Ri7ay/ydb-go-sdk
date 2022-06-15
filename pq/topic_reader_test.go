package pq

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/backgroundworkers"
)

func TestReader_Close(t *testing.T) {
	testErr := errors.New("test error")
	readerContext, readerCancel := context.WithCancel(context.Background())
	baseReader := &topicStreamReaderMock{}
	baseReader.On("ReadMessageBatch", mock.Anything).Run(func(args mock.Arguments) {
		<-readerContext.Done()
		return
	}).Return(nil, testErr)
	baseReader.On("Commit", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		<-readerContext.Done()
		return
	}).Return(testErr)
	baseReader.On("Close", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		readerCancel()
	}).Return(nil)

	reader := &Reader{
		reader:                baseReader,
		messageReaderLoopOnce: sync.Once{},
		background:            backgroundworkers.BackgroundWorker{},
	}

	type callState struct {
		callCompleted chan struct{}
		err           error
	}

	isCallCompleted := func(state *callState) bool {
		select {
		case <-state.callCompleted:
			return true
		default:
			return false
		}
	}

	var allStates []*callState
	newCallState := func() *callState {
		state := &callState{
			callCompleted: make(chan struct{}),
		}
		allStates = append(allStates, state)
		return state
	}

	readerCommitState := newCallState()
	readerCommitBatchState := newCallState()
	readerReadMessageState := newCallState()
	readerReadMessageBatchState := newCallState()
	readerCommitMessagesState := newCallState()

	go func() {
		readerCommitState.err = reader.Commit(context.Background(), nil)
		close(readerCommitState.callCompleted)
	}()

	go func() {
		readerCommitBatchState.err = reader.CommitBatch(context.Background(), nil)
		close(readerCommitBatchState.callCompleted)
	}()

	go func() {
		readerCommitMessagesState.err = reader.CommitMessages(context.Background())
		close(readerCommitMessagesState.callCompleted)
	}()

	go func() {
		_, readerReadMessageState.err = reader.ReadMessage(context.Background())
		close(readerReadMessageState.callCompleted)
	}()

	go func() {
		_, readerReadMessageBatchState.err = reader.ReadMessageBatch(context.Background())
		close(readerReadMessageBatchState.callCompleted)
	}()

	for i := range allStates {
		require.False(t, isCallCompleted(allStates[i]))
	}
	require.NoError(t, reader.Close())

	for i := range allStates {
		require.True(t, isCallCompleted(allStates[i]))
		require.ErrorIs(t, allStates[i].err, testErr)
	}

}
