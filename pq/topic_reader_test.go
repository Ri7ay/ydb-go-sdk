package pq

import (
	"context"
	"errors"
	"runtime"
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
	baseReader.On("ReadMessageBatch", mock.Anything, ReadMessageBatchOptions{}).Run(func(args mock.Arguments) {
		<-readerContext.Done()
		return
	}).Return(nil, testErr)
	baseReader.On("ReadMessageBatch", mock.Anything, ReadMessageBatchOptions{maxMessages: 1}).Run(func(args mock.Arguments) {
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
		readerCommitState.err = reader.Commit(context.Background(), Message{})
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

	runtime.Gosched()

	// check about no methods finished before close
	for i := range allStates {
		require.False(t, isCallCompleted(allStates[i]))
	}
	require.NoError(t, reader.Close())

	// check about all methods stop work after close
	for i := range allStates {
		<-allStates[i].callCompleted
		require.Error(t, allStates[i].err, i)
	}
}

func TestReader_Commit(t *testing.T) {
	baseReader := &topicStreamReaderMock{}
	reader := &Reader{reader: baseReader}

	expectedBatchOk := CommitBatch{{
		Offset:   1,
		ToOffset: 10,
	}}
	expectedBatchOk[0].partitionSessionID.FromInt64(10)
	baseReader.On("Commit", mock.Anything, expectedBatchOk).Once().Return(nil)
	require.NoError(t, reader.Commit(context.Background(), Message{CommitOffset: expectedBatchOk[0]}))

	expectedBatchErr := CommitBatch{{
		Offset:   15,
		ToOffset: 20,
	}}
	expectedBatchErr[0].partitionSessionID.FromInt64(30)
	testErr := errors.New("test err")
	baseReader.On("Commit", mock.Anything, expectedBatchErr).Once().Return(testErr)
	require.ErrorIs(t, reader.Commit(context.Background(), Message{CommitOffset: expectedBatchErr[0]}), testErr)
}

func TestReader_CommitBatch(t *testing.T) {
	baseReader := &topicStreamReaderMock{}
	reader := &Reader{reader: baseReader}

	expectedBatchOk := CommitBatch{{
		Offset:   1,
		ToOffset: 10,
	}}
	expectedBatchOk[0].partitionSessionID.FromInt64(10)
	baseReader.On("Commit", mock.Anything, expectedBatchOk).Once().Return(nil)
	require.NoError(t, reader.CommitBatch(context.Background(), expectedBatchOk))

	expectedBatchErr := CommitBatch{{
		Offset:   15,
		ToOffset: 20,
	}}
	expectedBatchErr[0].partitionSessionID.FromInt64(30)
	testErr := errors.New("test err")
	baseReader.On("Commit", mock.Anything, expectedBatchErr).Once().Return(testErr)
	require.ErrorIs(t, reader.CommitBatch(context.Background(), expectedBatchErr), testErr)
}

func TestReader_CommitMessages(t *testing.T) {
	baseReader := &topicStreamReaderMock{}
	reader := &Reader{reader: baseReader}

	expectedBatchOk := CommitBatch{
		{
			Offset:   1,
			ToOffset: 2,
		},
		{
			Offset:   2,
			ToOffset: 3,
		},
	}
	expectedBatchOk[0].partitionSessionID.FromInt64(10)
	expectedBatchOk[1].partitionSessionID.FromInt64(10)
	baseReader.On("Commit", mock.Anything, expectedBatchOk).Once().Return(nil)
	require.NoError(t, reader.CommitMessages(context.Background(),
		&Message{CommitOffset: expectedBatchOk[0]}, &Message{CommitOffset: expectedBatchOk[1]},
	))

	expectedBatchErr := CommitBatch{
		{
			Offset:   3,
			ToOffset: 4,
		},
		{
			Offset:   4,
			ToOffset: 5,
		},
	}
	expectedBatchErr[0].partitionSessionID.FromInt64(30)
	expectedBatchErr[1].partitionSessionID.FromInt64(30)
	testErr := errors.New("test err")
	baseReader.On("Commit", mock.Anything, expectedBatchErr).Once().Return(testErr)
	require.ErrorIs(t, reader.CommitMessages(context.Background(),
		&Message{CommitOffset: expectedBatchErr[0]},
		&Message{CommitOffset: expectedBatchErr[1]},
	), testErr)
}
