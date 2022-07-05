package topicreader

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestReader_Close(t *testing.T) {
	mc := gomock.NewController(t)
	defer mc.Finish()

	testErr := errors.New("test error")
	readerContext, readerCancel := context.WithCancel(context.Background())
	baseReader := NewMockbatchedStreamReader(mc)
	baseReader.EXPECT().ReadMessageBatch(gomock.Any(), readMessageBatchOptions{}).Do(func(_, _ interface{}) {
		<-readerContext.Done()
	}).Return(Batch{}, testErr)
	baseReader.EXPECT().ReadMessageBatch(gomock.Any(), readMessageBatchOptions{batcherGetOptions: batcherGetOptions{MaxCount: 1, MinCount: 1}}).Do(func(_, _ interface{}) {
		<-readerContext.Done()
		return
	}).Return(Batch{}, testErr)
	baseReader.EXPECT().Commit(gomock.Any(), gomock.Any()).Do(func(_, _ interface{}) {
		<-readerContext.Done()
		return
	}).Return(testErr)
	baseReader.EXPECT().Close(gomock.Any(), gomock.Any()).Do(func(_, _ interface{}) {
		readerCancel()
	})

	reader := &Reader{
		reader:                baseReader,
		messageReaderLoopOnce: sync.Once{},
	}

	type callState struct {
		callCompleted emptyChan
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
			callCompleted: make(emptyChan),
		}
		allStates = append(allStates, state)
		return state
	}

	readerCommitState := newCallState()
	readerReadMessageState := newCallState()
	readerReadMessageBatchState := newCallState()

	go func() {
		readerCommitState.err = reader.Commit(context.Background(), Message{})
		close(readerCommitState.callCompleted)
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
	mc := gomock.NewController(t)
	defer mc.Finish()

	baseReader := NewMockbatchedStreamReader(mc)
	reader := &Reader{reader: baseReader}

	expectedRangeOk := commitRange{
		commitOffsetStart: 1,
		commitOffsetEnd:   10,
		partitionSession:  &partitionSession{partitionSessionID: 10},
	}
	baseReader.EXPECT().Commit(gomock.Any(), expectedRangeOk).Return(nil)
	require.NoError(t, reader.Commit(context.Background(), Message{commitRange: expectedRangeOk}))

	expectedRangeErr := commitRange{
		commitOffsetStart: 15,
		commitOffsetEnd:   20,
		partitionSession:  &partitionSession{partitionSessionID: 30},
	}

	testErr := errors.New("test err")
	baseReader.EXPECT().Commit(gomock.Any(), expectedRangeErr).Return(testErr)
	require.ErrorIs(t, reader.Commit(context.Background(), Message{commitRange: expectedRangeErr}), testErr)
}