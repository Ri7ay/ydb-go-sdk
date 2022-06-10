package xcontext

import (
	"context"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeCloseByFinalizer(t *testing.T) {
	var closeChan chan struct{}
	ctx, _ := Merge(context.Background())
	mergedCtx := ctx.(*mergeContext)
	closeChan = mergedCtx.stopDoneWaiter

	runtime.GC()

	// write to closed channel cause panic
	require.Panics(t, func() {
		closeChan <- struct{}{}
	})
}

func TestMergeCancelledByChild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	merged, _ := Merge(context.Background(), ctx)
	go cancel()
	<-merged.Done()
}

func TestMergeCancel(t *testing.T) {
	t.Run("ContextCancel", func(t *testing.T) {
		ctx, cancel := Merge(context.Background())
		cancel(context.Canceled)

		merged := ctx.(*mergeContext)
		require.Panics(t, func() {
			merged.done <- struct{}{}
		})
		require.Panics(t, func() {
			merged.stopDoneWaiter <- struct{}{}
		})
	})

	t.Run("CancelAfterChildCancelled", func(t *testing.T) {
		ctxWithCancel, ctxCancel := context.WithCancel(context.Background())
		ctx, mergedCancel := Merge(context.Background(), ctxWithCancel)
		go ctxCancel()
		<-ctx.Done()

		mergedCancel(context.Canceled)
		merged := ctx.(*mergeContext)
		require.Panics(t, func() {
			merged.done <- struct{}{}
		})
		require.Panics(t, func() {
			merged.stopDoneWaiter <- struct{}{}
		})
	})

	t.Run("CancelWithNilError", func(t *testing.T) {
		ctx, cancel := Merge(context.Background())
		require.Panics(t, func() {
			cancel(nil)
		})
		runtime.KeepAlive(ctx)
	})

	t.Run("CancelAfterCtxOutOfScope", func(t *testing.T) {
		_, cancel := Merge(context.Background())

		runtime.GC()
		cancel(context.Canceled)
	})
}
