package topicreader

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

var _ LenReader = &oneTimeReader{}

func TestOneTimeReader(t *testing.T) {
	t.Run("FullRead", func(t *testing.T) {
		r := &oneTimeReader{reader: bytes.NewReader([]byte{1, 2, 3}), len: 3}
		dstBuf := make([]byte, 3)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3}, dstBuf)
		require.Empty(t, r.reader)
		require.Zero(t, r.len)
		require.Equal(t, io.EOF, r.err)
	})
	t.Run("DstMoreThenContent", func(t *testing.T) {
		r := &oneTimeReader{reader: bytes.NewReader([]byte{1, 2, 3}), len: 3}
		dstBuf := make([]byte, 4)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3, 0}, dstBuf)
		require.Empty(t, r.reader)
		require.Zero(t, r.len)
		require.Equal(t, io.EOF, r.err)
	})
	t.Run("ReadLess", func(t *testing.T) {
		r := &oneTimeReader{reader: bytes.NewReader([]byte{1, 2, 3}), len: 3}
		dstBuf := make([]byte, 2)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{1, 2}, dstBuf)
		require.NotEmpty(t, r.reader)
		require.Equal(t, 1, r.len)
		require.NoError(t, r.err)
	})
	t.Run("ReadAfterError", func(t *testing.T) {
		testErr := errors.New("err")
		r := &oneTimeReader{err: testErr}
		dstBuf := make([]byte, 2)
		n, err := r.Read(dstBuf)
		require.Equal(t, testErr, err)
		require.Equal(t, 0, n)
	})
	t.Run("InnerErr", func(t *testing.T) {
		r := &oneTimeReader{reader: iotest.TimeoutReader(bytes.NewReader([]byte{1, 2, 3})), len: 3}

		// first read is ok
		firstBuf := make([]byte, 2)
		n, err := r.Read(firstBuf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{1, 2}, firstBuf)
		require.NoError(t, err)

		// iotest.TimeoutReader return timeout for second read
		secondBuf := []byte{9, 8, 7}
		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
		require.Equal(t, []byte{9, 8, 7}, secondBuf)

		// Next read again
		n, err = r.Read(secondBuf)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		require.Equal(t, []byte{3, 8, 7}, secondBuf)
	})
	t.Run("InnterReadMoreThenLen", func(t *testing.T) {
		r := &oneTimeReader{reader: bytes.NewReader([]byte{1, 2, 3}), len: 2}
		dstBuf := make([]byte, 3)
		n, err := r.Read(dstBuf)
		require.Error(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3}, dstBuf)
		require.Empty(t, r.reader)
		require.Equal(t, -1, r.len)
		require.Equal(t, err, r.err)
	})
}
