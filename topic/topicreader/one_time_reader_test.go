package topicreader

import (
	"bufio"
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
		r := newOneTimeReader(bytes.NewReader([]byte{1, 2, 3}), 3)
		dstBuf := make([]byte, 3)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3}, dstBuf)
		require.Equal(t, 0, r.reader.Buffered())
		require.Zero(t, r.len)
		require.Equal(t, io.EOF, r.err)
	})
	t.Run("DstMoreThenContent", func(t *testing.T) {
		r := newOneTimeReader(bytes.NewReader([]byte{1, 2, 3}), 3)
		dstBuf := make([]byte, 4)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3, 0}, dstBuf)
		require.Equal(t, 0, r.reader.Buffered())
		require.Zero(t, r.len)
		require.Equal(t, io.EOF, r.err)
	})
	t.Run("ReadLess", func(t *testing.T) {
		r := newOneTimeReader(bytes.NewReader([]byte{1, 2, 3}), 3)
		dstBuf := make([]byte, 2)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 2, n)
		require.Equal(t, []byte{1, 2}, dstBuf)
		require.Equal(t, 1, r.reader.Buffered())
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
		r := newOneTimeReader(nil, 3)
		r.reader = *bufio.NewReaderSize(nil, 2)

		bufSize := r.reader.Size()
		preparedData := make([]byte, 2*bufSize)
		for i := 0; i < 2*bufSize; i++ {
			if i < bufSize {
				preparedData[i] = 1
			} else {
				preparedData[i] = 2
			}
		}
		r.reader.Reset(iotest.TimeoutReader(bytes.NewReader(preparedData)))

		// first read is ok
		firstBuf := make([]byte, bufSize)
		n, err := r.Read(firstBuf)
		require.NoError(t, err)
		require.Equal(t, bufSize, n)
		require.Equal(t, preparedData[:bufSize], firstBuf)
		require.NoError(t, err)

		// iotest.TimeoutReader return timeout for second read
		secondBuf := make([]byte, bufSize)
		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
		require.Equal(t, make([]byte, bufSize), secondBuf)

		// Next read again
		n, err = r.Read(secondBuf)
		require.Equal(t, err, iotest.ErrTimeout)
		require.Equal(t, 0, n)
	})
	t.Run("InnterReadMoreThenLen", func(t *testing.T) {
		r := newOneTimeReader(bytes.NewReader([]byte{1, 2, 3}), 2)
		dstBuf := make([]byte, 3)
		n, err := r.Read(dstBuf)
		require.NoError(t, err)
		require.Equal(t, 3, n)
		require.Equal(t, []byte{1, 2, 3}, dstBuf)
		require.Equal(t, 0, r.reader.Buffered())
		require.Equal(t, -1, r.len)
		require.Equal(t, 0, r.Len())
	})
}
