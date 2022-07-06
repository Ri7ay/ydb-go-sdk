package topicreader

import (
	"bufio"
	"io"
)

type oneTimeReader struct {
	len    int
	err    error
	reader bufio.Reader
}

func newOneTimeReader(reader io.Reader, uncompressedSize int) *oneTimeReader {
	res := &oneTimeReader{
		len: uncompressedSize,
	}
	res.reader.Reset(reader)
	return res
}

func (s *oneTimeReader) Read(p []byte) (n int, err error) {
	if s.err != nil {
		return 0, s.err
	}

	n, err = s.reader.Read(p)
	s.len -= n

	if _, nextErr := s.reader.Peek(1); nextErr != nil {
		s.err = nextErr
		s.reader.Reset(nil)
	}

	return n, err
}

func (s *oneTimeReader) Len() int {
	if s.len < 0 {
		return 0
	}
	return s.len
}
