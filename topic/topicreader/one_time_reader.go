package topicreader

import (
	"io"
)

type oneTimeReader struct {
	err    error
	reader io.Reader
}

func newOneTimeReader(reader io.Reader) *oneTimeReader {
	res := &oneTimeReader{}
	res.reader = reader
	return res
}

func (s *oneTimeReader) Read(p []byte) (n int, err error) {
	if s.err != nil {
		return 0, s.err
	}

	n, err = s.reader.Read(p)

	if err != nil {
		s.err = err
		s.reader = nil
	}

	return n, err
}
