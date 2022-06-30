package topicreader

import (
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

type oneTimeReader struct {
	len    int
	err    error
	reader io.Reader
}

func (s *oneTimeReader) Read(p []byte) (n int, err error) {
	if s.err != nil {
		return 0, s.err
	}

	n, err = s.reader.Read(p)
	s.len -= n

	if s.len < 0 {
		s.err = xerrors.NewWithStackTrace("ydb: uncompressed size more, then received from server")
		s.reader = nil
		return n, s.err
	}

	if s.len == 0 {
		s.err = err
		if err == nil {
			s.err = io.EOF
		}
		s.reader = nil
	}

	return n, err
}

func (s *oneTimeReader) Len() int {
	return s.len
}
