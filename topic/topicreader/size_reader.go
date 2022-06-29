package topicreader

import "io"

type LenReader interface {
	io.Reader
	Len() int
}

type lenReader struct {
	len    int
	reader io.Reader
}

func (s *lenReader) Read(p []byte) (n int, err error) {
	n, err = s.reader.Read(p)
	s.len -= n
	return n, err
}

func (s *lenReader) Size() int {
	return s.len
}
