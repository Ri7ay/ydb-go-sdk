package topicreader

import "io"

type SizeReader interface {
	io.Reader

	// Size return full uncompressed size of data
	// it is not changed while read data
	Size() int
}

type sizeReader struct {
	size   int
	reader io.Reader
}

func (s sizeReader) Read(p []byte) (n int, err error) {
	return s.reader.Read(p)
}

func (s sizeReader) Size() int {
	return s.size
}
