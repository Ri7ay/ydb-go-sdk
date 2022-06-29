package topicreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnexpectedCodec          = errors.New("unexpected codec")
	ErrContextExplicitCancelled = errors.New("context explicit cancelled")
)

type Message struct {
	commitRange

	SeqNo                int64
	CreatedAt            time.Time
	MessageGroupID       string
	WriteSessionMetadata map[string]string
	Offset               int64
	WrittenAt            time.Time
	Data                 io.Reader

	rawDataLen         int
	bufferBytesAccount int
}

func (m *Message) Context() context.Context {
	return m.commitRange.session().Context()
}

func (m *Message) Topic() string {
	return m.session().Topic
}

func createReader(codec rawtopic.Codec, rawBytes []byte, uncompressedSize int64) *lenReader {
	var reader io.Reader
	switch codec {
	case rawtopic.CodecRaw:
		reader = bytes.NewReader(rawBytes)
	case rawtopic.CodecGzip:
		gzipReader, err := gzip.NewReader(bytes.NewReader(rawBytes))
		if err == nil {
			reader = gzipReader
		} else {
			reader = errorReader{err: xerrors.WithStackTrace(fmt.Errorf("failed read gzip message: %w", err))}
		}
	default:
		reader = errorReader{
			err: xerrors.WithStackTrace(fmt.Errorf("received message with codec '%v': %w", codec, ErrUnexpectedCodec)),
		}
	}

	return &lenReader{reader: reader, len: int(uncompressedSize)}
}

type errorReader struct {
	err error
}

func (u errorReader) Read(p []byte) (n int, err error) {
	return 0, u.err
}
