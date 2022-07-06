package topicreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopiccommon"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
)

var (
	ErrUnexpectedCodec          = errors.New("unexpected codec")
	ErrContextExplicitCancelled = errors.New("context explicit cancelled")
)

type LenReader interface {
	io.Reader

	// Len calculated from uncompressed size, sent by writer. Server doesn't check it and it can be wrong.
	// can be use for optimization, but code must ready for wrong size, returned by len.
	Len() int
}

type Message struct {
	commitRange

	SeqNo                int64
	CreatedAt            time.Time
	MessageGroupID       string
	WriteSessionMetadata map[string]string
	Offset               int64
	WrittenAt            time.Time
	Data                 LenReader

	rawDataLen         int
	bufferBytesAccount int
}

func (m *Message) Context() context.Context {
	return m.commitRange.session().Context()
}

func (m *Message) Topic() string {
	return m.session().Topic
}

func createReader(codec rawtopiccommon.Codec, rawBytes []byte, uncompressedSize int64) *oneTimeReader {
	var reader io.Reader
	switch codec {
	case rawtopiccommon.CodecRaw:
		reader = bytes.NewReader(rawBytes)
		if uncompressedSize == 0 {
			// TODO: Migration protocol workaround
			uncompressedSize = int64(len(rawBytes))
		}
	case rawtopiccommon.CodecGzip:
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

	return newOneTimeReader(reader, int(uncompressedSize))
}

type errorReader struct {
	err error
}

func (u errorReader) Read(p []byte) (n int, err error) {
	return 0, u.err
}
