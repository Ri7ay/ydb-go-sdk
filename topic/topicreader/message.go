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
	UncompressedSize   int // as sent by sender, server/sdk doesn't check the field. It may be empty or wrong.
}

func (m *Message) Context() context.Context {
	return m.commitRange.session().Context()
}

func (m *Message) Topic() string {
	return m.session().Topic
}

func createReader(codec rawtopiccommon.Codec, rawBytes []byte) *oneTimeReader {
	var reader io.Reader
	switch codec {
	case rawtopiccommon.CodecRaw:
		reader = bytes.NewReader(rawBytes)
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

	return newOneTimeReader(reader)
}

type errorReader struct {
	err error
}

func (u errorReader) Read(p []byte) (n int, err error) {
	return 0, u.err
}
