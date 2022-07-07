package topicreader

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/grpcwrapper/rawtopic/rawtopicreader"
)

type serverMessageWrapper struct {
	message rawtopicreader.ServerMessage
}

func (w serverMessageWrapper) Type() string {
	return reflect.TypeOf(w.message).String()
}

func (w serverMessageWrapper) JsonData() io.Reader {
	content, _ := json.MarshalIndent(w.message, "", "  ")
	return newOneTimeReader(bytes.NewReader(content))
}

func (w serverMessageWrapper) IsReadStreamServerMessageDebugInfo() {}

type clientMessageWrapper struct {
	message rawtopicreader.ClientMessage
}

func (w clientMessageWrapper) Type() string {
	return reflect.TypeOf(w.message).String()
}

func (w clientMessageWrapper) JsonData() io.Reader {
	content, _ := json.MarshalIndent(w.message, "", "  ")
	return newOneTimeReader(bytes.NewReader(content))
}

func (w clientMessageWrapper) IsReadStreamClientMessageDebugInfo() {}
