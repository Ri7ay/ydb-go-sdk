package blocks_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/ipq/blocks"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

func TestPerMessageEncode(t *testing.T) {
	strings := []string{"one", "two", "three"}
	mi := msgSliceIter{}
	mi.AddStrings(strings...)

	enc := blocks.NewBasicFormatBlockEncoder(context.TODO(), &mi)

	want := []blocks.Block{}
	for i, s := range strings {
		want = append(want, blocks.Block{
			Meta: blocks.Meta{
				Offset:           i + 1,
				PartNumber:       0,
				MessageCount:     1,
				UncompressedSize: len(s), Codec: topic.CodecRaw,
			},
			Data: *bytes.NewBuffer([]byte(s)),
		})
	}

	i := 0
	for {
		b, end := enc.NextBlock()
		if end {
			break
		}
		if enc.Err() != nil {
			t.Fatalf("unexpected error: %s", enc.Err())
		}

		w := want[i]
		i++
		switch {
		case b.Meta != w.Meta:
			t.Fatalf("blocks meta not equal: %#v != %#v", b.Meta, w.Meta)
		case b.Data.String() != w.Data.String():
			t.Fatalf("blocks data not equal: %v != %v", b.Data.String(), w.Data.String())
		}
	}
}

type msgSliceIter []topic.EncodeReader

func (mi *msgSliceIter) NextMessage() (topic.EncodeReader, bool) {
	slice := *mi
	if len(slice) == 0 {
		return nil, true
	}
	*mi = slice[1:]
	return slice[0], false
}

func (mi *msgSliceIter) AddStrings(str ...string) {
	for _, s := range str {
		*mi = append(*mi, strings.NewReader(s))
	}
}
