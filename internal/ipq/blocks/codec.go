package blocks

import (
	"context"
	"io"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type Encoder interface {
	Codec() topic.Codec
	Encode(context.Context, io.Reader, io.Writer) error
}

type Decode interface {
	Codec() topic.Codec
	Decode(context.Context, io.Reader, io.Writer) error
}

type RawCodec struct{}

func (RawCodec) Codec() topic.Codec {
	return topic.CodecRaw
}

func (c RawCodec) Encode(ctx context.Context, r io.Reader, w io.Writer) error {
	return c.copy(ctx, r, w)
}

func (c RawCodec) Decode(ctx context.Context, r io.Reader, w io.Writer) error {
	return c.copy(ctx, r, w)
}

func (RawCodec) copy(ctx context.Context, r io.Reader, w io.Writer) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	_, err := io.Copy(w, r)
	return err
}
