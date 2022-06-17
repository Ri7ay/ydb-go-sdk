package blocks

import (
	"bytes"
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
)

type Block struct {
	Meta
	Data bytes.Buffer
}

type Meta struct {
	Offset           int
	PartNumber       int
	MessageCount     int
	UncompressedSize int
	Codec            topic.Codec
}

var _ BlockIteratot = &BasicFormatBlockEncoder{}

type BasicFormatBlockEncoder struct {
	messages MessageIterator
	encoder  Encoder

	runCtx context.Context
	stop   context.CancelFunc
	err    error

	blocksChan chan *Block
	curBlock   *Block

	blocksPool *Pool
}

func NewBasicFormatBlockEncoder(ctx context.Context, it MessageIterator) *BasicFormatBlockEncoder {
	enc := &BasicFormatBlockEncoder{
		messages:   it,
		encoder:    RawCodec{},
		blocksChan: make(chan *Block, 1),
		blocksPool: &defaultBlocksPool,
	}
	enc.runCtx, enc.stop = context.WithCancel(ctx)
	go enc.prefetch()
	return enc
}

func (e *BasicFormatBlockEncoder) SetBlocksPool(p *Pool) {
	e.blocksPool = p
}

func (e *BasicFormatBlockEncoder) SetEncoder(enc Encoder) {
	e.encoder = enc
}

func (e *BasicFormatBlockEncoder) NextBlock() (*Block, bool) {
	e.blocksPool.Put(e.curBlock)
	select {
	case b, ok := <-e.blocksChan:
		if !ok {
			return nil, true
		}
		e.curBlock = b
		return e.curBlock, false
	case <-e.runCtx.Done():
	}
	return nil, true
}

func (e *BasicFormatBlockEncoder) Err() error {
	switch {
	case e.err != nil:
		return e.err
	default:
		return e.runCtx.Err()
	}
}

func (e *BasicFormatBlockEncoder) Close() error {
	e.stop()
	return nil
}

const prefetch = 16

type blockFuture struct {
	errCh <-chan error
	block *Block
}

// prefetch make some concurrent magic for parallel block compression
func (e *BasicFormatBlockEncoder) prefetch() {
	defer close(e.blocksChan)

	if e.runCtx.Err() != nil {
		return
	}

	// prefetchChan should be replaced if more precision limits needed for compress buffer
	prefetchChan := make(chan blockFuture, prefetch)

	go func() {
		defer close(prefetchChan)
		curOffset := 0 // actually start from 1 but increnented in loop
		codec := e.encoder.Codec()

		for {
			msg, end := e.messages.NextMessage()
			if end {
				return
			}

			errCh := make(chan error)

			fut := blockFuture{
				block: e.blocksPool.Get(),
				errCh: errCh,
			}
			// Specific for basic format
			curOffset++
			fut.block.Offset = curOffset
			fut.block.PartNumber = 0
			fut.block.MessageCount = 1
			fut.block.UncompressedSize = msg.Len()

			fut.block.Codec = codec

			go func() {
				defer close(errCh)
				if err := e.encoder.Encode(e.runCtx, msg, &fut.block.Data); err != nil {
					errCh <- err
				}
			}()
			select {
			case <-e.runCtx.Done():
				return
			case prefetchChan <- fut:
			}
		}
	}()

	for fut := range prefetchChan {
		futErr := <-fut.errCh
		if e.err != nil {
			continue
		}
		e.err = futErr
		if e.err != nil {
			e.stop()
			continue
		}

		select {
		case <-e.runCtx.Done():
			return
		case e.blocksChan <- fut.block:
		}
	}
}
