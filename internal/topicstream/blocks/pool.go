package blocks

import "sync"

var defaultBlocksPool Pool

type Pool struct {
	pool sync.Pool
}

func (p *Pool) Get() *Block {
	b := p.pool.Get()
	if b != nil {
		return b.(*Block)
	}
	return &Block{}
}

func (p *Pool) Put(b *Block) {
	if b == nil {
		return
	}
	b.Data.Reset()
	*b = Block{Data: b.Data}
	p.pool.Put(b)
}
