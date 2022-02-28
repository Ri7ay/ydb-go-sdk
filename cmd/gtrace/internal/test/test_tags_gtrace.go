// Code generated by gtrace. DO NOT EDIT.

//go:build gtrace
// +build gtrace

package test

import (
	"bytes"
	"context"
)

// Compose returns a new BuildTagTrace which has functional fields composed
// both from t and x.
func (t BuildTagTrace) Compose(x BuildTagTrace) (ret BuildTagTrace) {
	switch {
	case t.OnSomethingA == nil:
		ret.OnSomethingA = x.OnSomethingA
	case x.OnSomethingA == nil:
		ret.OnSomethingA = t.OnSomethingA
	default:
		h1 := t.OnSomethingA
		h2 := x.OnSomethingA
		ret.OnSomethingA = func() func() {
			r1 := h1()
			r2 := h2()
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func() {
					r1()
					r2()
				}
			}
		}
	}
	switch {
	case t.OnSomethingB == nil:
		ret.OnSomethingB = x.OnSomethingB
	case x.OnSomethingB == nil:
		ret.OnSomethingB = t.OnSomethingB
	default:
		h1 := t.OnSomethingB
		h2 := x.OnSomethingB
		ret.OnSomethingB = func(i int8, i1 int16) func(int32, int64) {
			r1 := h1(i, i1)
			r2 := h2(i, i1)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(i int32, i1 int64) {
					r1(i, i1)
					r2(i, i1)
				}
			}
		}
	}
	switch {
	case t.OnSomethingC == nil:
		ret.OnSomethingC = x.OnSomethingC
	case x.OnSomethingC == nil:
		ret.OnSomethingC = t.OnSomethingC
	default:
		h1 := t.OnSomethingC
		h2 := x.OnSomethingC
		ret.OnSomethingC = func(t Type) func(Type) {
			r1 := h1(t)
			r2 := h2(t)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(t Type) {
					r1(t)
					r2(t)
				}
			}
		}
	}
	return ret
}

type buildTagTraceContextKey struct{}

// WithBuildTagTrace returns context which has associated BuildTagTrace with it.
func WithBuildTagTrace(ctx context.Context, t BuildTagTrace) context.Context {
	return context.WithValue(ctx,
		buildTagTraceContextKey{},
		ContextBuildTagTrace(ctx).Compose(t),
	)
}

// ContextBuildTagTrace returns BuildTagTrace associated with ctx.
// If there is no BuildTagTrace associated with ctx then zero value
// of BuildTagTrace is returned.
func ContextBuildTagTrace(ctx context.Context) BuildTagTrace {
	t, _ := ctx.Value(buildTagTraceContextKey{}).(BuildTagTrace)
	return t
}

func (t BuildTagTrace) onSomethingA(ctx context.Context) func() {
	c := ContextBuildTagTrace(ctx)
	var fn func() func()
	switch {
	case t.OnSomethingA == nil:
		fn = c.OnSomethingA
	case c.OnSomethingA == nil:
		fn = t.OnSomethingA
	default:
		h1 := t.OnSomethingA
		h2 := c.OnSomethingA
		fn = func() func() {
			r1 := h1()
			r2 := h2()
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func() {
					r1()
					r2()
				}
			}
		}
	}
	if fn == nil {
		return func() {
			return
		}
	}
	res := fn()
	if res == nil {
		return func() {
			return
		}
	}
	return res
}
func (t BuildTagTrace) onSomethingB(ctx context.Context, i int8, i1 int16) func(int32, int64) {
	c := ContextBuildTagTrace(ctx)
	var fn func(int8, int16) func(int32, int64)
	switch {
	case t.OnSomethingB == nil:
		fn = c.OnSomethingB
	case c.OnSomethingB == nil:
		fn = t.OnSomethingB
	default:
		h1 := t.OnSomethingB
		h2 := c.OnSomethingB
		fn = func(i int8, i1 int16) func(int32, int64) {
			r1 := h1(i, i1)
			r2 := h2(i, i1)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(i int32, i1 int64) {
					r1(i, i1)
					r2(i, i1)
				}
			}
		}
	}
	if fn == nil {
		return func(int32, int64) {
			return
		}
	}
	res := fn(i, i1)
	if res == nil {
		return func(int32, int64) {
			return
		}
	}
	return res
}
func (t BuildTagTrace) onSomethingC(ctx context.Context, t1 Type) func(Type) {
	c := ContextBuildTagTrace(ctx)
	var fn func(Type) func(Type)
	switch {
	case t.OnSomethingC == nil:
		fn = c.OnSomethingC
	case c.OnSomethingC == nil:
		fn = t.OnSomethingC
	default:
		h1 := t.OnSomethingC
		h2 := c.OnSomethingC
		fn = func(t Type) func(Type) {
			r1 := h1(t)
			r2 := h2(t)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(t Type) {
					r1(t)
					r2(t)
				}
			}
		}
	}
	if fn == nil {
		return func(Type) {
			return
		}
	}
	res := fn(t1)
	if res == nil {
		return func(Type) {
			return
		}
	}
	return res
}
func buildTagTraceOnSomethingA(ctx context.Context, t BuildTagTrace) func() {
	res := t.onSomethingA(ctx)
	return func() {
		res()
	}
}
func buildTagTraceOnSomethingB(ctx context.Context, t BuildTagTrace, i int8, i1 int16) func(int32, int64) {
	res := t.onSomethingB(ctx, i, i1)
	return func(i int32, i1 int64) {
		res(i, i1)
	}
}
func buildTagTraceOnSomethingC(ctx context.Context, t BuildTagTrace, e Embedded, s string, integer int, boolean bool, e1 error, r bytes.Reader) func(_ Embedded, _ string, integer int, boolean bool, _ error, _ bytes.Reader) {
	var p Type
	p.Embedded = e
	p.String = s
	p.Integer = integer
	p.Boolean = boolean
	p.Error = e1
	p.Reader = r
	res := t.onSomethingC(ctx, p)
	return func(e Embedded, s string, integer int, boolean bool, e1 error, r bytes.Reader) {
		var p Type
		p.Embedded = e
		p.String = s
		p.Integer = integer
		p.Boolean = boolean
		p.Error = e1
		p.Reader = r
		res(p)
	}
}
