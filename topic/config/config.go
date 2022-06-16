package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config struct {
	trace trace.Persqueue

	operationTimeout     time.Duration
	operationCancelAfter time.Duration

	cluster string

	panicCallback func(e interface{})
}

func (c Config) OperationTimeout() time.Duration {
	return c.operationTimeout
}

func (c Config) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

func (c Config) Cluster() string {
	return c.cluster
}

func (c Config) Trace() trace.Persqueue {
	return c.trace
}

func (c Config) PanicCallback() func(e interface{}) {
	return c.panicCallback
}

type Option func(c *Config)

// WithTrace defines trace over persqueue client calls
func WithTrace(trace trace.Persqueue, opts ...trace.PersqueueComposeOption) Option {
	return func(c *Config) {
		c.trace = c.trace.Compose(trace, opts...)
	}
}

// WithOperationTimeout set the maximum amount of time a YDB server will process
// an operation. After timeout exceeds YDB will try to cancel operation and
// regardless of the cancellation appropriate error will be returned to
// the client.
// If OperationTimeout is zero then no timeout is used.
func WithOperationTimeout(operationTimeout time.Duration) Option {
	return func(c *Config) {
		c.operationTimeout = operationTimeout
	}
}

// WithOperationCancelAfter set the maximum amount of time a YDB server will process an
// operation. After timeout exceeds YDB will try to cancel operation and if
// it succeeds appropriate error will be returned to the client; otherwise
// processing will be continued.
// If OperationCancelAfter is zero then no timeout is used.
func WithOperationCancelAfter(operationCancelAfter time.Duration) Option {
	return func(c *Config) {
		c.operationCancelAfter = operationCancelAfter
	}
}

// WithCluster set name of persqueue cluster explicitly.
// If no cluster discovery  used then this options should be setted.
func WithCluster(name string) Option {
	return func(c *Config) {
		c.cluster = name
	}
}

// WithPanicCallback set user-defined panic callback
// If nil - panic callback not defined
func WithPanicCallback(cb func(e interface{})) Option {
	return func(c *Config) {
		c.panicCallback = cb
	}
}

func New(opts ...Option) Config {
	c := Config{}
	for _, o := range opts {
		o(&c)
	}
	return c
}
