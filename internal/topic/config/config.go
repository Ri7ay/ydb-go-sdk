package config

import (
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Config struct {
	trace trace.Topic

	operationTimeout     time.Duration
	operationCancelAfter time.Duration
}

func (c Config) OperationTimeout() time.Duration {
	return c.operationTimeout
}

func (c Config) OperationCancelAfter() time.Duration {
	return c.operationCancelAfter
}

func (c Config) Trace() trace.Topic {
	return c.trace
}

type Option func(c *Config)

// WithTrace defines trace over persqueue client calls
func WithTrace(trace trace.Topic, opts ...trace.TopicComposeOption) Option {
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

func New(opts ...Option) Config {
	c := Config{}
	for _, o := range opts {
		o(&c)
	}
	return c
}
