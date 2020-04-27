package kafka

import (
	"context"
	"time"
)

type topicCtx struct {
	ctx context.Context
	cancel context.CancelFunc
}

func newTopicCtx(ctx context.Context, cancelFunc context.CancelFunc) *topicCtx {
	return &topicCtx{ctx: ctx, cancel:cancelFunc}
}

func (c *topicCtx) Cancel() {
	c.cancel()
}

func (c *topicCtx) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *topicCtx) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *topicCtx) Err() error {
	return c.ctx.Err()
}

func (c *topicCtx) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}
