package counter

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type Counter struct {
	value    atomic.Int64
	interval time.Duration
}

func New(interval time.Duration) (*Counter, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("counter.New: invalid interval %v", interval)
	}
	return &Counter{interval: interval}, nil
}

func (c *Counter) Start(ctx context.Context) {
	go c.count(ctx)
}

func (c *Counter) Value() int64 {
	return c.value.Load()
}

func (c *Counter) count(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.value.Add(1)
		}
	}
}
