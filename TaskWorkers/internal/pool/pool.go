package pool

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Pool struct {
	workerCount int
	delay       time.Duration
	tasks       chan Task
	Results     chan Result
	wg          sync.WaitGroup
}

func New(workerCount, bufferSize int) (*Pool, error) {
	if workerCount < 1 {
		return nil, fmt.Errorf("pool.New: invalid number of workers %d", workerCount)
	}
	if bufferSize < 1 {
		return nil, fmt.Errorf("pool.New: invalid buffer size %d", bufferSize)
	}
	return &Pool{
		workerCount: workerCount,
		delay:       100 * time.Millisecond,
		tasks:       make(chan Task, bufferSize),
		Results:     make(chan Result, bufferSize),
	}, nil
}

func (p *Pool) Start(ctx context.Context) {
	for range p.workerCount {
		p.wg.Add(1)
		go work(ctx, p.tasks, p.Results, &p.wg, p.delay)
	}
}

func (p *Pool) Submit(ctx context.Context, task Task) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("pool.Submit: submission interrupted %w", ctx.Err())
	case p.tasks <- task:
		return nil
	}
}

func (p *Pool) Shutdown() {
	close(p.tasks)
	p.wg.Wait()
	close(p.Results)
}
