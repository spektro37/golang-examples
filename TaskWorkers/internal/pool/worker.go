package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

func work(ctx context.Context, tasks <-chan Task, results chan<- Result, wg *sync.WaitGroup, delay time.Duration) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case task, ok := <-tasks:
			if !ok {
				return
			}
			results <- process(ctx, task, delay)
		}
	}
}

func process(ctx context.Context, task Task, delay time.Duration) Result {
	select {
	case <-ctx.Done():
		return Result{
			TaskId: task.Id,
			Error:  fmt.Errorf("pool.work: task interrupted %w", ctx.Err()),
		}
	case <-time.After(delay):
		if task.Payload == "" {
			return Result{
				TaskId: task.Id,
				Error:  errors.New("pool.work: empty payload"),
			}
		}
		return Result{
			TaskId:  task.Id,
			Payload: reverse(task.Payload),
		}
	}
}

func reverse(input string) string {
	result := []rune(input)
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return string(result)
}
