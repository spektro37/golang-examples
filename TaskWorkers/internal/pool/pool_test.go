package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestBaseline(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		bufferSize  int
		taskCount   int
		payload     string
		expectError bool
	}{
		{"small size and load - positive", 1, 1, 1, "TestString", false},
		{"medium size and load - positive", 4, 10, 80, "TestString", false},
		{"large size and load - positive", 8, 100, 4000, "TestString", false},
		{"small size and load - negative", 1, 1, 1, "", true},
		{"medium size and load - negative", 4, 10, 80, "", true},
		{"large size and load - negative", 8, 100, 4000, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := New(tt.workerCount, tt.bufferSize)
			p.delay = time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			p.Start(ctx)

			var results []Result
			var wg sync.WaitGroup

			wg.Go(func() {
				for result := range p.Results {
					results = append(results, result)
				}
			})

			for i := range tt.taskCount {
				if err := p.Submit(ctx, Task{
					Id:      i,
					Payload: tt.payload,
				}); err != nil {
					break
				}
			}

			p.Shutdown()

			wg.Wait()

			if len(results) != tt.taskCount {
				t.Errorf("result count %d does not match test count %d", len(results), tt.taskCount)
			}

			for _, r := range results {
				if !tt.expectError && r.Error != nil {
					t.Errorf("task %d: unexpected error: %v", r.TaskId, r.Error)
				}
				reversedPayload := reverse(tt.payload)
				if !tt.expectError && r.Payload != reversedPayload {
					t.Errorf("task %d: payload want %q, got %q", r.TaskId, reversedPayload, r.Payload)
				}
				if tt.expectError && r.Error == nil {
					t.Errorf("task %d: expected error but got none", r.TaskId)
				}
			}
		})
	}
}

func TestShutdown(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		bufferSize  int
		taskCount   int
		delay       time.Duration
	}{
		{"small size and load", 2, 5, 10, 100 * time.Millisecond},
		{"medium size and load", 4, 10, 20, 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := New(tt.workerCount, tt.bufferSize)
			p.delay = tt.delay
			ctx, cancel := context.WithCancel(context.Background())

			p.Start(ctx)

			var results []Result
			var wg sync.WaitGroup

			wg.Go(func() {
				for result := range p.Results {
					results = append(results, result)
				}
			})

			for i := range tt.taskCount {
				if err := p.Submit(ctx, Task{Id: i, Payload: "TestString"}); err != nil {
					break
				}
			}

			cancel()
			p.Shutdown()
			wg.Wait()

			for _, r := range results {
				if r.Error != nil && !errors.Is(r.Error, context.Canceled) {
					t.Errorf("task %d: expected context.Canceled, got %v", r.TaskId, r.Error)
				}
			}
		})
	}
}

func TestConcurrency(t *testing.T) {
	tests := []struct {
		name        string
		workerCount int
		delay       time.Duration
	}{
		{"small size", 2, 100 * time.Millisecond},
		{"medium size", 4, 100 * time.Millisecond},
		{"large size", 8, 100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, _ := New(tt.workerCount, tt.workerCount)
			p.delay = tt.delay
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			p.Start(ctx)

			var results []Result
			var wg sync.WaitGroup

			wg.Go(func() {
				for result := range p.Results {
					results = append(results, result)
				}
			})

			start := time.Now()
			for i := range tt.workerCount {
				p.Submit(ctx, Task{Id: i, Payload: "TestString"})
			}

			p.Shutdown()
			wg.Wait()
			elapsed := time.Since(start)

			if elapsed > 2*tt.delay {
				t.Errorf("expected concurrent execution under %v, took %v", 2*tt.delay, elapsed)
			}
			if len(results) != tt.workerCount {
				t.Errorf("want %d, got %d", tt.workerCount, len(results))
			}
		})
	}
}
