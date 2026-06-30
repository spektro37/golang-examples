package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"taskworkers/internal/pool"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	p, err := pool.New(4, 4)
	if err != nil {
		return fmt.Errorf("main.run(): could not create a pool %w", err)
	}

	p.Start(ctx)

	var wg sync.WaitGroup
	wg.Add(1)
	go consumeResults(p.Results, &wg)

	for i := range 20 {
		if err := p.Submit(ctx, pool.Task{
			Id:      i,
			Payload: "TestString" + strconv.Itoa(i),
		}); err != nil {
			break
		}
	}

	p.Shutdown()
	wg.Wait()

	return nil
}

func consumeResults(results <-chan pool.Result, wg *sync.WaitGroup) {
	defer wg.Done()
	for result := range results {
		// Логика обработки ошибок и результатов может отличаться в зависимости от реального сценария.
		if result.Error != nil {
			fmt.Printf("Error: ID %d, error %v\n", result.TaskId, result.Error)
		} else {
			fmt.Printf("Success: ID %d, payload %s\n", result.TaskId, result.Payload)
		}
	}
}
