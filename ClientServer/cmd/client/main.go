package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	serverAddr      = ":9000"
	pollInterval    = 100 * time.Millisecond
	responseBufSize = 64
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := net.Dial("udp", serverAddr)
	if err != nil {
		return fmt.Errorf("main.run: could not connect to %s %w", serverAddr, err)
	}
	defer conn.Close()

	poll(ctx, conn)

	return nil
}

func poll(ctx context.Context, conn net.Conn) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	buf := make([]byte, responseBufSize)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := request(conn, buf); err != nil {
				fmt.Printf("client.poll: %v\n", err)
				return
			}
		}
	}
}

func request(conn net.Conn, buf []byte) error {
	if _, err := conn.Write([]byte("GET")); err != nil {
		return fmt.Errorf("client.request: could not send request %w", err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		return fmt.Errorf("client.request: could not read response %w", err)
	}
	fmt.Printf("counter: %s\n", buf[:n])
	return nil
}
