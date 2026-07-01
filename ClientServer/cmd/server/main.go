package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"clientserver/internal/counter"
)

const (
	addr           = ":9000"
	tickInterval   = time.Second
	requestBufSize = 64
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	c, err := counter.New(tickInterval)
	if err != nil {
		return fmt.Errorf("main.run: could not create counter %w", err)
	}
	c.Start(ctx)

	conn, err := net.ListenPacket("udp", addr)
	if err != nil {
		return fmt.Errorf("main.run: could not listen on %s %w", addr, err)
	}

	go func() {
		<-ctx.Done()
		conn.Close()
	}()

	fmt.Printf("server listening on %s\n", addr)
	serve(conn, c)

	return nil
}

func serve(conn net.PacketConn, c *counter.Counter) {
	var wg sync.WaitGroup
	buf := make([]byte, requestBufSize)

	for {
		_, clientAddr, err := conn.ReadFrom(buf)
		if err != nil {
			break
		}
		wg.Add(1)
		go respond(conn, clientAddr, c.Value(), &wg)
	}

	wg.Wait()
}

func respond(conn net.PacketConn, addr net.Addr, value int64, wg *sync.WaitGroup) {
	defer wg.Done()
	payload := []byte(strconv.FormatInt(value, 10))
	if _, err := conn.WriteTo(payload, addr); err != nil {
		fmt.Printf("server.respond: could not write to %s: %v\n", addr, err)
	}
}
