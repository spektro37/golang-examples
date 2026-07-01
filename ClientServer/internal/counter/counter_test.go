package counter

import (
	"context"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	tests := []struct {
		name        string
		interval    time.Duration
		expectError bool
	}{
		{"valid interval", 10 * time.Millisecond, false},
		{"zero interval", 0, true},
		{"negative interval", -time.Millisecond, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.interval)

			if tt.expectError && err == nil {
				t.Fatal("want error, got nil")
			}
			if !tt.expectError && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.expectError && c.Value() != 0 {
				t.Errorf("want initial value 0, got %d", c.Value())
			}
		})
	}
}

func TestIncrement(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		sleep    time.Duration
		minValue int64
	}{
		{"fast tick", 10 * time.Millisecond, 105 * time.Millisecond, 5},
		{"slower tick", 20 * time.Millisecond, 105 * time.Millisecond, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.interval)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			c.Start(ctx)
			time.Sleep(tt.sleep)

			if v := c.Value(); v < tt.minValue {
				t.Errorf("want value >= %d after %v with a %v tick, got %d", tt.minValue, tt.sleep, tt.interval, v)
			}
		})
	}
}

func TestStopsOnCancel(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
	}{
		{"fast tick", 5 * time.Millisecond},
		{"slower tick", 15 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.interval)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())

			c.Start(ctx)
			time.Sleep(6 * tt.interval)
			cancel()
			time.Sleep(2 * tt.interval)

			stopped := c.Value()
			time.Sleep(6 * tt.interval)

			if v := c.Value(); v != stopped {
				t.Errorf("want value to stay at %d after cancel, got %d", stopped, v)
			}
		})
	}
}
