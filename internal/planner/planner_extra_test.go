package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNewNoopPlanner(t *testing.T) {
	p := NewNoopPlanner()
	require.NotNil(t, p)

	// A noop planner still hands out a usable selector and has no cleanup routine to stop.
	sel := p.GetPlanSelector(testPlanKey("some-key"))
	require.NotNil(t, sel)
}

func TestPlanner_CleanupRoutineStartsAndStops(t *testing.T) {
	// EvictionThreshold + CleanupInterval both > 0 triggers the background cleanup routine.
	p := New(&Config{
		EvictionThreshold: 10 * time.Millisecond,
		CleanupInterval:   time.Millisecond,
	})
	require.NotNil(t, p)

	// Register a key so the planner has something to track.
	p.GetPlanSelector(testPlanKey("key-1"))

	// Stop must terminate the cleanup goroutine without hanging.
	done := make(chan struct{})
	go func() {
		p.Stop()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return; cleanup goroutine likely leaked")
	}
}
