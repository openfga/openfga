package track_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/openfga/openfga/internal/listobjects/pipeline/internal/track"
)

func TestStatusPoolWaitWithNoReporters(t *testing.T) {
	sp := track.NewStatusPool()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if !sp.Wait(ctx) {
		t.Fatal("Wait returned false with no reporters registered")
	}
}

func TestStatusPoolSingleReporter(t *testing.T) {
	sp := track.NewStatusPool()
	r := sp.Register()

	done := make(chan bool, 1)
	go func() {
		done <- sp.Wait(context.Background())
	}()

	select {
	case <-done:
		t.Fatal("Wait returned before reporter called Report")
	case <-time.After(50 * time.Millisecond):
	}

	r.Report()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Wait returned false after Report")
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after Report")
	}
}

func TestStatusPoolMultipleReporters(t *testing.T) {
	sp := track.NewStatusPool()
	r1 := sp.Register()
	r2 := sp.Register()
	r3 := sp.Register()

	done := make(chan bool, 1)
	go func() {
		done <- sp.Wait(context.Background())
	}()

	r1.Report()
	r3.Report()

	select {
	case <-done:
		t.Fatal("Wait returned before all reporters called Report")
	case <-time.After(50 * time.Millisecond):
	}

	r2.Report()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Wait returned false after all Report calls")
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after all Report calls")
	}
}

func TestStatusPoolWaitReturnsAfterQuiescence(t *testing.T) {
	sp := track.NewStatusPool()
	r := sp.Register()

	r.Inc()

	done := make(chan bool, 1)
	go func() {
		done <- sp.Wait(context.Background())
	}()

	r.Report()

	// All reported, but in-flight is still 1.
	select {
	case <-done:
		t.Fatal("Wait returned before in-flight count reached zero")
	case <-time.After(50 * time.Millisecond):
	}

	r.Dec()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Wait returned false after quiescence")
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after quiescence")
	}
}

func TestStatusPoolWaitContextCancelled(t *testing.T) {
	sp := track.NewStatusPool()
	sp.Register() // never reports

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool, 1)
	go func() {
		done <- sp.Wait(ctx)
	}()

	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("Wait returned true after context cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}

func TestStatusPoolWaitContextCancelledDuringQuiescence(t *testing.T) {
	sp := track.NewStatusPool()
	r := sp.Register()

	r.Inc()

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool, 1)
	go func() {
		done <- sp.Wait(ctx)
	}()

	// Report so ready phase completes, but in-flight is still 1.
	r.Report()

	// Give Wait time to enter the quiescence select.
	time.Sleep(50 * time.Millisecond)

	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Fatal("Wait returned true after context cancellation during quiescence")
		}
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after context cancellation during quiescence")
	}
}

func TestStatusPoolConcurrentReporters(t *testing.T) {
	sp := track.NewStatusPool()

	const n = 100
	reporters := make([]*track.Reporter, n)
	for i := range n {
		reporters[i] = sp.Register()
	}

	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			reporters[i].Report()
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wg.Wait()

	if !sp.Wait(ctx) {
		t.Fatal("Wait returned false after all concurrent Report calls")
	}
}

func TestReporterReportIsIdempotent(t *testing.T) {
	sp := track.NewStatusPool()
	r := sp.Register()

	r.Report()
	r.Report() // should not panic or change outcome

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if !sp.Wait(ctx) {
		t.Fatal("Wait returned false after idempotent Report calls")
	}
}

func TestReporterWaitDelegatesToPool(t *testing.T) {
	sp := track.NewStatusPool()
	r1 := sp.Register()
	r2 := sp.Register()

	done := make(chan bool, 1)
	go func() {
		done <- r1.Wait(context.Background())
	}()

	r1.Report()
	r2.Report()

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("Reporter.Wait returned false")
		}
	case <-time.After(time.Second):
		t.Fatal("Reporter.Wait did not return")
	}
}

func BenchmarkStatusPool(b *testing.B) {
	const concurrency = 1000

	sp := track.NewStatusPool()

	reporters := make([]*track.Reporter, concurrency)

	for i := range concurrency {
		reporters[i] = sp.Register()
	}

	for b.Loop() {
		for i := range concurrency {
			go func() {
				reporters[i].Report()
			}()
		}
		sp.Wait(context.Background())
	}
}
