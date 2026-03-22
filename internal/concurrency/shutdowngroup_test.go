package concurrency

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShutdownGroupGoAndWaitRace(t *testing.T) {
	for i := 0; i < 100; i++ {
		var sg ShutdownGroup

		start := make(chan struct{})
		done := make(chan struct{})

		go func() {
			<-start
			sg.Go(func() {})
			close(done)
		}()

		go func() {
			<-start
			sg.Wait()
		}()

		close(start)
		<-done
		sg.Wait()
	}
}

func TestShutdownGroupGoRunsFunc(t *testing.T) {
	var sg ShutdownGroup
	done := make(chan struct{})

	sg.Go(func() {
		close(done)
	})

	sg.Wait()

	select {
	case <-done:
	default:
		t.Fatal("expected goroutine to run before wait returned")
	}
}

func TestShutdownGroupWaitBlocksUntilStartedGoroutinesFinish(t *testing.T) {
	var sg ShutdownGroup

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	waitDone := make(chan struct{})

	sg.Go(func() {
		close(started)
		<-release
		close(finished)
	})

	go func() {
		sg.Wait()
		close(waitDone)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	select {
	case <-waitDone:
		t.Fatal("expected wait to block until the goroutine is released")
	default:
	}

	close(release)

	require.Eventually(t, func() bool {
		select {
		case <-finished:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.Eventually(t, func() bool {
		select {
		case <-waitDone:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)
}

func TestShutdownGroupGoAfterWaitDoesNotRun(t *testing.T) {
	var sg ShutdownGroup
	var ran atomic.Bool
	done := make(chan struct{})

	sg.Wait()
	sg.Go(func() {
		ran.Store(true)
		close(done)
	})

	sg.Wait()

	select {
	case <-done:
		t.Fatal("expected no goroutine to start after wait")
	default:
	}

	require.False(t, ran.Load())
}
