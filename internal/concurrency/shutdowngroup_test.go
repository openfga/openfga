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
		accepted := make(chan bool, 1)

		go func() {
			<-start
			accepted <- sg.Go(func() {})
		}()

		go func() {
			<-start
			sg.Wait()
		}()

		close(start)
		<-accepted
		sg.Wait()
	}
}

func TestShutdownGroupGoRunsFunc(t *testing.T) {
	var sg ShutdownGroup
	done := make(chan struct{})

	accepted := sg.Go(func() {
		close(done)
	})
	require.True(t, accepted)

	sg.Wait()

	select {
	case <-done:
	default:
		t.Fatal("expected func to run before wait returned")
	}
}

func TestShutdownGroupWaitBlocksUntilStartedFuncFinish(t *testing.T) {
	var sg ShutdownGroup

	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	waitDone := make(chan struct{})

	accepted := sg.Go(func() {
		close(started)
		<-release
		close(finished)
	})
	require.True(t, accepted)

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
		t.Fatal("expected wait to block until the func is released")
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
	accepted := sg.Go(func() {
		ran.Store(true)
		close(done)
	})
	require.False(t, accepted)

	sg.Wait()

	select {
	case <-done:
		t.Fatal("expected no func to start after wait")
	default:
	}

	require.False(t, ran.Load())
}
