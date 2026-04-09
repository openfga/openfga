package concurrency

import "sync"

// ShutdownGroup is like sync.WaitGroup, but allows Go and Wait to run concurrently,
// rejecting new function executions once Wait begins.
type ShutdownGroup struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

// Go starts func in a new goroutine unless shutdown has already begun.
// It reports whether the function was accepted for execution.
func (sg *ShutdownGroup) Go(f func()) (accepted bool) {
	sg.mu.Lock()
	if sg.closed {
		sg.mu.Unlock()
		return false
	}

	sg.wg.Add(1)
	sg.mu.Unlock()

	go func() {
		defer sg.wg.Done()
		f()
	}()

	return true
}

// Wait marks the group as closed and waits for all started goroutines to finish.
func (sg *ShutdownGroup) Wait() {
	sg.mu.Lock()
	sg.closed = true
	sg.mu.Unlock()
	sg.wg.Wait()
}
