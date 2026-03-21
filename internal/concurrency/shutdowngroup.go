package concurrency

import "sync"

// ShutdownGroup tracks goroutines and stops accepting new work after shutdown begins.
type ShutdownGroup struct {
	mu     sync.Mutex
	wg     sync.WaitGroup
	closed bool
}

// Go starts func in a new goroutine unless shutdown has already begun.
func (sg *ShutdownGroup) Go(f func()) {
	sg.mu.Lock()
	if sg.closed {
		sg.mu.Unlock()
		return
	}

	sg.wg.Add(1)
	sg.mu.Unlock()

	go func() {
		defer sg.wg.Done()
		f()
	}()
}

// Wait marks the group as closed and waits for all started goroutines to finish.
func (sg *ShutdownGroup) Wait() {
	sg.mu.Lock()
	sg.closed = true
	sg.mu.Unlock()
	sg.wg.Wait()
}
