package utils

import (
	"sync"
	"testing"
)

// Test the resolution metadata by ensuring whether forking keeps new db counter and allows using of the same counter
func TestResolutionMetadata(t *testing.T) {
	resolutionCounter := NewResolutionMetadata()
	const numResolve = 5
	for i := 0; i < numResolve; i++ {
		resolutionCounter.AddResolve()
	}
	const numRead = 2
	for i := 0; i < numRead; i++ {
		resolutionCounter.AddReadCall()
	}

	const numWrite = 4
	for i := 0; i < numWrite; i++ {
		resolutionCounter.AddWriteCall()
	}

	var wg sync.WaitGroup
	const numForLoop = 20
	wg.Add(numForLoop)

	for i := 0; i < numForLoop; i++ {
		forkedResolutionCounter := resolutionCounter.Fork()
		go func() {
			defer wg.Done()
			forkedResolutionCounter.AddResolve()
			forkedResolutionCounter.AddReadCall()
			forkedResolutionCounter.AddWriteCall()

		}()
	}

	wg.Wait()
	if resolutionCounter.GetResolve() != numResolve {
		t.Errorf("Expect resolve call to be %d, actual %d", numResolve, resolutionCounter.GetResolve())
	}
	if resolutionCounter.GetReadCalls() != numRead+numForLoop {
		t.Errorf("Expect db read call to be %d, actual %d", numRead+numForLoop, resolutionCounter.GetReadCalls())
	}
	if resolutionCounter.GetWriteCalls() != numWrite+numForLoop {
		t.Errorf("Expect db write call to be %d, actual %d", numWrite+numForLoop, resolutionCounter.GetWriteCalls())
	}

}

func TestDBCallsCounter(t *testing.T) {
	rwCounter := NewDBCallCounter()
	const numWrite = 5
	const numRead = 10
	for i := 0; i < numWrite; i++ {
		rwCounter.AddWriteCall()
	}
	for i := 0; i < numRead; i++ {
		rwCounter.AddReadCall()
	}
	if rwCounter.GetWriteCalls() != numWrite {
		t.Errorf("Expect %d writes, acutal %d", numWrite, rwCounter.GetWriteCalls())
	}
	if rwCounter.GetReadCalls() != numRead {
		t.Errorf("Expect %d reads, acutal %d", numRead, rwCounter.GetReadCalls())
	}
}
