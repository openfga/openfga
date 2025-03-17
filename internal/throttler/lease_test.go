package throttler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestImpatientLessorTimeout(t *testing.T) {
	l := Impatient(1, 100*time.Millisecond)

	err := l.Acquire(context.Background())
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	err = l.Acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	l.Done()
	l.Done()
	cancel()
}

func TestImpatientLessorCancel(t *testing.T) {
	l := Impatient(1, 10*time.Second)

	ctx, cancel := context.WithCancel(context.Background())

	l.Acquire(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := l.Acquire(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}()
	cancel()
	wg.Wait()
	l.Done()
	l.Done()
}

func TestImpatientLessorSuccess(t *testing.T) {
	l := Impatient(1, 10*time.Second)

	err := l.Acquire(context.Background())
	require.NoError(t, err)
	l.Done()
	err = l.Acquire(context.Background())
	require.NoError(t, err)
	l.Done()
}
