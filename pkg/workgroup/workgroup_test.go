package workgroup

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	var i atomic.Int32

	ctx := context.Background()

	p := Bound(2, func(j int32) {
		i.Add(j)
	})

	p.Push(ctx, 1)
	p.Close()
	p.Push(ctx, 2)

	require.Equal(t, int32(1), i.Load())
}

func TestWorkerPoolWaitCancel(t *testing.T) {
	var i atomic.Int32

	ctx := context.Background()

	p := Bound(2, func(j int32) {
		i.Add(j)
		time.Sleep(1 * time.Second)
	})

	p.Push(ctx, 1)
	p.Push(ctx, 1)

	go func() {
		p.Push(ctx, 1)
		p.Push(ctx, 1)
		p.Push(ctx, 1)
		p.Push(ctx, 1)
		p.Push(ctx, 1)
	}()
	p.Close()

	require.Equal(t, int32(2), i.Load())
}
