package workgroup

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundWorkGroupPushToClosedPool(t *testing.T) {
	var i atomic.Int32

	ctx := context.Background()

	p := Bound(2, func(j int32) error {
		i.Add(j)
		return nil
	})

	hnd1 := p.Push(ctx, 1)
	p.Close()
	hnd2 := p.Push(ctx, 2)

	err := <-hnd1
	require.NoError(t, err)
	err = <-hnd2
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.Equal(t, int32(1), i.Load())
}

func TestBoundWorkGroupPushToCanceledContext(t *testing.T) {
	var i atomic.Int32

	ctx, cancel := context.WithCancel(context.Background())

	p := Bound(2, func(j int32) error {
		i.Add(j)
		return nil
	})

	hnd1 := p.Push(ctx, 1)
	cancel()
	hnd2 := p.Push(ctx, 2)

	err := <-hnd1
	require.NoError(t, err)
	err = <-hnd2
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
	require.Equal(t, int32(1), i.Load())
}

func TestBoundWorkGroupBlocking(t *testing.T) {
	var i atomic.Int32

	ctx := context.Background()

	p := Bound(1, func(j int32) error {
		i.Add(j)
		return nil
	})

	hnd1 := p.Push(ctx, 1)
	hnd2 := p.Push(ctx, 1)
	hnd3 := p.Push(ctx, 1)

	err := <-hnd1
	require.NoError(t, err)
	err = <-hnd2
	require.NoError(t, err)
	err = <-hnd3
	require.NoError(t, err)
	require.Equal(t, int32(3), i.Load())
}

func TestBoundWorkGroupNonBlocking(t *testing.T) {
	var i atomic.Int32

	ctx := context.Background()

	p := Bound(2, func(j int32) error {
		i.Add(j)
		return nil
	})

	hnd1 := p.Push(ctx, 1)
	hnd2 := p.Push(ctx, 1)
	hnd3 := p.Push(ctx, 1)

	err := <-hnd1
	require.NoError(t, err)
	err = <-hnd2
	require.NoError(t, err)
	err = <-hnd3
	require.NoError(t, err)
	require.Equal(t, int32(3), i.Load())
}

func TestBoundWorkGroupPanic(t *testing.T) {
	ctx := context.Background()

	p := Bound(2, func(j int32) error {
		panic("panic")
	})

	hnd := p.Push(ctx, 1)
	err := <-hnd
	require.Error(t, err)
}
