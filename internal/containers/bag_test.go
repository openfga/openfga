package containers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBag(t *testing.T) {
	var b Bag[int]

	for i := range 1000 {
		b.Add(i + 1)
	}

	values := b.Seq()

	var count int

	cmp := 1000
	for i := range values {
		require.Equal(t, cmp, i)
		cmp--
		count++
	}
	require.Equal(t, 1000, count)
}

func TestBag_Clear(t *testing.T) {
	var b Bag[int]
	b.Add(1, 2, 3)
	b.Clear()

	var count int
	for range b.Seq() {
		count++
	}
	require.Equal(t, 0, count)
}

func TestBag_Add_Empty(t *testing.T) {
	var b Bag[int]
	b.Add(1)
	b.Add() // no-op

	var count int
	for range b.Seq() {
		count++
	}
	require.Equal(t, 1, count)
}

func TestBag_Seq_DrainsTheBag(t *testing.T) {
	var b Bag[int]
	b.Add(1, 2, 3)

	var first int
	for range b.Seq() {
		first++
	}
	require.Equal(t, 3, first)

	// Second Seq on the same bag should yield nothing because Seq drains.
	var second int
	for range b.Seq() {
		second++
	}
	require.Equal(t, 0, second)
}

func BenchmarkBag(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var b Bag[int]
		for i := range 1000 {
			b.Add(i)
		}
	}
}
