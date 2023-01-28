package storage

import (
	"testing"
)

func TestNewPaginationOptions(t *testing.T) {
	type test struct {
		size             int32
		token            string
		expectedPageSize int
		expectedFrom     string
	}
	tests := []test{
		{
			size:             0,
			token:            "",
			expectedPageSize: DefaultPageSize,
			expectedFrom:     "",
		},
		{
			size:             0,
			token:            "test",
			expectedPageSize: DefaultPageSize,
			expectedFrom:     "test",
		},
	}

	for _, test := range tests {
		opts := NewPaginationOptions(test.size, test.token)
		if opts.PageSize != test.expectedPageSize {
			t.Errorf("Expected PageSize: %d, got %d", test.expectedPageSize, opts.PageSize)
		}
		if opts.From != test.expectedFrom {
			t.Errorf("Expected From: %s, got %s", test.expectedFrom, opts.From)
		}
	}
}
