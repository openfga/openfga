package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPaginationOptions(t *testing.T) {
	type test struct {
		_name            string
		size             int32
		token            string
		expectedPageSize int
		expectedFrom     string
	}
	tests := []test{
		{
			_name:            "zero_token",
			size:             0,
			token:            "",
			expectedPageSize: DefaultPageSize,
			expectedFrom:     "",
		},
		{
			_name:            "non-zero_token",
			size:             0,
			token:            "test",
			expectedPageSize: DefaultPageSize,
			expectedFrom:     "test",
		},
		{
			_name:            "non-zero_size",
			size:             14,
			token:            "",
			expectedPageSize: 14,
			expectedFrom:     "",
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			opts := NewPaginationOptions(test.size, test.token)
			require.Equal(t, test.expectedPageSize, opts.PageSize)
			require.Equal(t, test.expectedFrom, opts.From)
		})
	}
}
