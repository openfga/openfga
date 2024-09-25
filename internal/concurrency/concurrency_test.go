package concurrency

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrySendThroughChannel(t *testing.T) {
	var testcases = map[string]struct {
		ctxCancelled bool
		message      struct{}
	}{
		`ctx_cancel`: {
			ctxCancelled: true,
			message:      struct{}{},
		},
		`no_ctx_cancel`: {
			ctxCancelled: false,
			message:      struct{}{},
		},
	}

	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			var channel chan struct{}
			ctx := context.Background()

			var cancelFunc context.CancelFunc
			if tc.ctxCancelled {
				channel = make(chan struct{})
				ctx, cancelFunc = context.WithCancel(ctx)
				cancelFunc()
			} else {
				channel = make(chan struct{}, 1)
			}
			TrySendThroughChannel(ctx, tc.message, channel)
			if tc.ctxCancelled {
				close(channel)
				_, ok := <-channel
				require.False(t, ok)
			} else {
				element, ok := <-channel
				require.True(t, ok)
				require.NotNil(t, element)
			}
		})
	}
}
