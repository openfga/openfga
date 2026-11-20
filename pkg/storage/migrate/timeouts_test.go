package migrate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server/config"
)

func TestPingTimeouts(t *testing.T) {
	tests := []struct {
		name            string
		cfg             MigrationConfig
		wantPingTimeout time.Duration
		wantTimeout     time.Duration
	}{
		{
			name:            "unset",
			wantPingTimeout: config.DefaultDatastorePingTimeout,
			wantTimeout:     config.DefaultDatastorePingRetryMaxElapsedTime,
		},
		{
			// A negative Timeout makes the backoff stop after the first attempt, so it is kept.
			name:            "negative",
			cfg:             MigrationConfig{PingTimeout: -time.Second, Timeout: -time.Second},
			wantPingTimeout: config.DefaultDatastorePingTimeout,
			wantTimeout:     -time.Second,
		},
		{
			name:            "set",
			cfg:             MigrationConfig{PingTimeout: 3 * time.Second, Timeout: 5 * time.Second},
			wantPingTimeout: 3 * time.Second,
			wantTimeout:     5 * time.Second,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pingTimeout, timeout := pingTimeouts(test.cfg)
			require.Equal(t, test.wantPingTimeout, pingTimeout)
			require.Equal(t, test.wantTimeout, timeout)
		})
	}
}
