package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestVerifyConfig(t *testing.T) {
	t.Run("UpstreamTimeout_cannot_be_less_than_ListObjectsDeadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDeadline = 5 * time.Minute
		cfg.HTTP.UpstreamTimeout = 2 * time.Second

		err := cfg.Verify()
		require.EqualError(t, err, "config 'http.upstreamTimeout' (2s) cannot be lower than 'listObjectsDeadline' config (5m0s)")
	})

	t.Run("failing_to_set_http_cert_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.HTTP.TLS = &TLSConfig{
			Enabled: true,
			KeyPath: "some/path",
		}

		err := cfg.Verify()
		require.EqualError(t, err, "'http.tls.cert' and 'http.tls.key' configs must be set")
	})

	t.Run("failing_to_set_grpc_cert_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.GRPC.TLS = &TLSConfig{
			Enabled: true,
			KeyPath: "some/path",
		}

		err := cfg.Verify()
		require.EqualError(t, err, "'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
	})

	t.Run("failing_to_set_http_key_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.HTTP.TLS = &TLSConfig{
			Enabled:  true,
			CertPath: "some/path",
		}

		err := cfg.Verify()
		require.EqualError(t, err, "'http.tls.cert' and 'http.tls.key' configs must be set")
	})

	t.Run("failing_to_set_grpc_key_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.GRPC.TLS = &TLSConfig{
			Enabled:  true,
			CertPath: "some/path",
		}

		err := cfg.Verify()
		require.EqualError(t, err, "'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
	})

	t.Run("non_log_format", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Format = "notaformat"

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_log_level", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level = "notalevel"

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("invalid_log_timestamp_format", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.TimestampFormat = "notatimestampformat"

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("empty_request_duration_datastore_query_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDatastoreQueryCountBuckets = []string{}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_int_request_duration_datastore_query_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDatastoreQueryCountBuckets = []string{"12", "45a", "66"}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_request_duration_datastore_query_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDatastoreQueryCountBuckets = []string{"12", "-45", "66"}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_dispatch_throttling_duration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottlingConfig = DispatchThrottlingConfig{
			Enabled:              true,
			TimeTickerFrequency:  0,
			MediumPriorityShaper: 30,
			MediumPriorityLevel:  30,
			LowPriorityShaper:    30,
			LowPriorityLevel:     30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_dispatch_medium_priority_shaper", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottlingConfig = DispatchThrottlingConfig{
			Enabled:              true,
			TimeTickerFrequency:  10 * time.Microsecond,
			MediumPriorityShaper: 0,
			MediumPriorityLevel:  30,
			LowPriorityShaper:    30,
			LowPriorityLevel:     30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_dispatch_medium_priority_level", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottlingConfig = DispatchThrottlingConfig{
			Enabled:              true,
			TimeTickerFrequency:  10 * time.Microsecond,
			MediumPriorityShaper: 30,
			MediumPriorityLevel:  0,
			LowPriorityShaper:    30,
			LowPriorityLevel:     30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_dispatch_low_priority_level", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottlingConfig = DispatchThrottlingConfig{
			Enabled:              true,
			TimeTickerFrequency:  10 * time.Microsecond,
			MediumPriorityShaper: 30,
			MediumPriorityLevel:  30,
			LowPriorityShaper:    30,
			LowPriorityLevel:     0,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_dispatch_low_shaper_level", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottlingConfig = DispatchThrottlingConfig{
			Enabled:              true,
			TimeTickerFrequency:  10 * time.Microsecond,
			MediumPriorityShaper: 30,
			MediumPriorityLevel:  30,
			LowPriorityShaper:    0,
			LowPriorityLevel:     30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("low_level_must_not_be_smaller_than_medium_level", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottlingConfig = DispatchThrottlingConfig{
			Enabled:              true,
			TimeTickerFrequency:  10 * time.Microsecond,
			MediumPriorityShaper: 30,
			MediumPriorityLevel:  30,
			LowPriorityShaper:    30,
			LowPriorityLevel:     29,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})
}

func TestDefaultMaxConditionValuationCost(t *testing.T) {
	// check to make sure DefaultMaxConditionEvaluationCost never drops below an explicit 100, because
	// API compatibility can be impacted otherwise
	require.GreaterOrEqual(t, DefaultMaxConditionEvaluationCost, 100)
}
