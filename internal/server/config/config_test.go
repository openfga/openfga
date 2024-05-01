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

	t.Run("empty_request_duration_dispatch_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDispatchCountBuckets = []string{}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_int_request_duration_dispatch_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDispatchCountBuckets = []string{"12", "45a", "66"}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_request_duration_dispatch_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDispatchCountBuckets = []string{"12", "-45", "66"}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_positive_check_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_positive_check_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_positive_list_objects_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_positive_list_objects_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("dispatch_throttling_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_request_timeout_duration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = -2 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_upstream_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = -3 * time.Second
		cfg.ListObjectsDeadline = -4 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("negative_list_objects_deadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = 3 * time.Second
		cfg.ListObjectsDeadline = -4 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("list_objects_deadline_request_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 1 * time.Second
		cfg.ListObjectsDeadline = 4 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})
}

func TestDefaultMaxConditionValuationCost(t *testing.T) {
	// check to make sure DefaultMaxConditionEvaluationCost never drops below an explicit 100, because
	// API compatibility can be impacted otherwise
	require.GreaterOrEqual(t, DefaultMaxConditionEvaluationCost, 100)
}

func TestDefaultContextTimeout(t *testing.T) {
	var testCases = map[string]struct {
		config                 Config
		expectedContextTimeout time.Duration
	}{
		"request_timeout_provided": {
			config: Config{
				RequestTimeout: 5 * time.Second,
				HTTP: HTTPConfig{
					Enabled:         true,
					UpstreamTimeout: 1 * time.Second,
				},
			},
			expectedContextTimeout: 5*time.Second + additionalUpstreamTimeout,
		},
		"only_http_config_timeout": {
			config: Config{
				HTTP: HTTPConfig{
					Enabled:         true,
					UpstreamTimeout: 1 * time.Second,
				},
			},
			expectedContextTimeout: 1 * time.Second,
		},
		"http_not_enable": {
			config: Config{
				HTTP: HTTPConfig{
					Enabled:         false,
					UpstreamTimeout: 1 * time.Second,
				},
			},
			expectedContextTimeout: 0,
		},
	}
	for name, test := range testCases {
		test := test
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			timeout := DefaultContextTimeout(&test.config)
			require.Equal(t, test.expectedContextTimeout, timeout)
		})
	}
}
