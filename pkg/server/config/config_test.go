package config

import (
	"bytes"
	"io"
	"os"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestVerifyConfig(t *testing.T) {
	t.Run("UpstreamTimeout_cannot_be_less_than_ListObjectsDeadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDeadline = 5 * time.Minute
		cfg.RequestTimeout = 0
		cfg.HTTP.UpstreamTimeout = 2 * time.Second

		err := cfg.Verify()
		require.EqualError(t, err, "configured request timeout (2s) cannot be lower than 'listObjectsDeadline' config (5m0s)")
	})
	t.Run("UpstreamTimeout_cannot_be_less_than_ListUsersDeadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDeadline = 2 * time.Second
		cfg.ListUsersDeadline = 5 * time.Minute
		cfg.RequestTimeout = 0
		cfg.HTTP.UpstreamTimeout = 2 * time.Second

		err := cfg.Verify()
		require.EqualError(t, err, "configured request timeout (2s) cannot be lower than 'listUsersDeadline' config (5m0s)")
	})

	t.Run("maxConcurrentReadsForListUsers_not_zero", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxConcurrentReadsForListUsers = 0

		err := cfg.Verify()
		require.EqualError(t, err, "config 'maxConcurrentReadsForListUsers' cannot be 0")
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
		require.ErrorContains(t, err, "'listObjectsDispatchThrottling.frequency' must be non-negative time duration")
	})

	t.Run("non_positive_list_users_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listUsersDispatchThrottling.frequency' must be non-negative time duration")
	})

	t.Run("non_positive_list_objects_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listObjectsDispatchThrottling.threshold' must be non-negative integer")
	})

	t.Run("non_positive_list_users_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listUsersDispatchThrottling.threshold' must be non-negative integer")
	})

	t.Run("dispatch_throttling_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("list_objects_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.Verify()
		require.ErrorContains(t, err, "'listObjectsDispatchThrottling.threshold' must be less than or equal to 'listObjectsDispatchThrottling.maxThreshold'")
	})

	t.Run("list_users_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.Verify()
		require.ErrorContains(t, err, "'listUsersDispatchThrottling.threshold' must be less than or equal to 'listUsersDispatchThrottling.maxThreshold'")
	})

	t.Run("non_positive_check_datastore_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDatastoreThrottle = DatastoreThrottleConfig{
			Enabled:   true,
			Threshold: 0,
			Duration:  100,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'checkDatastoreThrottler.threshold' must be greater than zero")
	})

	t.Run("non_positive_check_datastore_duration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDatastoreThrottle = DatastoreThrottleConfig{
			Enabled:   true,
			Threshold: 100,
			Duration:  0,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'checkDatastoreThrottler.duration' must be greater than zero")
	})

	t.Run("non_positive_list_objects_datastore_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDatastoreThrottle = DatastoreThrottleConfig{
			Enabled:   true,
			Threshold: 0,
			Duration:  100,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listObjectsDatastoreThrottler.threshold' must be greater than zero")
	})

	t.Run("non_positive_list_objects_datastore_duration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDatastoreThrottle = DatastoreThrottleConfig{
			Enabled:   true,
			Threshold: 100,
			Duration:  0,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listObjectsDatastoreThrottler.duration' must be greater than zero")
	})

	t.Run("non_positive_list_users_datastore_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDatastoreThrottle = DatastoreThrottleConfig{
			Enabled:   true,
			Threshold: 0,
			Duration:  100,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listUsersDatastoreThrottler.threshold' must be greater than zero")
	})

	t.Run("non_positive_list_users_datastore_duration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDatastoreThrottle = DatastoreThrottleConfig{
			Enabled:   true,
			Threshold: 100,
			Duration:  0,
		}

		err := cfg.Verify()
		require.ErrorContains(t, err, "'listUsersDatastoreThrottler.duration' must be greater than zero")
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

	t.Run("negative_list_users_deadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = 3 * time.Second
		cfg.ListUsersDeadline = -4 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("list_objects_deadline_request_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 500 * time.Millisecond
		cfg.ListObjectsDeadline = 4 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("list_users_deadline_request_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 500 * time.Millisecond
		cfg.ListUsersDeadline = 4 * time.Second

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("cache_query_cache", func(t *testing.T) {
		t.Run("enable_but_ttl_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckQueryCache.Enabled = true
			cfg.CheckQueryCache.TTL = 0
			err := cfg.Verify()
			require.Error(t, err)
		})
		t.Run("enable_but_ttl_negative", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckQueryCache.Enabled = true
			cfg.CheckQueryCache.TTL = -2 * time.Second
			err := cfg.Verify()
			require.Error(t, err)
		})
		t.Run("enable_and_ttl_positive", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckQueryCache.Enabled = true
			cfg.CheckQueryCache.TTL = 2 * time.Second
			err := cfg.Verify()
			require.NoError(t, err)
		})
		t.Run("disable_and_ttl_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckQueryCache.Enabled = false
			cfg.CheckQueryCache.TTL = 0
			err := cfg.Verify()
			require.NoError(t, err)
		})
	})

	t.Run("check_iterator_cache", func(t *testing.T) {
		t.Run("enable_but_ttl_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckIteratorCache.Enabled = true
			cfg.CheckIteratorCache.TTL = 0
			cfg.CheckIteratorCache.MaxResults = 1000
			err := cfg.Verify()
			require.Error(t, err)
		})

		t.Run("enable_but_ttl_negative", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckIteratorCache.Enabled = true
			cfg.CheckIteratorCache.TTL = -2 * time.Second
			cfg.CheckIteratorCache.MaxResults = 1000
			err := cfg.Verify()
			require.Error(t, err)
		})

		t.Run("enable_but_max_results_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckIteratorCache.Enabled = true
			cfg.CheckIteratorCache.TTL = 2 * time.Second
			cfg.CheckIteratorCache.MaxResults = 0
			err := cfg.Verify()
			require.Error(t, err)
		})

		t.Run("disable_but_ttl_and_max_results_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckIteratorCache.Enabled = false
			cfg.CheckIteratorCache.TTL = 0
			cfg.CheckIteratorCache.MaxResults = 0
			err := cfg.Verify()
			require.NoError(t, err)
		})

		t.Run("enable_and_ttl_and_max_results_positive", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CheckIteratorCache.Enabled = true
			cfg.CheckIteratorCache.TTL = 10 * time.Second
			cfg.CheckIteratorCache.MaxResults = 10000
			err := cfg.Verify()
			require.NoError(t, err)
		})
	})

	t.Run("list_objects_iterator_cache", func(t *testing.T) {
		t.Run("enable_but_ttl_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ListObjectsIteratorCache.Enabled = true
			cfg.ListObjectsIteratorCache.TTL = 0
			cfg.ListObjectsIteratorCache.MaxResults = 1000
			err := cfg.Verify()
			require.Error(t, err)
		})

		t.Run("enable_but_ttl_negative", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ListObjectsIteratorCache.Enabled = true
			cfg.ListObjectsIteratorCache.TTL = -2 * time.Second
			cfg.ListObjectsIteratorCache.MaxResults = 1000
			err := cfg.Verify()
			require.Error(t, err)
		})

		t.Run("enable_but_max_results_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ListObjectsIteratorCache.Enabled = true
			cfg.ListObjectsIteratorCache.TTL = 2 * time.Second
			cfg.ListObjectsIteratorCache.MaxResults = 0
			err := cfg.Verify()
			require.Error(t, err)
		})

		t.Run("disable_but_ttl_and_max_results_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ListObjectsIteratorCache.Enabled = false
			cfg.ListObjectsIteratorCache.TTL = 0
			cfg.ListObjectsIteratorCache.MaxResults = 0
			err := cfg.Verify()
			require.NoError(t, err)
		})

		t.Run("enable_and_ttl_and_max_results_positive", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.ListObjectsIteratorCache.Enabled = true
			cfg.ListObjectsIteratorCache.TTL = 10 * time.Second
			cfg.ListObjectsIteratorCache.MaxResults = 10000
			err := cfg.Verify()
			require.NoError(t, err)
		})
	})

	t.Run("cache_controller", func(t *testing.T) {
		t.Run("enable_but_ttl_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CacheController.Enabled = true
			cfg.CacheController.TTL = 0
			err := cfg.Verify()
			require.Error(t, err)
		})
		t.Run("enable_but_ttl_negative", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CacheController.Enabled = true
			cfg.CacheController.TTL = -2 * time.Second
			err := cfg.Verify()
			require.Error(t, err)
		})
		t.Run("enable_and_ttl_positive", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CacheController.Enabled = true
			cfg.CacheController.TTL = 2 * time.Second
			err := cfg.Verify()
			require.NoError(t, err)
		})
		t.Run("disable_and_ttl_zero", func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.CacheController.Enabled = false
			cfg.CacheController.TTL = 0
			err := cfg.Verify()
			require.NoError(t, err)
		})
	})

	t.Run("prints_warning_when_log_level_is_none", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level = "none"

		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			// Restore the original stdout
			os.Stdout = oldStdout
			w.Close()
		}()

		cfg.Verify()
		w.Close()

		// Read the captured output
		var buf bytes.Buffer
		io.Copy(&buf, r)

		require.Contains(t, buf.String(), "WARNING: Logging is not enabled. It is highly recommended to enable logging in production environments to avoid masking attacker operations.")
	})

	t.Run("does_not_print_warning_when_log_level_is_not_none", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level = "info"

		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			// Restore the original stdout
			os.Stdout = oldStdout
			w.Close()
		}()

		cfg.Verify()
		w.Close()

		// Read the captured output
		var buf bytes.Buffer
		io.Copy(&buf, r)

		require.NotContains(t, buf.String(), "WARNING: Logging is not enabled. It is highly recommended to enable logging in production environments to avoid masking attacker operations.")
	})
}

func TestVerifyServerSettings(t *testing.T) {
	t.Run("UpstreamTimeout_cannot_be_less_than_ListObjectsDeadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDeadline = 5 * time.Minute
		cfg.RequestTimeout = 0
		cfg.HTTP.UpstreamTimeout = 2 * time.Second

		err := cfg.VerifyServerSettings()
		require.EqualError(t, err, "configured request timeout (2s) cannot be lower than 'listObjectsDeadline' config (5m0s)")
	})
	t.Run("UpstreamTimeout_cannot_be_less_than_ListUsersDeadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDeadline = 2 * time.Second
		cfg.ListUsersDeadline = 5 * time.Minute
		cfg.RequestTimeout = 0
		cfg.HTTP.UpstreamTimeout = 2 * time.Second

		err := cfg.VerifyServerSettings()
		require.EqualError(t, err, "configured request timeout (2s) cannot be lower than 'listUsersDeadline' config (5m0s)")
	})

	t.Run("maxConcurrentReadsForListUsers_not_zero", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxConcurrentReadsForListUsers = 0

		err := cfg.VerifyServerSettings()
		require.EqualError(t, err, "config 'maxConcurrentReadsForListUsers' cannot be 0")
	})

	t.Run("empty_request_duration_datastore_query_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDatastoreQueryCountBuckets = []string{}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("non_int_request_duration_datastore_query_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDatastoreQueryCountBuckets = []string{"12", "45a", "66"}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("negative_request_duration_datastore_query_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDatastoreQueryCountBuckets = []string{"12", "-45", "66"}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("empty_request_duration_dispatch_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDispatchCountBuckets = []string{}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("non_int_request_duration_dispatch_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDispatchCountBuckets = []string{"12", "45a", "66"}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("negative_request_duration_dispatch_count_buckets", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestDurationDispatchCountBuckets = []string{"12", "-45", "66"}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("non_positive_check_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("non_positive_check_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("non_positive_list_objects_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.VerifyServerSettings()
		require.ErrorContains(t, err, "'listObjectsDispatchThrottling.frequency' must be non-negative time duration")
	})

	t.Run("non_positive_list_users_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.VerifyServerSettings()
		require.ErrorContains(t, err, "'listUsersDispatchThrottling.frequency' must be non-negative time duration")
	})

	t.Run("non_positive_list_objects_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.VerifyServerSettings()
		require.ErrorContains(t, err, "'listObjectsDispatchThrottling.threshold' must be non-negative integer")
	})

	t.Run("non_positive_list_users_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.VerifyServerSettings()
		require.ErrorContains(t, err, "'listUsersDispatchThrottling.threshold' must be non-negative integer")
	})

	t.Run("dispatch_throttling_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.CheckDispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("list_objects_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListObjectsDispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.VerifyServerSettings()
		require.ErrorContains(t, err, "'listObjectsDispatchThrottling.threshold' must be less than or equal to 'listObjectsDispatchThrottling.maxThreshold'")
	})

	t.Run("list_users_threshold_larger_than_max_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.ListUsersDispatchThrottling = DispatchThrottlingConfig{
			Enabled:      true,
			Frequency:    10 * time.Microsecond,
			Threshold:    30,
			MaxThreshold: 29,
		}
		err := cfg.VerifyServerSettings()
		require.ErrorContains(t, err, "'listUsersDispatchThrottling.threshold' must be less than or equal to 'listUsersDispatchThrottling.maxThreshold'")
	})

	t.Run("negative_upstream_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = -3 * time.Second
		cfg.ListObjectsDeadline = -4 * time.Second

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("negative_list_objects_deadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = 3 * time.Second
		cfg.ListObjectsDeadline = -4 * time.Second

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("negative_list_users_deadline", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = 3 * time.Second
		cfg.ListUsersDeadline = -4 * time.Second

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("list_objects_deadline_request_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 500 * time.Millisecond
		cfg.ListObjectsDeadline = 4 * time.Second

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("list_users_deadline_request_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 500 * time.Millisecond
		cfg.ListUsersDeadline = 4 * time.Second

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})

	t.Run("does_not_print_warning_when_log_level_is_not_none", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level = "info"

		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			// Restore the original stdout
			os.Stdout = oldStdout
			w.Close()
		}()

		cfg.VerifyServerSettings()
		w.Close()

		// Read the captured output
		var buf bytes.Buffer
		io.Copy(&buf, r)

		require.NotContains(t, buf.String(), "WARNING: Logging is not enabled. It is highly recommended to enable logging in production environments to avoid masking attacker operations.")
	})

	t.Run("success_when_experimentals_are_correct", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Experimentals = []string{"enable-check-optimizations", "enable-list-objects-optimizations"}

		err := cfg.VerifyServerSettings()
		require.NoError(t, err)
	})

	t.Run("verifies_experimentals_are_correct", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Experimentals = []string{"invalid-experimental"}

		err := cfg.VerifyServerSettings()
		require.Error(t, err)
	})
}

func TestVerifyBinarySettings(t *testing.T) {
	t.Run("failing_to_set_http_cert_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.HTTP.TLS = &TLSConfig{
			Enabled: true,
			KeyPath: "some/path",
		}

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "'http.tls.cert' and 'http.tls.key' configs must be set")
	})

	t.Run("failing_to_set_grpc_cert_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.GRPC.TLS = &TLSConfig{
			Enabled: true,
			KeyPath: "some/path",
		}

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
	})

	t.Run("failing_to_set_http_key_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.HTTP.TLS = &TLSConfig{
			Enabled:  true,
			CertPath: "some/path",
		}

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "'http.tls.cert' and 'http.tls.key' configs must be set")
	})

	t.Run("failing_to_set_grpc_key_path_will_not_allow_server_to_start", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.GRPC.TLS = &TLSConfig{
			Enabled:  true,
			CertPath: "some/path",
		}

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
	})

	t.Run("non_log_format", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Format = "notaformat"

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "config 'log.format' must be one of ['text', 'json']")
	})

	t.Run("invalid_log_timestamp_format", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.TimestampFormat = "notatimestampformat"

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "config 'log.TimestampFormat' must be one of ['Unix', 'ISO8601']")
	})

	t.Run("negative_request_timeout_duration", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = -1 * time.Second

		err := cfg.VerifyBinarySettings()
		require.EqualError(t, err, "requestTimeout must be a non-negative time duration")
	})

	t.Run("negative_http_upstream_timeout", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.RequestTimeout = 0
		cfg.HTTP.Enabled = true
		cfg.HTTP.UpstreamTimeout = -1 * time.Second

		err := cfg.VerifyBinarySettings()
		require.Error(t, err)
		require.Contains(t, err.Error(), "http.upstreamTimeout must be a non-negative time duration")
	})

	t.Run("invalid_log_level", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level = "invalid_level"

		err := cfg.VerifyBinarySettings()
		require.Error(t, err)
		require.Contains(t, err.Error(), "config 'log.level' must be one of ['none', 'debug', 'info', 'warn', 'error', 'panic', 'fatal']")
	})

	t.Run("playground_enabled_without_http", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Playground.Enabled = true
		cfg.HTTP.Enabled = false

		err := cfg.VerifyBinarySettings()
		require.Error(t, err)
		require.Contains(t, err.Error(), "the HTTP server must be enabled to run the openfga playground")
	})

	t.Run("playground_enabled_with_unsupported_authn", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Playground.Enabled = true
		cfg.HTTP.Enabled = true
		cfg.Authn.Method = "unsupported"

		err := cfg.VerifyBinarySettings()
		require.Error(t, err)
		require.Contains(t, err.Error(), "the playground only supports authn methods 'none' and 'preshared'")
	})

	t.Run("playground_enabled_with_supported_authn", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Playground.Enabled = true
		cfg.HTTP.Enabled = true
		cfg.Authn.Method = "none"

		err := cfg.VerifyBinarySettings()
		require.NoError(t, err)

		cfg.Authn.Method = "preshared"
		err = cfg.VerifyBinarySettings()
		require.NoError(t, err)
	})

	t.Run("prints_warning_when_log_level_is_none", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level = "none"
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		defer func() {
			// Restore the original stdout
			os.Stdout = oldStdout
			w.Close()
		}()

		cfg.VerifyBinarySettings()
		w.Close()
		// Read the captured output
		var buf bytes.Buffer

		io.Copy(&buf, r)
		require.Contains(t, buf.String(), "WARNING: Logging is not enabled. It is highly recommended to enable logging in production environments to avoid masking attacker operations.")
	})
}

func TestDefaultMaxConditionValuationCost(t *testing.T) {
	// check to make sure DefaultMaxConditionEvaluationCost never drops below an explicit 100, because
	// API compatibility can be impacted otherwise
	t.Run("max_condition_evaluation_cost_too_low", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxConditionEvaluationCost = 99

		err := cfg.Verify()
		require.Error(t, err)
	})
	t.Run("max_condition_evaluation_cost_valid", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxConditionEvaluationCost = uint64(120)
		viper.Set("maxConditionEvaluationCost", uint64(120))

		err := cfg.Verify()
		require.NoError(t, err)
	})
	require.Equal(t, uint64(120), MaxConditionEvaluationCost())
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
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			timeout := DefaultContextTimeout(&test.config)
			require.Equal(t, test.expectedContextTimeout, timeout)
		})
	}
}
