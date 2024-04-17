package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/cmd"
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

	t.Run("non_positive_dispatch_throttling_frequency", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 0,
			Threshold: 30,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("non_positive_dispatch_threshold", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.DispatchThrottling = DispatchThrottlingConfig{
			Enabled:   true,
			Frequency: 10 * time.Microsecond,
			Threshold: 0,
		}

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("unsupported_datastore_engine_specified", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Datastore.Engine = "mssql"

		err := cfg.Verify()
		require.Error(t, err)
	})

	t.Run("missing_engine_uri_for_postgres", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Datastore.Engine = cmd.Postgres
		cfg.Datastore.URI = ""

		err := cfg.Verify()
		require.ErrorContains(t, err, "engine requires URI")
	})

	t.Run("missing_engine_uri_for_mysql", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Datastore.Engine = cmd.MySQL
		cfg.Datastore.URI = ""

		err := cfg.Verify()
		require.ErrorContains(t, err, "engine requires URI")
	})

	t.Run("forbid_empty_engine", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Datastore.Engine = ""

		err := cfg.Verify()
		require.ErrorContains(t, err, "invalid datastore engine")
	})

	t.Run("forbid_memory_engine_with_uri_set", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Datastore.Engine = cmd.Memory
		cfg.Datastore.URI = "postgres://user:pass@host:port/datastore?opts"

		err := cfg.Verify()
		require.ErrorContains(t, err, "engine does not allow URI")
	})

	t.Run("allow_empty_engine_uri_for_memory", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Datastore.Engine = cmd.Memory
		cfg.Datastore.URI = ""

		err := cfg.Verify()
		require.NoError(t, err)
	})
}

func TestDefaultMaxConditionValuationCost(t *testing.T) {
	// check to make sure DefaultMaxConditionEvaluationCost never drops below an explicit 100, because
	// API compatibility can be impacted otherwise
	require.GreaterOrEqual(t, DefaultMaxConditionEvaluationCost, 100)
}
