package run

import (
	"errors"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
)

func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../../..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

func TestDefaultConfig(t *testing.T) {
	cfg, err := ReadConfig()
	require.NoError(t, err)

	_, basepath, _, _ := runtime.Caller(0)
	jsonSchema, err := os.ReadFile(path.Join(filepath.Dir(basepath), "..", "..", ".config-schema.json"))
	require.NoError(t, err)

	res := gjson.ParseBytes(jsonSchema)

	val := res.Get("properties.datastore.properties.engine.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Datastore.Engine)

	val = res.Get("properties.datastore.properties.maxCacheSize.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxCacheSize)

	val = res.Get("properties.datastore.properties.maxTypesystemCacheSize.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxTypesystemCacheSize)

	val = res.Get("properties.datastore.properties.maxIdleConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxIdleConns)

	val = res.Get("properties.datastore.properties.minIdleConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MinIdleConns)

	val = res.Get("properties.datastore.properties.maxOpenConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MaxOpenConns)

	val = res.Get("properties.datastore.properties.minOpenConns.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Datastore.MinOpenConns)

	val = res.Get("properties.datastore.properties.connMaxIdleTime.default")
	require.True(t, val.Exists())

	val = res.Get("properties.datastore.properties.connMaxLifetime.default")
	require.True(t, val.Exists())

	val = res.Get("properties.datastore.properties.metrics.properties.enabled.default")
	require.True(t, val.Exists())
	require.False(t, val.Bool())

	val = res.Get("properties.grpc.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.GRPC.Addr)

	val = res.Get("properties.http.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.HTTP.Enabled)

	val = res.Get("properties.http.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.HTTP.Addr)

	val = res.Get("properties.playground.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Playground.Enabled)

	val = res.Get("properties.playground.properties.port.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.Playground.Port)

	val = res.Get("properties.profiler.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Profiler.Enabled)

	val = res.Get("properties.profiler.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Profiler.Addr)

	val = res.Get("properties.authn.properties.method.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Authn.Method)

	val = res.Get("properties.log.properties.format.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Log.Format)

	val = res.Get("properties.maxTuplesPerWrite.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxTuplesPerWrite)

	val = res.Get("properties.maxTypesPerAuthorizationModel.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxTypesPerAuthorizationModel)

	val = res.Get("properties.maxConcurrentReadsForListObjects.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentReadsForListObjects)

	val = res.Get("properties.maxConcurrentReadsForCheck.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentReadsForCheck)

	val = res.Get("properties.maxConcurrentChecksPerBatchCheck.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentChecksPerBatchCheck)

	val = res.Get("properties.maxChecksPerBatchCheck.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxChecksPerBatchCheck)

	val = res.Get("properties.maxConditionEvaluationCost.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Uint(), cfg.MaxConditionEvaluationCost)

	val = res.Get("properties.maxConcurrentReadsForListUsers.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.MaxConcurrentReadsForListUsers)

	val = res.Get("properties.changelogHorizonOffset.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ChangelogHorizonOffset)

	val = res.Get("properties.resolveNodeBreadthLimit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ResolveNodeBreadthLimit)

	val = res.Get("properties.resolveNodeLimit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ResolveNodeLimit)

	val = res.Get("properties.grpc.properties.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.GRPC.TLS.Enabled)

	val = res.Get("properties.http.properties.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.HTTP.TLS.Enabled)

	val = res.Get("properties.listObjectsDeadline.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsDeadline.String())

	val = res.Get("properties.listObjectsMaxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsMaxResults)

	val = res.Get("properties.listUsersDeadline.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListUsersDeadline.String())

	val = res.Get("properties.listUsersMaxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersMaxResults)

	val = res.Get("properties.experimentals.default")
	require.True(t, val.Exists())
	require.Len(t, cfg.Experimentals, len(val.Array()))

	val = res.Get("properties.metrics.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Metrics.Enabled)

	val = res.Get("properties.metrics.properties.addr.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Metrics.Addr)

	val = res.Get("properties.metrics.properties.enableRPCHistograms.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Metrics.EnableRPCHistograms)

	val = res.Get("properties.trace.properties.serviceName.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Trace.ServiceName)

	val = res.Get("properties.trace.properties.resourceAttributes.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Trace.ResourceAttributes)

	val = res.Get("properties.trace.properties.otlp.properties.endpoint.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.Trace.OTLP.Endpoint)

	val = res.Get("properties.trace.properties.otlp.properties.tls.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.Trace.OTLP.TLS.Enabled)

	val = res.Get("properties.checkCache.properties.limit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckCache.Limit)

	val = res.Get("properties.checkQueryCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckQueryCache.Enabled)

	val = res.Get("properties.checkQueryCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckQueryCache.TTL.String())

	val = res.Get("properties.checkIteratorCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckIteratorCache.Enabled)

	val = res.Get("properties.checkIteratorCache.properties.maxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckIteratorCache.MaxResults)

	val = res.Get("properties.checkIteratorCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckIteratorCache.TTL.String())

	val = res.Get("properties.listObjectsIteratorCache.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListObjectsIteratorCache.Enabled)

	val = res.Get("properties.listObjectsIteratorCache.properties.maxResults.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsIteratorCache.MaxResults)

	val = res.Get("properties.listObjectsIteratorCache.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsIteratorCache.TTL.String())

	val = res.Get("properties.cacheController.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CacheController.Enabled)

	val = res.Get("properties.cacheController.properties.ttl.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CacheController.TTL.String())

	val = res.Get("properties.sharedIterator.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.SharedIterator.Enabled)

	val = res.Get("properties.sharedIterator.properties.limit.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.SharedIterator.Limit)

	val = res.Get("properties.requestDurationDatastoreQueryCountBuckets.default")
	require.True(t, val.Exists())
	require.Len(t, cfg.RequestDurationDatastoreQueryCountBuckets, len(val.Array()))
	for index, arrayVal := range val.Array() {
		require.Equal(t, arrayVal.String(), cfg.RequestDurationDatastoreQueryCountBuckets[index])
	}

	val = res.Get("properties.requestDurationDispatchCountBuckets.default")
	require.True(t, val.Exists())
	require.Len(t, cfg.RequestDurationDispatchCountBuckets, len(val.Array()))
	for index, arrayVal := range val.Array() {
		require.Equal(t, arrayVal.String(), cfg.RequestDurationDispatchCountBuckets[index])
	}

	val = res.Get("properties.contextPropagationToDatastore.default")
	require.True(t, val.Exists())
	require.False(t, val.Bool())

	val = res.Get("properties.checkDispatchThrottling.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.CheckDispatchThrottling.Enabled)

	val = res.Get("properties.checkDispatchThrottling.properties.frequency.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckDispatchThrottling.Frequency.String())

	val = res.Get("properties.checkDispatchThrottling.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckDispatchThrottling.Threshold)

	val = res.Get("properties.checkDispatchThrottling.properties.maxThreshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckDispatchThrottling.MaxThreshold)

	val = res.Get("properties.listObjectsDispatchThrottling.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListObjectsDispatchThrottling.Enabled)

	val = res.Get("properties.listObjectsDispatchThrottling.properties.frequency.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsDispatchThrottling.Frequency.String())

	val = res.Get("properties.listObjectsDispatchThrottling.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsDispatchThrottling.Threshold)

	val = res.Get("properties.listObjectsDispatchThrottling.properties.maxThreshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsDispatchThrottling.MaxThreshold)

	val = res.Get("properties.listUsersDispatchThrottling.properties.enabled.default")
	require.True(t, val.Exists())
	require.Equal(t, val.Bool(), cfg.ListUsersDispatchThrottling.Enabled)

	val = res.Get("properties.listUsersDispatchThrottling.properties.frequency.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListUsersDispatchThrottling.Frequency.String())

	val = res.Get("properties.listUsersDispatchThrottling.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersDispatchThrottling.Threshold)

	val = res.Get("properties.listUsersDispatchThrottling.properties.maxThreshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersDispatchThrottling.MaxThreshold)

	val = res.Get("properties.checkDatastoreThrottle.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.CheckDatastoreThrottle.Threshold)

	val = res.Get("properties.checkDatastoreThrottle.properties.duration.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.CheckDatastoreThrottle.Duration.String())

	val = res.Get("properties.listObjectsDatastoreThrottle.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListObjectsDatastoreThrottle.Threshold)

	val = res.Get("properties.listObjectsDatastoreThrottle.properties.duration.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListObjectsDatastoreThrottle.Duration.String())

	val = res.Get("properties.listUsersDatastoreThrottle.properties.threshold.default")
	require.True(t, val.Exists())
	require.EqualValues(t, val.Int(), cfg.ListUsersDatastoreThrottle.Threshold)

	val = res.Get("properties.listUsersDatastoreThrottle.properties.duration.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.ListUsersDatastoreThrottle.Duration.String())

	val = res.Get("properties.requestTimeout.default")
	require.True(t, val.Exists())
	require.Equal(t, val.String(), cfg.RequestTimeout.String())
}

func TestRunCommandNoConfigDefaultValues(t *testing.T) {
	util.PrepareTempConfigDir(t)
	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Empty(t, viper.GetString(datastoreEngineFlag))
		require.Empty(t, viper.GetString(datastoreURIFlag))
		require.False(t, viper.GetBool("check-query-cache-enabled"))
		require.False(t, viper.GetBool("context-propagation-to-datastore"))
		require.Equal(t, uint32(0), viper.GetUint32("check-query-cache-limit"))
		require.Equal(t, 0*time.Second, viper.GetDuration("check-query-cache-ttl"))
		require.Empty(t, viper.GetIntSlice("request-duration-datastore-query-count-buckets"))
		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestRunCommandConfigFileValuesAreParsed(t *testing.T) {
	config := `datastore:
    engine: postgres
    uri: postgres://postgres:password@127.0.0.1:5432/postgres
`
	util.PrepareTempConfigFile(t, config)

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:password@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestParseConfig(t *testing.T) {
	config := `checkQueryCache:
    enabled: true
    TTL: 5s
requestDurationDatastoreQueryCountBuckets: [33,44]
requestDurationDispatchCountBuckets: [32,42]
`
	util.PrepareTempConfigFile(t, config)

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		return nil
	}
	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())

	cfg, err := ReadConfig()
	require.NoError(t, err)
	require.True(t, cfg.CheckQueryCache.Enabled)
	require.Equal(t, 5*time.Second, cfg.CheckQueryCache.TTL)
	require.Equal(t, []string{"33", "44"}, cfg.RequestDurationDatastoreQueryCountBuckets)
	require.Equal(t, []string{"32", "42"}, cfg.RequestDurationDispatchCountBuckets)
}

func TestRunCommandConfigIsMerged(t *testing.T) {
	config := `datastore:
    engine: postgres
`
	util.PrepareTempConfigFile(t, config)

	t.Setenv("OPENFGA_DATASTORE_URI", "postgres://postgres:PASS2@127.0.0.1:5432/postgres")
	t.Setenv("OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "1")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_ENABLED", "true")
	t.Setenv("OPENFGA_CHECK_CACHE_LIMIT", "33")
	t.Setenv("OPENFGA_CHECK_QUERY_CACHE_TTL", "5s")
	t.Setenv("OPENFGA_CACHE_CONTROLLER_ENABLED", "true")
	t.Setenv("OPENFGA_CACHE_CONTROLLER_TTL", "4s")
	t.Setenv("OPENFGA_REQUEST_DURATION_DATASTORE_QUERY_COUNT_BUCKETS", "33 44")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_ENABLED", "true")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_FREQUENCY", "1ms")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_THRESHOLD", "120")
	t.Setenv("OPENFGA_DISPATCH_THROTTLING_MAX_THRESHOLD", "130")
	t.Setenv("OPENFGA_MAX_CONDITION_EVALUATION_COST", "120")
	t.Setenv("OPENFGA_ACCESS_CONTROL_ENABLED", "true")
	t.Setenv("OPENFGA_ACCESS_CONTROL_STORE_ID", "12345")
	t.Setenv("OPENFGA_ACCESS_CONTROL_MODEL_ID", "67891")
	t.Setenv("OPENFGA_CONTEXT_PROPAGATION_TO_DATASTORE", "true")
	t.Setenv("OPENFGA_SHARED_ITERATOR_ENABLED", "true")
	t.Setenv("OPENFGA_SHARED_ITERATOR_LIMIT", "950")

	runCmd := NewRunCommand()
	runCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		require.Equal(t, "postgres", viper.GetString(datastoreEngineFlag))
		require.Equal(t, "postgres://postgres:PASS2@127.0.0.1:5432/postgres", viper.GetString(datastoreURIFlag))
		require.Equal(t, "1", viper.GetString("max-types-per-authorization-model"))
		require.True(t, viper.GetBool("check-query-cache-enabled"))
		require.Equal(t, uint32(33), viper.GetUint32("check-cache-limit"))
		require.Equal(t, 5*time.Second, viper.GetDuration("check-query-cache-ttl"))
		require.True(t, viper.GetBool("cache-controller-enabled"))
		require.Equal(t, 4*time.Second, viper.GetDuration("cache-controller-ttl"))

		require.Equal(t, []string{"33", "44"}, viper.GetStringSlice("request-duration-datastore-query-count-buckets"))
		require.True(t, viper.GetBool("dispatch-throttling-enabled"))
		require.Equal(t, "1ms", viper.GetString("dispatch-throttling-frequency"))
		require.Equal(t, "120", viper.GetString("dispatch-throttling-threshold"))
		require.Equal(t, "130", viper.GetString("dispatch-throttling-max-threshold"))
		require.Equal(t, "120", viper.GetString("max-condition-evaluation-cost"))
		require.Equal(t, uint64(120), viper.GetUint64("max-condition-evaluation-cost"))
		require.True(t, viper.GetBool("access-control-enabled"))
		require.Equal(t, "12345", viper.GetString("access-control-store-id"))
		require.Equal(t, "67891", viper.GetString("access-control-model-id"))
		require.True(t, viper.GetBool("context-propagation-to-datastore"))
		require.True(t, viper.GetBool("shared-iterator-enabled"))
		require.Equal(t, uint32(950), viper.GetUint32("shared-iterator-limit"))

		return nil
	}

	rootCmd := cmd.NewRootCommand()
	rootCmd.AddCommand(runCmd)
	rootCmd.SetArgs([]string{"run"})
	require.NoError(t, rootCmd.Execute())
}

func TestServerContext_datastoreConfig(t *testing.T) {
	tests := []struct {
		name           string
		config         *serverconfig.Config
		wantDSType     interface{}
		wantSerializer encoder.ContinuationTokenSerializer
		wantErr        error
	}{
		{
			name: "sqlite",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine: "sqlite",
				},
			},
			wantDSType:     &sqlite.Datastore{},
			wantSerializer: &sqlcommon.SQLContinuationTokenSerializer{},
			wantErr:        nil,
		},
		{
			name: "sqlite_bad_uri",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine: "sqlite",
					URI:    "uri?is;bad=true",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("invalid semicolon separator in query"),
		},
		{
			name: "mysql_bad_uri",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine:   "mysql",
					Username: "root",
					Password: "password",
					URI:      "uri?is;bad=true",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("missing the slash separating the database name"),
		},
		{
			name: "postgres_bad_uri",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine:   "postgres",
					Username: "root",
					Password: "password",
					URI:      "~!@#$%^&*()_+}{:<>?",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("parse postgres connection uri"),
		},
		{
			name: "unsupported_engine",
			config: &serverconfig.Config{
				Datastore: serverconfig.DatastoreConfig{
					Engine: "unsupported",
				},
			},
			wantDSType:     nil,
			wantSerializer: nil,
			wantErr:        errors.New("storage engine 'unsupported' is unsupported"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &ServerContext{
				Logger: logger.NewNoopLogger(),
			}
			datastore, serializer, err := s.datastoreConfig(tt.config)
			if tt.wantErr != nil {
				require.Error(t, err)
				assert.Nil(t, datastore)
				assert.Nil(t, serializer)
				assert.ErrorContains(t, err, tt.wantErr.Error())
			} else {
				require.NoError(t, err)
				assert.IsType(t, tt.wantDSType, datastore)
				assert.Equal(t, tt.wantSerializer, serializer)
			}
		})
	}
}
