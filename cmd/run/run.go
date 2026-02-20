// Package run contains the command to run an OpenFGA server.
package run

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/log"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/internal/authn/oidc"
	"github.com/openfga/openfga/internal/authn/presharedkey"
	"github.com/openfga/openfga/internal/build"
	authnmw "github.com/openfga/openfga/internal/middleware/authn"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/gateway"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/middleware"
	httpmiddleware "github.com/openfga/openfga/pkg/middleware/http"
	"github.com/openfga/openfga/pkg/middleware/logging"
	"github.com/openfga/openfga/pkg/middleware/recovery"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/storeid"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/server/health"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
	"github.com/openfga/openfga/pkg/telemetry"
)

const (
	datastoreEngineFlag = "datastore-engine"
	datastoreURIFlag    = "datastore-uri"
)

func NewRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the OpenFGA server",
		Long:  "Run the OpenFGA server.",
		Run:   run,
		Args:  cobra.NoArgs,
	}

	defaultConfig := serverconfig.DefaultConfig()
	flags := cmd.Flags()

	flags.StringSlice("experimentals", defaultConfig.Experimentals, fmt.Sprintf("a comma-separated list of experimental features to enable. Allowed values: %s, %s, %s, %s, %s", serverconfig.ExperimentalCheckOptimizations, serverconfig.ExperimentalListObjectsOptimizations, serverconfig.ExperimentalAccessControlParams, serverconfig.ExperimentalPipelineListObjects, serverconfig.ExperimentalDatastoreThrottling))

	flags.Bool("access-control-enabled", defaultConfig.AccessControl.Enabled, "enable/disable the access control feature")

	flags.String("access-control-store-id", defaultConfig.AccessControl.StoreID, "the store ID of the OpenFGA store that will be used to access the access control store")

	flags.String("access-control-model-id", defaultConfig.AccessControl.ModelID, "the model ID of the OpenFGA store that will be used to access the access control store")

	cmd.MarkFlagsRequiredTogether("access-control-enabled", "access-control-store-id", "access-control-model-id")

	flags.String("grpc-addr", defaultConfig.GRPC.Addr, "the host:port address to serve the grpc server on")

	flags.Bool("grpc-tls-enabled", defaultConfig.GRPC.TLS.Enabled, "enable/disable transport layer security (TLS)")

	flags.String("grpc-tls-cert", defaultConfig.GRPC.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")

	flags.String("grpc-tls-key", defaultConfig.GRPC.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")

	cmd.MarkFlagsRequiredTogether("grpc-tls-enabled", "grpc-tls-cert", "grpc-tls-key")

	flags.Bool("http-enabled", defaultConfig.HTTP.Enabled, "enable/disable the OpenFGA HTTP server")

	flags.String("http-addr", defaultConfig.HTTP.Addr, "the host:port address to serve the HTTP server on")

	flags.Bool("http-tls-enabled", defaultConfig.HTTP.TLS.Enabled, "enable/disable transport layer security (TLS)")

	flags.String("http-tls-cert", defaultConfig.HTTP.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")

	flags.String("http-tls-key", defaultConfig.HTTP.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")

	cmd.MarkFlagsRequiredTogether("http-tls-enabled", "http-tls-cert", "http-tls-key")

	flags.Duration("http-upstream-timeout", defaultConfig.HTTP.UpstreamTimeout, "the timeout duration for proxying HTTP requests upstream to the grpc endpoint")

	flags.StringSlice("http-cors-allowed-origins", defaultConfig.HTTP.CORSAllowedOrigins, "specifies the CORS allowed origins")

	flags.StringSlice("http-cors-allowed-headers", defaultConfig.HTTP.CORSAllowedHeaders, "specifies the CORS allowed headers")

	flags.String("authn-method", defaultConfig.Authn.Method, "the authentication method to use")

	flags.StringSlice("authn-preshared-keys", defaultConfig.Authn.Keys, "one or more preshared keys to use for authentication")

	flags.String("authn-oidc-audience", defaultConfig.Authn.Audience, "the OIDC audience of the tokens being signed by the authorization server")

	flags.String("authn-oidc-issuer", defaultConfig.Authn.Issuer, "the OIDC issuer (authorization server) signing the tokens, and where the keys will be fetched from")

	flags.StringSlice("authn-oidc-issuer-aliases", defaultConfig.Authn.IssuerAliases, "the OIDC issuer DNS aliases that will be accepted as valid when verifying the `iss` field of the JWTs.")

	flags.StringSlice("authn-oidc-subjects", defaultConfig.Authn.Subjects, "the OIDC subject names that will be accepted as valid when verifying the `sub` field of the JWTs. If empty, every `sub` will be allowed")

	flags.StringSlice("authn-oidc-client-id-claims", defaultConfig.Authn.ClientIDClaims, "the ClientID claims that will be used to parse the clientID - configure in order of priority (first is highest). Defaults to [`azp`, `client_id`]")

	flags.String("datastore-engine", defaultConfig.Datastore.Engine, "the datastore engine that will be used for persistence")

	flags.String("datastore-uri", defaultConfig.Datastore.URI, "the connection uri to use to connect to the datastore (for any engine other than 'memory')")

	flags.String("datastore-secondary-uri", defaultConfig.Datastore.SecondaryURI, "the connection uri to use to connect to the secondary datastore (for postgres only)")

	flags.String("datastore-username", "", "the connection username to use to connect to the datastore (overwrites any username provided in the connection uri)")

	flags.String("datastore-password", "", "the connection password to use to connect to the datastore (overwrites any password provided in the connection uri)")

	flags.String("datastore-secondary-username", "", "the connection username to use to connect to the secondary datastore (overwrites any username provided in the connection uri)")

	flags.String("datastore-secondary-password", "", "the connection password to use to connect to the secondary datastore (overwrites any password provided in the connection uri)")

	flags.Int("datastore-max-cache-size", defaultConfig.Datastore.MaxCacheSize, "the maximum number of authorization models that will be cached in memory")

	flags.Int("datastore-max-typesystem-cache-size", defaultConfig.Datastore.MaxTypesystemCacheSize, "the maximum number of type system models that will be cached in memory")

	flags.Int("datastore-min-open-conns", defaultConfig.Datastore.MinOpenConns, "the minimum number of open connections to the datastore")

	flags.Int("datastore-max-open-conns", defaultConfig.Datastore.MaxOpenConns, "the maximum number of open connections to the datastore")

	flags.Int("datastore-min-idle-conns", defaultConfig.Datastore.MinIdleConns, "the minimum number of connections to the datastore in the idle connection pool")

	flags.Int("datastore-max-idle-conns", defaultConfig.Datastore.MaxIdleConns, "the maximum number of connections to the datastore in the idle connection pool")

	flags.Duration("datastore-conn-max-idle-time", defaultConfig.Datastore.ConnMaxIdleTime, "the maximum amount of time a connection to the datastore may be idle")

	flags.Duration("datastore-conn-max-lifetime", defaultConfig.Datastore.ConnMaxLifetime, "the maximum amount of time a connection to the datastore may be reused")

	flags.Bool("datastore-metrics-enabled", defaultConfig.Datastore.Metrics.Enabled, "enable/disable sql metrics")

	flags.Bool("playground-enabled", defaultConfig.Playground.Enabled, "enable/disable the OpenFGA Playground")

	flags.Int("playground-port", defaultConfig.Playground.Port, "the port to serve the local OpenFGA Playground on")

	flags.Bool("profiler-enabled", defaultConfig.Profiler.Enabled, "enable/disable pprof profiling")

	flags.String("profiler-addr", defaultConfig.Profiler.Addr, "the host:port address to serve the pprof profiler server on")

	flags.String("log-format", defaultConfig.Log.Format, "the log format to output logs in")

	flags.String("log-level", defaultConfig.Log.Level, "the log level to use")

	flags.String("log-timestamp-format", defaultConfig.Log.TimestampFormat, "the timestamp format to use for log messages")

	flags.Bool("trace-enabled", defaultConfig.Trace.Enabled, "enable tracing")

	flags.String("trace-otlp-endpoint", defaultConfig.Trace.OTLP.Endpoint, "the endpoint of the trace collector")

	flags.Bool("trace-otlp-tls-enabled", defaultConfig.Trace.OTLP.TLS.Enabled, "use TLS connection for trace collector")

	flags.Float64("trace-sample-ratio", defaultConfig.Trace.SampleRatio, "the fraction of traces to sample. 1 means all, 0 means none.")

	flags.String("trace-service-name", defaultConfig.Trace.ServiceName, "the service name included in sampled traces.")

	flags.String("trace-resource-attributes", defaultConfig.Trace.ResourceAttributes, "key-value pairs to be used as resource attributes")

	flags.Bool("metrics-enabled", defaultConfig.Metrics.Enabled, "enable/disable prometheus metrics on the '/metrics' endpoint")

	flags.String("metrics-addr", defaultConfig.Metrics.Addr, "the host:port address to serve the prometheus metrics server on")

	flags.Bool("metrics-enable-rpc-histograms", defaultConfig.Metrics.EnableRPCHistograms, "enables prometheus histogram metrics for RPC latency distributions")

	flags.Uint32("max-concurrent-checks-per-batch-check", defaultConfig.MaxConcurrentChecksPerBatchCheck, "the maximum number of checks that can be processed concurrently in a batch check request")

	flags.Uint32("max-checks-per-batch-check", defaultConfig.MaxChecksPerBatchCheck, "the maximum number of tuples allowed in a BatchCheck request")

	flags.Int("max-tuples-per-write", defaultConfig.MaxTuplesPerWrite, "the maximum allowed number of tuples per Write transaction")

	flags.Int("max-types-per-authorization-model", defaultConfig.MaxTypesPerAuthorizationModel, "the maximum allowed number of type definitions per authorization model")

	flags.Int("max-authorization-model-size-in-bytes", defaultConfig.MaxAuthorizationModelSizeInBytes, "the maximum size in bytes allowed for persisting an Authorization Model.")

	flags.Uint32("max-concurrent-reads-for-list-users", defaultConfig.MaxConcurrentReadsForListUsers, "the maximum allowed number of concurrent datastore reads in a single ListUsers query. A high number will consume more connections from the datastore pool and will attempt to prioritize performance for the request at the expense of other queries performance.")

	flags.Uint32("max-concurrent-reads-for-list-objects", defaultConfig.MaxConcurrentReadsForListObjects, "the maximum allowed number of concurrent datastore reads in a single ListObjects or StreamedListObjects query. A high number will consume more connections from the datastore pool and will attempt to prioritize performance for the request at the expense of other queries performance.")

	flags.Uint32("max-concurrent-reads-for-check", defaultConfig.MaxConcurrentReadsForCheck, "the maximum allowed number of concurrent datastore reads in a single Check query. A high number will consume more connections from the datastore pool and will attempt to prioritize performance for the request at the expense of other queries performance.")

	flags.Uint64("max-condition-evaluation-cost", defaultConfig.MaxConditionEvaluationCost, "the maximum cost for CEL condition evaluation before a request returns an error")

	flags.Int("changelog-horizon-offset", defaultConfig.ChangelogHorizonOffset, "the offset (in minutes) from the current time. Changes that occur after this offset will not be included in the response of ReadChanges")

	flags.Uint32("resolve-node-limit", defaultConfig.ResolveNodeLimit, "maximum resolution depth to attempt before throwing an error (defines how deeply nested an authorization model can be before a query errors out).")

	flags.Uint32("resolve-node-breadth-limit", defaultConfig.ResolveNodeBreadthLimit, "defines how many nodes on a given level can be evaluated concurrently in a Check resolution tree")

	flags.Duration("listObjects-deadline", defaultConfig.ListObjectsDeadline, "the timeout deadline for serving ListObjects and StreamedListObjects requests")

	flags.Uint32("listObjects-max-results", defaultConfig.ListObjectsMaxResults, "the maximum results to return in non-streaming ListObjects API responses. If 0, all results can be returned")

	flags.Duration("listUsers-deadline", defaultConfig.ListUsersDeadline, "the timeout deadline for serving ListUsers requests. If 0, there is no deadline")

	flags.Uint32("listUsers-max-results", defaultConfig.ListUsersMaxResults, "the maximum results to return in ListUsers API responses. If 0, all results can be returned")

	flags.Uint32("readChanges-max-page-size", defaultConfig.ReadChangesMaxPageSize, "the maximum page size allowed for ReadChanges API requests")

	flags.Uint32("check-cache-limit", defaultConfig.CheckCache.Limit, "if check-query-cache-enabled or check-iterator-cache-enabled, this is the size limit of the cache")

	flags.Bool("shared-iterator-enabled", defaultConfig.SharedIterator.Enabled, "enabling sharing of datastore iterators with different consumers. Each iterator is the result of a database query, for example usersets related to a specific object, or objects related to a specific user, up to a certain number of tuples per iterator.")

	flags.Uint32("shared-iterator-limit", defaultConfig.SharedIterator.Limit, "if shared-iterator-enabled is enabled, this is the limit of the number of iterators that can be shared.")

	flags.Bool("check-iterator-cache-enabled", defaultConfig.CheckIteratorCache.Enabled, "enable caching of datastore iterators. The key is a string representing a database query, and the value is a list of tuples. Each iterator is the result of a database query, for example usersets related to a specific object, or objects related to a specific user, up to a certain number of tuples per iterator. If the request's consistency is HIGHER_CONSISTENCY, this cache is not used.")

	flags.Uint32("check-iterator-cache-max-results", defaultConfig.CheckIteratorCache.MaxResults, "if caching of datastore iterators of Check requests is enabled, this is the limit of tuples to cache per key.")

	flags.Duration("check-iterator-cache-ttl", defaultConfig.CheckIteratorCache.TTL, "if caching of datastore iterators of Check requests is enabled, this is the TTL of each value")

	flags.Bool("list-objects-iterator-cache-enabled", defaultConfig.ListObjectsIteratorCache.Enabled, "enable caching of datastore iterators for ListObjects. The key is a string representing a database query, and the value is a list of tuples. Each iterator is the result of a database query, for example usersets related to a specific object, or objects related to a specific user, up to a certain number of tuples per iterator. If the request's consistency is HIGHER_CONSISTENCY, this cache is not used.")

	flags.Uint32("list-objects-iterator-cache-max-results", defaultConfig.ListObjectsIteratorCache.MaxResults, "if caching of datastore iterators of ListObjects requests is enabled, this is the limit of tuples to cache per key.")

	flags.Duration("list-objects-iterator-cache-ttl", defaultConfig.ListObjectsIteratorCache.TTL, "if caching of datastore iterators of ListObjects requests is enabled, this is the TTL of each value")

	flags.Bool("check-query-cache-enabled", defaultConfig.CheckQueryCache.Enabled, "enable caching of Check requests. For example, if you have a relation define viewer: owner or editor, and the query is Check(user:anne, viewer, doc:1), we'll evaluate the owner relation and the editor relation and cache both results: (user:anne, viewer, doc:1) -> allowed=true and (user:anne, owner, doc:1) -> allowed=true. The cache is stored in-memory; the cached values are overwritten on every change in the result, and cleared after the configured TTL. This flag improves latency, but turns Check and ListObjects into eventually consistent APIs. If the request's consistency is HIGHER_CONSISTENCY, this cache is not used.")

	flags.Uint32("check-query-cache-limit", defaultConfig.CheckCache.Limit, "DEPRECATED: Use check-cache-limit instead. If caching of Check and ListObjects calls is enabled, this is the size limit of the cache")

	flags.Duration("check-query-cache-ttl", defaultConfig.CheckQueryCache.TTL, "if check-query-cache-enabled, this is the TTL of each value")

	flags.Bool("cache-controller-enabled", defaultConfig.CacheController.Enabled, "enable invalidation of check query cache and iterator cache based on recent tuple writes. Invalidation is triggered by Check and List Objects requests, which periodically check the datastore's changelog table for writes and invalidate cache entries earlier than recent writes. Invalidations from Check requests are rate-limited by cache-controller-ttl, whereas List Objects requests invalidate every time if list objects iterator cache is enabled.")

	flags.Duration("cache-controller-ttl", defaultConfig.CacheController.TTL, "if cache controller is enabled, this is the minimum time interval for Check requests to trigger cache invalidation. List Objects requests may trigger invalidation even sooner if list objects iterator cache is enabled.")

	// Unfortunately UintSlice/IntSlice does not work well when used as environment variable, we need to stick with string slice and convert back to integer
	flags.StringSlice("request-duration-datastore-query-count-buckets", defaultConfig.RequestDurationDatastoreQueryCountBuckets, "datastore query count buckets used in labelling request_duration_ms.")

	flags.StringSlice("request-duration-dispatch-count-buckets", defaultConfig.RequestDurationDispatchCountBuckets, "dispatch count (i.e number of concurrent traversals to resolve a query) buckets used in labelling request_duration_ms.")

	flags.Bool("context-propagation-to-datastore", defaultConfig.ContextPropagationToDatastore, "enable propagation of a request's context to the datastore")

	flags.Bool("check-dispatch-throttling-enabled", defaultConfig.CheckDispatchThrottling.Enabled, "enable throttling for Check requests when the request's number of dispatches is high. Enabling this feature will prioritize dispatched requests requiring less than the configured dispatch threshold over requests whose dispatch count exceeds the configured threshold.")

	flags.Duration("check-dispatch-throttling-frequency", defaultConfig.CheckDispatchThrottling.Frequency, "defines how frequent Check dispatch throttling will be evaluated. This controls how frequently throttled dispatch Check requests are dispatched.")

	flags.Uint32("check-dispatch-throttling-threshold", defaultConfig.CheckDispatchThrottling.Threshold, "define the number of dispatches above which Check requests will be throttled.")

	flags.Uint32("check-dispatch-throttling-max-threshold", defaultConfig.CheckDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold beyond which a Check requests will be throttled. 0 will use the 'check-dispatch-throttling-threshold' value as maximum")

	flags.Bool("listObjects-dispatch-throttling-enabled", defaultConfig.ListObjectsDispatchThrottling.Enabled, "enable throttling when a ListObjects request's number of dispatches is high. Enabling this feature will prioritize dispatched requests requiring less than the configured dispatch threshold over requests whose dispatch count exceeds the configured threshold.")

	flags.Duration("listObjects-dispatch-throttling-frequency", defaultConfig.ListObjectsDispatchThrottling.Frequency, "defines how frequent ListObjects dispatch throttling will be evaluated. Frequency controls how frequently throttled dispatch ListObjects requests are dispatched.")

	flags.Uint32("listObjects-dispatch-throttling-threshold", defaultConfig.ListObjectsDispatchThrottling.Threshold, "defines the number of dispatches above which ListObjects requests will be throttled.")

	flags.Uint32("listObjects-dispatch-throttling-max-threshold", defaultConfig.ListObjectsDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold beyond which a ListObjects requests will be throttled. 0 will use the 'listObjects-dispatch-throttling-threshold' value as maximum")

	flags.Bool("listUsers-dispatch-throttling-enabled", defaultConfig.ListUsersDispatchThrottling.Enabled, "enable throttling when a ListUsers request's number of dispatches is high. Enabling this feature will prioritize dispatched requests requiring less than the configured dispatch threshold over requests whose dispatch count exceeds the configured threshold.")

	flags.Duration("listUsers-dispatch-throttling-frequency", defaultConfig.ListUsersDispatchThrottling.Frequency, "defines how frequent ListUsers dispatch throttling will be evaluated. Frequency controls how frequently throttled dispatch ListUsers requests are dispatched.")

	flags.Uint32("listUsers-dispatch-throttling-threshold", defaultConfig.ListUsersDispatchThrottling.Threshold, "defines the number of dispatches above which ListUsers requests will be throttled.")

	flags.Uint32("listUsers-dispatch-throttling-max-threshold", defaultConfig.ListUsersDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold beyond which a list users requests will be throttled. 0 will use the 'listUsers-dispatch-throttling-threshold' value as maximum")

	flags.Int("check-datastore-throttle-threshold", defaultConfig.CheckDatastoreThrottle.Threshold, "define the number of datastore requests allowed before being throttled.")

	flags.Duration("check-datastore-throttle-duration", defaultConfig.CheckDatastoreThrottle.Duration, "defines the time for which the datastore request will be suspended for being throttled.")

	flags.Int("listObjects-datastore-throttle-threshold", defaultConfig.ListObjectsDatastoreThrottle.Threshold, "define the number of datastore requests allowed before being throttled.")

	flags.Duration("listObjects-datastore-throttle-duration", defaultConfig.ListObjectsDatastoreThrottle.Duration, "defines the time for which the datastore request will be suspended for being throttled.")

	flags.Int("listUsers-datastore-throttle-threshold", defaultConfig.ListUsersDatastoreThrottle.Threshold, "define the number of datastore requests allowed before being throttled.")

	flags.Duration("listUsers-datastore-throttle-duration", defaultConfig.ListUsersDatastoreThrottle.Duration, "defines the time for which the datastore request will be suspended for being throttled.")

	flags.Duration("request-timeout", defaultConfig.RequestTimeout, "configures request timeout.  If both HTTP upstream timeout and request timeout are specified, request timeout will be used.")

	flags.Duration("planner-eviction-threshold", defaultConfig.Planner.EvictionThreshold, "how long a planner key can be unused before being evicted")
	flags.Duration("planner-cleanup-interval", defaultConfig.Planner.CleanupInterval, "how often the planner checks for stale keys")

	// NOTE: if you add a new flag here, update the function below, too

	cmd.PreRun = bindRunFlagsFunc(flags)

	return cmd
}

// ReadConfig returns the OpenFGA server configuration based on the values provided in the server's 'config.yaml' file.
// The 'config.yaml' file is loaded from '/etc/openfga', '$HOME/.openfga', or the current working directory. If no configuration
// file is present, the default values are returned.
func ReadConfig() (*serverconfig.Config, error) {
	config := serverconfig.DefaultConfig()

	viper.SetTypeByDefaultValue(true)
	err := viper.ReadInConfig()
	if err != nil {
		if !errors.As(err, &viper.ConfigFileNotFoundError{}) {
			return nil, fmt.Errorf("failed to load server config: %w", err)
		}
	}

	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server config: %w", err)
	}

	return config, nil
}

func run(_ *cobra.Command, _ []string) {
	config, err := ReadConfig()
	if err != nil {
		panic(err)
	}

	if err := config.Verify(); err != nil {
		panic(err)
	}

	logger := logger.MustNewLogger(config.Log.Format, config.Log.Level, config.Log.TimestampFormat)
	serverCtx := &ServerContext{Logger: logger}
	if err := serverCtx.Run(context.Background(), config); err != nil {
		panic(err)
	}
}

type ServerContext struct {
	Logger logger.Logger
}

func convertStringArrayToUintArray(stringArray []string) []uint {
	uintArray := []uint{}
	for _, val := range stringArray {
		// note that we have already validated whether the array item is non-negative integer
		valInt, err := strconv.Atoi(val)
		if err == nil {
			uintArray = append(uintArray, uint(valInt))
		}
	}
	return uintArray
}

// telemetryConfig returns the function that must be called to shut down tracing.
// The context provided to this function should be error-free, or shut down will be incomplete.
func (s *ServerContext) telemetryConfig(config *serverconfig.Config) func() error {
	if config.Trace.Enabled {
		s.Logger.Info(fmt.Sprintf("🕵 tracing enabled: sampling ratio is %v and sending traces to '%s', tls: %t", config.Trace.SampleRatio, config.Trace.OTLP.Endpoint, config.Trace.OTLP.TLS.Enabled))

		options := []telemetry.TracerOption{
			telemetry.WithOTLPEndpoint(
				config.Trace.OTLP.Endpoint,
			),
			telemetry.WithAttributes(
				semconv.ServiceNameKey.String(config.Trace.ServiceName),
				semconv.ServiceVersionKey.String(build.Version),
			),
			telemetry.WithSamplingRatio(config.Trace.SampleRatio),
		}

		if !config.Trace.OTLP.TLS.Enabled {
			options = append(options, telemetry.WithOTLPInsecure())
		}

		tp := telemetry.MustNewTracerProvider(options...)
		return func() error {
			// can take up to 5 seconds to complete (https://github.com/open-telemetry/opentelemetry-go/blob/aebcbfcbc2962957a578e9cb3e25dc834125e318/sdk/trace/batch_span_processor.go#L97)
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			return errors.Join(tp.ForceFlush(ctx), tp.Shutdown(ctx))
		}
	}
	otel.SetTracerProvider(noop.NewTracerProvider())
	return func() error {
		return nil
	}
}

func (s *ServerContext) datastoreConfig(config *serverconfig.Config) (storage.OpenFGADatastore, encoder.ContinuationTokenSerializer, error) {
	// SQL Token Serializer by default
	tokenSerializer := sqlcommon.NewSQLContinuationTokenSerializer()
	datastoreOptions := []sqlcommon.DatastoreOption{
		sqlcommon.WithSecondaryURI(config.Datastore.SecondaryURI),
		sqlcommon.WithUsername(config.Datastore.Username),
		sqlcommon.WithPassword(config.Datastore.Password),
		sqlcommon.WithSecondaryUsername(config.Datastore.SecondaryUsername),
		sqlcommon.WithSecondaryPassword(config.Datastore.SecondaryPassword),
		sqlcommon.WithLogger(s.Logger),
		sqlcommon.WithMaxTuplesPerWrite(config.MaxTuplesPerWrite),
		sqlcommon.WithMaxTypesPerAuthorizationModel(config.MaxTypesPerAuthorizationModel),
		sqlcommon.WithMaxOpenConns(config.Datastore.MaxOpenConns),
		sqlcommon.WithMinOpenConns(config.Datastore.MinOpenConns),
		sqlcommon.WithMaxIdleConns(config.Datastore.MaxIdleConns),
		sqlcommon.WithMinIdleConns(config.Datastore.MinIdleConns),
		sqlcommon.WithConnMaxIdleTime(config.Datastore.ConnMaxIdleTime),
		sqlcommon.WithConnMaxLifetime(config.Datastore.ConnMaxLifetime),
	}

	if config.Datastore.Metrics.Enabled {
		datastoreOptions = append(datastoreOptions, sqlcommon.WithMetrics())
	}

	dsCfg := sqlcommon.NewConfig(datastoreOptions...)

	var datastore storage.OpenFGADatastore
	var err error
	switch config.Datastore.Engine {
	case "memory":
		// override for "memory" datastore
		tokenSerializer = encoder.NewStringContinuationTokenSerializer()
		opts := []memory.StorageOption{
			memory.WithMaxTypesPerAuthorizationModel(config.MaxTypesPerAuthorizationModel),
			memory.WithMaxTuplesPerWrite(config.MaxTuplesPerWrite),
		}
		datastore = memory.New(opts...)
	case "mysql":
		datastore, err = mysql.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("initialize mysql datastore: %w", err)
		}
	case "postgres":
		datastore, err = postgres.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("initialize postgres datastore: %w", err)
		}
	case "sqlite":
		datastore, err = sqlite.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("initialize sqlite datastore: %w", err)
		}
	default:
		return nil, nil, fmt.Errorf("storage engine '%s' is unsupported", config.Datastore.Engine)
	}

	s.Logger.Info(fmt.Sprintf("using '%v' storage engine", config.Datastore.Engine))

	return datastore, tokenSerializer, nil
}

func (s *ServerContext) authenticatorConfig(config *serverconfig.Config) (authn.Authenticator, error) {
	var authenticator authn.Authenticator
	var err error

	switch config.Authn.Method {
	case "none":
		s.Logger.Warn("authentication is disabled")
		authenticator = authn.NoopAuthenticator{}
	case "preshared":
		s.Logger.Info("using 'preshared' authentication")
		authenticator, err = presharedkey.NewPresharedKeyAuthenticator(config.Authn.Keys)
	case "oidc":
		s.Logger.Info("using 'oidc' authentication")
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.Authn.Issuer, config.Authn.IssuerAliases, config.Authn.Audience, config.Authn.Subjects, config.Authn.ClientIDClaims)
	default:
		return nil, fmt.Errorf("unsupported authentication method '%v'", config.Authn.Method)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authenticator: %w", err)
	}
	return authenticator, nil
}

func (s *ServerContext) buildServerOpts(ctx context.Context, config *serverconfig.Config, authenticator authn.Authenticator) ([]grpc.ServerOption, *grpc_prometheus.ServerMetrics, error) {
	serverOpts := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(serverconfig.DefaultMaxRPCMessageSizeInBytes),
		grpc.ChainUnaryInterceptor(
			[]grpc.UnaryServerInterceptor{
				grpc_recovery.UnaryServerInterceptor( // panic middleware must be 1st in chain
					grpc_recovery.WithRecoveryHandlerContext(
						recovery.PanicRecoveryHandler(s.Logger),
					),
				),
				grpc_ctxtags.UnaryServerInterceptor(), // needed for logging
				requestid.NewUnaryInterceptor(),       // add request_id to ctxtags
			}...,
		),
		grpc.ChainStreamInterceptor(
			[]grpc.StreamServerInterceptor{
				grpc_recovery.StreamServerInterceptor( // panic middleware must be 1st in chain
					grpc_recovery.WithRecoveryHandlerContext(
						recovery.PanicRecoveryHandler(s.Logger),
					),
				),
				grpc_ctxtags.StreamServerInterceptor(), // needed for logging
				requestid.NewStreamingInterceptor(),    // add request_id to ctxtags
			}...,
		),
	}

	if config.RequestTimeout > 0 {
		timeoutMiddleware := middleware.NewTimeoutInterceptor(config.RequestTimeout, s.Logger)

		serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(timeoutMiddleware.NewUnaryTimeoutInterceptor()))
		serverOpts = append(serverOpts, grpc.ChainStreamInterceptor(timeoutMiddleware.NewStreamTimeoutInterceptor()))
	}

	serverOpts = append(serverOpts,
		grpc.ChainUnaryInterceptor(
			[]grpc.UnaryServerInterceptor{
				storeid.NewUnaryInterceptor(),           // if available, add store_id to ctxtags
				logging.NewLoggingInterceptor(s.Logger), // needed to log invalid requests
				validator.UnaryServerInterceptor(),
			}...,
		),
		grpc.ChainStreamInterceptor(
			[]grpc.StreamServerInterceptor{
				validator.StreamServerInterceptor(),
			}...,
		),
	)

	var prometheusMetrics *grpc_prometheus.ServerMetrics
	if config.Metrics.Enabled {
		var metricsOpts []grpc_prometheus.ServerMetricsOption
		if config.Metrics.EnableRPCHistograms {
			metricsOpts = append(metricsOpts, grpc_prometheus.WithServerHandlingTimeHistogram())
		}

		prometheusMetrics = grpc_prometheus.NewServerMetrics(metricsOpts...)
		prometheus.MustRegister(prometheusMetrics)

		serverOpts = append(serverOpts,
			grpc.ChainUnaryInterceptor(prometheusMetrics.UnaryServerInterceptor()),
			grpc.ChainStreamInterceptor(prometheusMetrics.StreamServerInterceptor()))
	}

	if config.Trace.Enabled {
		serverOpts = append(serverOpts, grpc.StatsHandler(otelgrpc.NewServerHandler()))
	}

	serverOpts = append(serverOpts, grpc.ChainUnaryInterceptor(
		[]grpc.UnaryServerInterceptor{
			grpcauth.UnaryServerInterceptor(authnmw.AuthFunc(authenticator)),
		}...),
		grpc.ChainStreamInterceptor(
			[]grpc.StreamServerInterceptor{
				grpcauth.StreamServerInterceptor(authnmw.AuthFunc(authenticator)),
				// The following interceptors wrap the server stream with our own
				// wrapper and must come last.
				storeid.NewStreamingInterceptor(),
				logging.NewStreamingLoggingInterceptor(s.Logger),
			}...,
		),
	)

	if config.GRPC.TLS.Enabled {
		if config.GRPC.TLS.CertPath == "" || config.GRPC.TLS.KeyPath == "" {
			return nil, prometheusMetrics, errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
		}
		grpcGetCertificate, err := watchAndLoadCertificateWithCertWatcher(ctx, config.GRPC.TLS.CertPath, config.GRPC.TLS.KeyPath, s.Logger)
		if err != nil {
			return nil, prometheusMetrics, err
		}
		creds := credentials.NewTLS(&tls.Config{
			GetCertificate: grpcGetCertificate,
		})

		serverOpts = append(serverOpts, grpc.Creds(creds))

		s.Logger.Info("gRPC TLS is enabled, serving connections using the provided certificate")
	} else {
		s.Logger.Warn("gRPC TLS is disabled, serving connections using insecure plaintext")
	}
	return serverOpts, prometheusMetrics, nil
}

func (s *ServerContext) dialGrpc(udsPath string, config *serverconfig.Config) *grpc.ClientConn {
	dialOpts := []grpc.DialOption{
		// UDS is local IPC — TLS is unnecessary.
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if config.Trace.Enabled {
		dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
	}

	conn, err := grpc.NewClient("unix://"+udsPath, dialOpts...)
	if err != nil {
		s.Logger.Fatal("failed to create gRPC client connection", zap.Error(err))
	}
	return conn
}

func (s *ServerContext) runHTTPServer(ctx context.Context, config *serverconfig.Config, grpcConn *grpc.ClientConn) (*http.Server, error) {
	muxOpts := []runtime.ServeMuxOption{
		runtime.WithForwardResponseOption(httpmiddleware.HTTPResponseModifier),
		runtime.WithErrorHandler(func(c context.Context, sr *runtime.ServeMux, mm runtime.Marshaler, w http.ResponseWriter, r *http.Request, e error) {
			intCode := serverErrors.ConvertToEncodedErrorCode(status.Convert(e))
			httpmiddleware.CustomHTTPErrorHandler(c, w, r, serverErrors.NewEncodedError(intCode, e.Error()))
		}),
		runtime.WithStreamErrorHandler(func(ctx context.Context, e error) *status.Status {
			intCode := serverErrors.ConvertToEncodedErrorCode(status.Convert(e))
			encodedErr := serverErrors.NewEncodedError(intCode, e.Error())
			return status.Convert(encodedErr)
		}),
		runtime.WithHealthzEndpoint(healthv1pb.NewHealthClient(grpcConn)),
		runtime.WithOutgoingHeaderMatcher(func(s string) (string, bool) { return s, true }),
	}
	mux := runtime.NewServeMux(muxOpts...)
	if err := openfgav1.RegisterOpenFGAServiceHandler(ctx, mux, grpcConn); err != nil {
		return nil, err
	}
	handler := http.Handler(mux)

	if config.Trace.Enabled {
		handler = otelhttp.NewHandler(handler, "grpc-gateway")
	}

	httpServer := &http.Server{
		Addr: config.HTTP.Addr,
		Handler: recovery.HTTPPanicRecoveryHandler(cors.New(cors.Options{
			AllowedOrigins:   config.HTTP.CORSAllowedOrigins,
			AllowCredentials: true,
			AllowedHeaders:   config.HTTP.CORSAllowedHeaders,
			AllowedMethods: []string{
				http.MethodGet, http.MethodPost,
				http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodPut,
			},
		}).Handler(handler), s.Logger),
	}

	listener, err := net.Listen("tcp", config.HTTP.Addr)
	if err != nil {
		return nil, err
	}

	if config.HTTP.TLS.Enabled {
		if config.HTTP.TLS.CertPath == "" || config.HTTP.TLS.KeyPath == "" {
			s.Logger.Fatal("'http.tls.cert' and 'http.tls.key' configs must be set")
		}
		httpGetCertificate, err := watchAndLoadCertificateWithCertWatcher(ctx, config.HTTP.TLS.CertPath, config.HTTP.TLS.KeyPath, s.Logger)
		if err != nil {
			return nil, err
		}
		listener = tls.NewListener(listener, &tls.Config{
			GetCertificate: httpGetCertificate,
		})

		s.Logger.Info("HTTP TLS is enabled, serving connections using the provided certificate")
	} else {
		s.Logger.Warn("HTTP TLS is disabled, serving connections using insecure plaintext")
	}

	go func() {
		s.Logger.Info(fmt.Sprintf("🚀 starting HTTP server on '%s'...", httpServer.Addr))
		if err := httpServer.Serve(listener); err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				s.Logger.Fatal("HTTP server closed with unexpected error", zap.Error(err))
			}
		}
		s.Logger.Info("HTTP server shut down.")
	}()
	return httpServer, nil
}

func (s *ServerContext) runPlaygroundServer(config *serverconfig.Config) (*http.Server, error) {
	if !config.HTTP.Enabled {
		return nil, errors.New("the HTTP server must be enabled to run the openfga playground")
	}

	authMethod := config.Authn.Method
	if authMethod != "none" && authMethod != "preshared" {
		return nil, errors.New("the playground only supports authn methods 'none' and 'preshared'")
	}

	playgroundAddr := fmt.Sprintf(":%d", config.Playground.Port)
	s.Logger.Info(fmt.Sprintf("🛝 starting openfga playground on http://localhost%s/playground", playgroundAddr))

	tmpl, err := template.ParseFS(assets.EmbedPlayground, "playground/index.html")
	if err != nil {
		return nil, fmt.Errorf("failed to parse playground index.html as Go template: %w", err)
	}

	fileServer := http.FileServer(http.FS(assets.EmbedPlayground))

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 3 * time.Second

	var conn net.Conn
	err = backoff.Retry(
		func() error {
			conn, err = net.Dial("tcp", config.HTTP.Addr)
			return err
		},
		policy,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to establish playground connection to HTTP server: %w", err)
	}

	playgroundAPIToken := ""
	if authMethod == "preshared" {
		playgroundAPIToken = config.Authn.Keys[0]
	}

	mux := http.NewServeMux()
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/playground") {
			if r.URL.Path == "/playground" || r.URL.Path == "/playground/index.html" {
				err = tmpl.Execute(w, struct {
					HTTPServerURL      string
					PlaygroundAPIToken string
				}{
					HTTPServerURL:      conn.RemoteAddr().String(),
					PlaygroundAPIToken: playgroundAPIToken,
				})
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					s.Logger.Error("failed to execute/render the playground web template", zap.Error(err))
				}

				return
			}

			fileServer.ServeHTTP(w, r)
			return
		}

		http.NotFound(w, r)
	}))

	playground := &http.Server{Addr: playgroundAddr, Handler: mux}

	go func() {
		err = playground.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			s.Logger.Fatal("failed to start the openfga playground server", zap.Error(err))
		}
		s.Logger.Info("playground shut down.")
	}()
	return playground, nil
}

// Run returns an error if the server was unable to start successfully.
// If it started and terminated successfully, it returns a nil error.
func (s *ServerContext) Run(ctx context.Context, config *serverconfig.Config) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, os.Kill, syscall.SIGTERM)
	defer stop()

	tracerProviderCloser := s.telemetryConfig(config)

	if len(config.Experimentals) > 0 {
		s.Logger.Info(fmt.Sprintf("🧪 experimental features enabled: %v", config.Experimentals))
	}

	var experimentals []string
	experimentals = append(experimentals, config.Experimentals...)

	datastore, continuationTokenSerializer, err := s.datastoreConfig(config)
	if err != nil {
		return err
	}

	authenticator, err := s.authenticatorConfig(config)

	if err != nil {
		return err
	}

	serverOpts, prometheusMetrics, err := s.buildServerOpts(ctx, config, authenticator)
	if prometheusMetrics != nil {
		defer prometheus.Unregister(prometheusMetrics)
	}
	if err != nil {
		return err
	}

	var profilerServer *http.Server
	if config.Profiler.Enabled {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		profilerServer = &http.Server{Addr: config.Profiler.Addr, Handler: mux}

		go func() {
			s.Logger.Info(fmt.Sprintf("🔬 starting pprof profiler on '%s'", config.Profiler.Addr))

			if err := profilerServer.ListenAndServe(); err != nil {
				if !errors.Is(err, http.ErrServerClosed) {
					s.Logger.Fatal("failed to start pprof profiler", zap.Error(err))
				}
			}
			s.Logger.Info("profiler shut down.")
		}()
	}

	var metricsServer *http.Server
	if config.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		metricsServer = &http.Server{Addr: config.Metrics.Addr, Handler: mux}

		go func() {
			s.Logger.Info(fmt.Sprintf("📈 starting prometheus metrics server on '%s'", config.Metrics.Addr))
			if err := metricsServer.ListenAndServe(); err != nil {
				if !errors.Is(err, http.ErrServerClosed) {
					s.Logger.Fatal("failed to start prometheus metrics server", zap.Error(err))
				}
			}
			s.Logger.Info("metrics server shut down.")
		}()
	}

	svr := server.MustNewServerWithOpts(
		server.WithDatastore(datastore),
		server.WithContinuationTokenSerializer(continuationTokenSerializer),
		server.WithAuthorizationModelCacheSize(config.Datastore.MaxCacheSize),
		server.WithTypesystemCacheSize(config.Datastore.MaxTypesystemCacheSize),
		server.WithLogger(s.Logger),
		server.WithTransport(gateway.NewRPCTransport(s.Logger)),
		server.WithResolveNodeLimit(config.ResolveNodeLimit),
		server.WithResolveNodeBreadthLimit(config.ResolveNodeBreadthLimit),
		server.WithChangelogHorizonOffset(config.ChangelogHorizonOffset),
		server.WithReadChangesMaxPageSize(config.ReadChangesMaxPageSize),
		server.WithListObjectsDeadline(config.ListObjectsDeadline),
		server.WithListObjectsMaxResults(config.ListObjectsMaxResults),
		server.WithListUsersDeadline(config.ListUsersDeadline),
		server.WithListUsersMaxResults(config.ListUsersMaxResults),
		server.WithMaxConcurrentReadsForListObjects(config.MaxConcurrentReadsForListObjects),
		server.WithMaxConcurrentReadsForCheck(config.MaxConcurrentReadsForCheck),
		server.WithMaxConcurrentReadsForListUsers(config.MaxConcurrentReadsForListUsers),
		server.WithCacheControllerEnabled(config.CacheController.Enabled),
		server.WithCacheControllerTTL(config.CacheController.TTL),
		server.WithCheckCacheLimit(config.CheckCache.Limit),
		server.WithCheckIteratorCacheEnabled(config.CheckIteratorCache.Enabled),
		server.WithCheckIteratorCacheMaxResults(config.CheckIteratorCache.MaxResults),
		server.WithCheckIteratorCacheTTL(config.CheckIteratorCache.TTL),
		server.WithCheckQueryCacheEnabled(config.CheckQueryCache.Enabled),
		server.WithCheckQueryCacheTTL(config.CheckQueryCache.TTL),
		server.WithRequestDurationByQueryHistogramBuckets(convertStringArrayToUintArray(config.RequestDurationDatastoreQueryCountBuckets)),
		server.WithRequestDurationByDispatchCountHistogramBuckets(convertStringArrayToUintArray(config.RequestDurationDispatchCountBuckets)),
		server.WithMaxAuthorizationModelSizeInBytes(config.MaxAuthorizationModelSizeInBytes),
		server.WithContextPropagationToDatastore(config.ContextPropagationToDatastore),
		server.WithDispatchThrottlingCheckResolverEnabled(config.CheckDispatchThrottling.Enabled),
		server.WithDispatchThrottlingCheckResolverFrequency(config.CheckDispatchThrottling.Frequency),
		server.WithDispatchThrottlingCheckResolverThreshold(config.CheckDispatchThrottling.Threshold),
		server.WithDispatchThrottlingCheckResolverMaxThreshold(config.CheckDispatchThrottling.MaxThreshold),
		server.WithListObjectsDispatchThrottlingEnabled(config.ListObjectsDispatchThrottling.Enabled),
		server.WithListObjectsDispatchThrottlingFrequency(config.ListObjectsDispatchThrottling.Frequency),
		server.WithListObjectsDispatchThrottlingThreshold(config.ListObjectsDispatchThrottling.Threshold),
		server.WithListObjectsDispatchThrottlingMaxThreshold(config.ListObjectsDispatchThrottling.MaxThreshold),
		server.WithListUsersDispatchThrottlingEnabled(config.ListUsersDispatchThrottling.Enabled),
		server.WithListUsersDispatchThrottlingFrequency(config.ListUsersDispatchThrottling.Frequency),
		server.WithListUsersDispatchThrottlingThreshold(config.ListUsersDispatchThrottling.Threshold),
		server.WithListUsersDispatchThrottlingMaxThreshold(config.ListUsersDispatchThrottling.MaxThreshold),
		server.WithCheckDatabaseThrottle(config.CheckDatastoreThrottle.Threshold, config.CheckDatastoreThrottle.Duration),
		server.WithListObjectsDatabaseThrottle(config.ListObjectsDatastoreThrottle.Threshold, config.ListObjectsDatastoreThrottle.Duration),
		server.WithListUsersDatabaseThrottle(config.ListUsersDatastoreThrottle.Threshold, config.ListUsersDatastoreThrottle.Duration),
		server.WithListObjectsIteratorCacheEnabled(config.ListObjectsIteratorCache.Enabled),
		server.WithListObjectsIteratorCacheMaxResults(config.ListObjectsIteratorCache.MaxResults),
		server.WithListObjectsIteratorCacheTTL(config.ListObjectsIteratorCache.TTL),
		server.WithMaxChecksPerBatchCheck(config.MaxChecksPerBatchCheck),
		server.WithMaxConcurrentChecksPerBatchCheck(config.MaxConcurrentChecksPerBatchCheck),
		server.WithSharedIteratorEnabled(config.SharedIterator.Enabled),
		server.WithSharedIteratorLimit(config.SharedIterator.Limit),
		server.WithPlanner(planner.New(&planner.Config{
			EvictionThreshold: config.Planner.EvictionThreshold,
			CleanupInterval:   config.Planner.CleanupInterval,
		})),
		// The shared iterator watchdog timeout is set to config.RequestTimeout + 2 seconds
		// to provide a small buffer for operations that might slightly exceed the request timeout.
		server.WithSharedIteratorTTL(config.RequestTimeout+2*time.Second),
		server.WithExperimentals(experimentals...),
		server.WithAccessControlParams(config.AccessControl.Enabled, config.AccessControl.StoreID, config.AccessControl.ModelID, config.Authn.Method),
		server.WithContext(ctx),
	)

	s.Logger.Info(
		"starting openfga service...",
		zap.String("version", build.Version),
		zap.String("date", build.Date),
		zap.String("commit", build.Commit),
		zap.String("go-version", goruntime.Version()),
		zap.Any("config", config),
	)

	// nosemgrep: grpc-server-insecure-connection
	grpcServer := grpc.NewServer(serverOpts...)
	openfgav1.RegisterOpenFGAServiceServer(grpcServer, svr)
	healthServer := &health.Checker{TargetService: svr, TargetServiceName: openfgav1.OpenFGAService_ServiceDesc.ServiceName}
	healthv1pb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", config.GRPC.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		s.Logger.Info(fmt.Sprintf("🚀 starting gRPC server on '%s'...", lis.Addr().String()))
		if err := grpcServer.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				s.Logger.Fatal("failed to start gRPC server", zap.Error(err))
			}
		}
		s.Logger.Info("gRPC server shut down.")
	}()

	// Create a Unix domain socket listener for the internal HTTP-to-gRPC proxy.
	udsPath := filepath.Join(os.TempDir(), fmt.Sprintf("openfga-grpc-%d.sock", os.Getpid()))
	_ = os.Remove(udsPath) // clean up stale socket file
	rawUDSLis, err := net.Listen("unix", udsPath)
	if err != nil {
		return fmt.Errorf("failed to listen on unix socket: %w", err)
	}
	udsLis := &addrOverrideListener{
		Listener: rawUDSLis,
		addr:     &net.UnixAddr{Name: udsPath, Net: "unix"},
	}

	go func() {
		s.Logger.Info(fmt.Sprintf("starting gRPC server on unix socket '%s'...", udsPath))
		if err := grpcServer.Serve(udsLis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				s.Logger.Fatal("failed to start gRPC server on unix socket", zap.Error(err))
			}
		}
	}()

	var httpServer *http.Server
	if config.HTTP.Enabled {
		runtime.DefaultContextTimeout = serverconfig.DefaultContextTimeout(config)

		grpcConn := s.dialGrpc(udsPath, config)
		defer grpcConn.Close()

		httpServer, err = s.runHTTPServer(ctx, config, grpcConn)
		if err != nil {
			return err
		}
	}

	var playground *http.Server
	if config.Playground.Enabled {
		playground, err = s.runPlaygroundServer(config)
		if err != nil {
			return err
		}
	}

	// wait for cancellation signal
	<-ctx.Done()
	s.Logger.Info("attempting to shutdown gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if playground != nil {
		if err := playground.Shutdown(ctx); err != nil {
			s.Logger.Info("failed to shutdown the playground", zap.Error(err))
		}
	}

	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil {
			s.Logger.Info("failed to shutdown the http server", zap.Error(err))
		}
	}

	if profilerServer != nil {
		if err := profilerServer.Shutdown(ctx); err != nil {
			s.Logger.Info("failed to shutdown the profiler", zap.Error(err))
		}
	}

	if metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil {
			s.Logger.Info("failed to shutdown the prometheus metrics server", zap.Error(err))
		}
	}

	grpcServer.GracefulStop()

	if err := os.Remove(udsPath); err != nil && !os.IsNotExist(err) {
		s.Logger.Warn("failed to remove unix socket file", zap.Error(err))
	}

	svr.Close()

	authenticator.Close()

	if err := tracerProviderCloser(); err != nil {
		s.Logger.Error("failed to shutdown tracing", zap.Error(err))
	}

	s.Logger.Info("server exited. goodbye 👋")

	return nil
}

// addrOverrideConn wraps a net.Conn to return a fixed remote address.
// This is used for UDS connections where RemoteAddr() would otherwise be empty.
type addrOverrideConn struct {
	net.Conn
	addr net.Addr
}

func (c *addrOverrideConn) RemoteAddr() net.Addr { return c.addr }

// addrOverrideListener wraps a net.Listener so that accepted connections
// report the given address as their RemoteAddr.
type addrOverrideListener struct {
	net.Listener
	addr net.Addr
}

func (l *addrOverrideListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &addrOverrideConn{Conn: conn, addr: l.addr}, nil
}

func watchAndLoadCertificateWithCertWatcher(ctx context.Context, certPath, keyPath string, logger logger.Logger) (func(*tls.ClientHelloInfo) (*tls.Certificate, error), error) {
	log.SetLogger(logr.New(nil))
	// Create a certificate watcher
	watcher, err := certwatcher.New(certPath, keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create certwatcher: %w", err)
	}

	// Load the initial certificate
	if err := watcher.ReadCertificate(); err != nil {
		return nil, fmt.Errorf("failed to load initial certificate: %w", err)
	}
	logger.Info("Initial TLS certificate loaded.", zap.String("certPath", certPath), zap.String("keyPath", keyPath))

	// Start watching for certificate changes
	go func() {
		logger.Info("Starting certificate watcher...", zap.String("certPath", certPath), zap.String("keyPath", keyPath))
		if err := watcher.Start(ctx); err != nil {
			logger.Error("Certwatcher encountered an error", zap.Error(err))
		}
	}()

	// Return a function that retrieves the updated certificate
	getCertificate := func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		return watcher.GetCertificate(nil)
	}

	return getCertificate, nil
}
