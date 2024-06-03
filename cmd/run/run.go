// Package run contains the command to run an OpenFGA server.
package run

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	goruntime "runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	grpc_prometheus "github.com/jon-whit/go-grpc-prometheus"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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

	"github.com/openfga/openfga/pkg/gateway"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/internal/authn/oidc"
	"github.com/openfga/openfga/internal/authn/presharedkey"
	"github.com/openfga/openfga/internal/build"
	authnmw "github.com/openfga/openfga/internal/middleware/authn"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/middleware"
	httpmiddleware "github.com/openfga/openfga/pkg/middleware/http"
	"github.com/openfga/openfga/pkg/middleware/logging"
	"github.com/openfga/openfga/pkg/middleware/recovery"
	"github.com/openfga/openfga/pkg/middleware/requestid"
	"github.com/openfga/openfga/pkg/middleware/storeid"
	"github.com/openfga/openfga/pkg/middleware/validator"
	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/server/health"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
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

	flags.StringSlice("experimentals", defaultConfig.Experimentals, "a list of experimental features to enable. Allowed values: `enable-list-users`")

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

	flags.StringSlice("authn-oidc-issuer-aliases", defaultConfig.Authn.IssuerAliases, "the OIDC issuer DNS aliases that will be accepted as valid when verifying tokens")

	flags.String("datastore-engine", defaultConfig.Datastore.Engine, "the datastore engine that will be used for persistence")

	flags.String("datastore-uri", defaultConfig.Datastore.URI, "the connection uri to use to connect to the datastore (for any engine other than 'memory')")

	flags.String("datastore-username", "", "the connection username to use to connect to the datastore (overwrites any username provided in the connection uri)")

	flags.String("datastore-password", "", "the connection password to use to connect to the datastore (overwrites any password provided in the connection uri)")

	flags.Int("datastore-max-cache-size", defaultConfig.Datastore.MaxCacheSize, "the maximum number of authorization models that will be cached in memory")

	flags.Int("datastore-max-open-conns", defaultConfig.Datastore.MaxOpenConns, "the maximum number of open connections to the datastore")

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

	flags.Bool("metrics-enabled", defaultConfig.Metrics.Enabled, "enable/disable prometheus metrics on the '/metrics' endpoint")

	flags.String("metrics-addr", defaultConfig.Metrics.Addr, "the host:port address to serve the prometheus metrics server on")

	flags.Bool("metrics-enable-rpc-histograms", defaultConfig.Metrics.EnableRPCHistograms, "enables prometheus histogram metrics for RPC latency distributions")

	flags.Int("max-tuples-per-write", defaultConfig.MaxTuplesPerWrite, "the maximum allowed number of tuples per Write transaction")

	flags.Int("max-types-per-authorization-model", defaultConfig.MaxTypesPerAuthorizationModel, "the maximum allowed number of type definitions per authorization model")

	flags.Int("max-authorization-model-size-in-bytes", defaultConfig.MaxAuthorizationModelSizeInBytes, "the maximum size in bytes allowed for persisting an Authorization Model.")

	flags.Uint32("max-concurrent-reads-for-list-users", defaultConfig.MaxConcurrentReadsForListUsers, "the maximum allowed number of concurrent datastore reads in a single ListUsers query. A high number will consume more connections from the datastore pool and will attempt to prioritize performance for the request at the expense of other queries performance.")

	flags.Uint32("max-concurrent-reads-for-list-objects", defaultConfig.MaxConcurrentReadsForListObjects, "the maximum allowed number of concurrent datastore reads in a single ListObjects or StreamedListObjects query. A high number will consume more connections from the datastore pool and will attempt to prioritize performance for the request at the expense of other queries performance.")

	flags.Uint32("max-concurrent-reads-for-check", defaultConfig.MaxConcurrentReadsForCheck, "the maximum allowed number of concurrent datastore reads in a single Check query. A high number will consume more connections from the datastore pool and will attempt to prioritize performance for the request at the expense of other queries performance.")

	flags.Int("changelog-horizon-offset", defaultConfig.ChangelogHorizonOffset, "the offset (in minutes) from the current time. Changes that occur after this offset will not be included in the response of ReadChanges")

	flags.Uint32("resolve-node-limit", defaultConfig.ResolveNodeLimit, "maximum resolution depth to attempt before throwing an error (defines how deeply nested an authorization model can be before a query errors out).")

	flags.Uint32("resolve-node-breadth-limit", defaultConfig.ResolveNodeBreadthLimit, "defines how many nodes on a given level can be evaluated concurrently in a Check resolution tree")

	flags.Duration("listObjects-deadline", defaultConfig.ListObjectsDeadline, "the timeout deadline for serving ListObjects and StreamedListObjects requests")

	flags.Uint32("listObjects-max-results", defaultConfig.ListObjectsMaxResults, "the maximum results to return in non-streaming ListObjects API responses. If 0, all results can be returned")

	flags.Duration("listUsers-deadline", defaultConfig.ListUsersDeadline, "the timeout deadline for serving ListUsers requests. If 0, there is no deadline")

	flags.Uint32("listUsers-max-results", defaultConfig.ListUsersMaxResults, "the maximum results to return in ListUsers API responses. If 0, all results can be returned")

	flags.Bool("check-query-cache-enabled", defaultConfig.CheckQueryCache.Enabled, "when executing Check and ListObjects requests, enables caching. This will turn Check and ListObjects responses into eventually consistent responses")

	flags.Uint32("check-query-cache-limit", defaultConfig.CheckQueryCache.Limit, "if caching of Check and ListObjects calls is enabled, this is the size limit of the cache")

	flags.Duration("check-query-cache-ttl", defaultConfig.CheckQueryCache.TTL, "if caching of Check and ListObjects is enabled, this is the TTL of each value")

	// Unfortunately UintSlice/IntSlice does not work well when used as environment variable, we need to stick with string slice and convert back to integer
	flags.StringSlice("request-duration-datastore-query-count-buckets", defaultConfig.RequestDurationDatastoreQueryCountBuckets, "datastore query count buckets used in labelling request_duration_ms.")

	flags.StringSlice("request-duration-dispatch-count-buckets", defaultConfig.RequestDurationDispatchCountBuckets, "dispatch count (i.e number of concurrent traversals to resolve a query) buckets used in labelling request_duration_ms.")

	flags.Bool("check-dispatch-throttling-enabled", defaultConfig.CheckDispatchThrottling.Enabled, "enable throttling for Check requests when the request's number of dispatches is high. Enabling this feature will prioritize dispatched requests requiring less than the configured dispatch threshold over requests whose dispatch count exceeds the configured threshold.")

	flags.Duration("check-dispatch-throttling-frequency", defaultConfig.CheckDispatchThrottling.Frequency, "defines how frequent Check dispatch throttling will be evaluated. This controls how frequently throttled dispatch Check requests are dispatched.")

	flags.Uint32("check-dispatch-throttling-threshold", defaultConfig.CheckDispatchThrottling.Threshold, "define the number of dispatches above which Check requests will be throttled.")

	flags.Uint32("check-dispatch-throttling-max-threshold", defaultConfig.CheckDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold beyond which a Check requests will be throttled. 0 will use the 'check-dispatch-throttling-threshold' value as maximum")

	flags.Bool("listObjects-dispatch-throttling-enabled", defaultConfig.ListObjectsDispatchThrottling.Enabled, "enable throttling when a ListObjects request's number of dispatches is high. Enabling this feature will prioritize dispatched requests requiring less than the configured dispatch threshold over requests whose dispatch count exceeds the configured threshold.")

	flags.Duration("listObjects-dispatch-throttling-frequency", defaultConfig.ListObjectsDispatchThrottling.Frequency, "defines how frequent ListObjects dispatch throttling will be evaluated. Frequency controls how frequently throttled dispatch ListObjects requests are dispatched.")

	flags.Uint32("listObjects-dispatch-throttling-threshold", defaultConfig.ListObjectsDispatchThrottling.Threshold, "defines the number of dispatches above which ListObjects requests will be throttled.")

	flags.Uint32("listObjects-dispatch-throttling-max-threshold", defaultConfig.ListObjectsDispatchThrottling.MaxThreshold, "define the maximum dispatch threshold beyond which a list objects requests will be throttled. 0 will use the 'listObjects-dispatch-throttling-threshold' value as maximum")

	flags.Bool("dispatch-throttling-enabled", defaultConfig.DispatchThrottling.Enabled, `DEPRECATED: Use check-dispatch-throttling-enabled instead.
    
    Enable throttling for Check requests when the request's number of dispatches is high. Enabling this feature will prioritize dispatched requests requiring less than the configured dispatch threshold over requests whose dispatch count exceeds the configured threshold.`)

	flags.Duration("dispatch-throttling-frequency", defaultConfig.DispatchThrottling.Frequency, `DEPRECATED: Use check-dispatch-throttling-frequency instead. 
    
    Defines how frequent Check dispatch throttling will be evaluated. Frequency controls how frequently throttled dispatch Check requests are dispatched.`)

	flags.Uint32("dispatch-throttling-threshold", defaultConfig.DispatchThrottling.Threshold, `DEPRECATED: Use check-dispatch-throttling-threshold instead. 

	Define the default threshold on number of dispatches above which requests will be throttled.`)

	flags.Uint32("dispatch-throttling-max-threshold", defaultConfig.DispatchThrottling.MaxThreshold, `DEPRECATED: Use check-dispatch-throttling-max-threshold instead. 

	Define the maximum dispatch threshold beyond which requests will be throttled. 0 will use the 'dispatch-throttling-threshold' value as maximum`)

	flags.Duration("request-timeout", defaultConfig.RequestTimeout, "configures request timeout.  If both HTTP upstream timeout and request timeout are specified, request timeout will be used.")

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
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
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
func (s *ServerContext) telemetryConfig(config *serverconfig.Config) func(ctx context.Context) error {
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
		return func(ctx context.Context) error {
			return errors.Join(tp.ForceFlush(ctx), tp.Shutdown(ctx))
		}
	}
	otel.SetTracerProvider(noop.NewTracerProvider())
	return func(ctx context.Context) error {
		return nil
	}
}

func (s *ServerContext) datastoreConfig(config *serverconfig.Config) (storage.OpenFGADatastore, error) {
	datastoreOptions := []sqlcommon.DatastoreOption{
		sqlcommon.WithUsername(config.Datastore.Username),
		sqlcommon.WithPassword(config.Datastore.Password),
		sqlcommon.WithLogger(s.Logger),
		sqlcommon.WithMaxTuplesPerWrite(config.MaxTuplesPerWrite),
		sqlcommon.WithMaxTypesPerAuthorizationModel(config.MaxTypesPerAuthorizationModel),
		sqlcommon.WithMaxOpenConns(config.Datastore.MaxOpenConns),
		sqlcommon.WithMaxIdleConns(config.Datastore.MaxIdleConns),
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
		opts := []memory.StorageOption{
			memory.WithMaxTypesPerAuthorizationModel(config.MaxTypesPerAuthorizationModel),
			memory.WithMaxTuplesPerWrite(config.MaxTuplesPerWrite),
		}
		datastore = memory.New(opts...)
	case "mysql":
		datastore, err = mysql.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return nil, fmt.Errorf("initialize mysql datastore: %w", err)
		}
	case "postgres":
		datastore, err = postgres.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return nil, fmt.Errorf("initialize postgres datastore: %w", err)
		}
	default:
		return nil, fmt.Errorf("storage engine '%s' is unsupported", config.Datastore.Engine)
	}

	s.Logger.Info(fmt.Sprintf("using '%v' storage engine", config.Datastore.Engine))
	return datastore, nil
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
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.Authn.Issuer, config.Authn.IssuerAliases, config.Authn.Audience)
	default:
		return nil, fmt.Errorf("unsupported authentication method '%v'", config.Authn.Method)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authenticator: %w", err)
	}
	return authenticator, nil
}

// Run returns an error if the server was unable to start successfully.
// If it started and terminated successfully, it returns a nil error.
func (s *ServerContext) Run(ctx context.Context, config *serverconfig.Config) error {
	tracerProviderCloser := s.telemetryConfig(config)

	if len(config.Experimentals) > 0 {
		s.Logger.Info(fmt.Sprintf("🧪 experimental features enabled: %v", config.Experimentals))
	}

	var experimentals []server.ExperimentalFeatureFlag
	for _, feature := range config.Experimentals {
		experimentals = append(experimentals, server.ExperimentalFeatureFlag(feature))
	}

	datastore, err := s.datastoreConfig(config)
	if err != nil {
		return err
	}

	authenticator, err := s.authenticatorConfig(config)

	if err != nil {
		return err
	}

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
				grpc_recovery.StreamServerInterceptor(
					grpc_recovery.WithRecoveryHandlerContext(
						recovery.PanicRecoveryHandler(s.Logger),
					),
				),
				requestid.NewStreamingInterceptor(),
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
				grpc_ctxtags.StreamServerInterceptor(),
			}...,
		),
	)

	if config.Metrics.Enabled {
		serverOpts = append(serverOpts,
			grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
			grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor))

		if config.Metrics.EnableRPCHistograms {
			grpc_prometheus.EnableHandlingTimeHistogram()
		}
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
			return errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
		}
		creds, err := credentials.NewServerTLSFromFile(config.GRPC.TLS.CertPath, config.GRPC.TLS.KeyPath)
		if err != nil {
			return err
		}

		serverOpts = append(serverOpts, grpc.Creds(creds))

		s.Logger.Info("gRPC TLS is enabled, serving connections using the provided certificate")
	} else {
		s.Logger.Warn("gRPC TLS is disabled, serving connections using insecure plaintext")
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
				if err != http.ErrServerClosed {
					s.Logger.Fatal("failed to start pprof profiler", zap.Error(err))
				}
			}
			s.Logger.Info("profiler shut down.")
		}()
	}

	var metricsServer *http.Server
	if config.Metrics.Enabled {
		s.Logger.Info(fmt.Sprintf("📈 starting prometheus metrics server on '%s'", config.Metrics.Addr))

		go func() {
			mux := http.NewServeMux()
			mux.Handle("/metrics", promhttp.Handler())

			metricsServer = &http.Server{Addr: config.Metrics.Addr, Handler: mux}
			if err := metricsServer.ListenAndServe(); err != nil {
				if err != http.ErrServerClosed {
					s.Logger.Fatal("failed to start prometheus metrics server", zap.Error(err))
				}
			}
			s.Logger.Info("metrics server shut down.")
		}()
	}

	checkDispatchThrottlingConfig := serverconfig.GetCheckDispatchThrottlingConfig(s.Logger, config)

	svr := server.MustNewServerWithOpts(
		server.WithDatastore(datastore),
		server.WithAuthorizationModelCacheSize(config.Datastore.MaxCacheSize),
		server.WithLogger(s.Logger),
		server.WithTransport(gateway.NewRPCTransport(s.Logger)),
		server.WithResolveNodeLimit(config.ResolveNodeLimit),
		server.WithResolveNodeBreadthLimit(config.ResolveNodeBreadthLimit),
		server.WithChangelogHorizonOffset(config.ChangelogHorizonOffset),
		server.WithListObjectsDeadline(config.ListObjectsDeadline),
		server.WithListObjectsMaxResults(config.ListObjectsMaxResults),
		server.WithListUsersDeadline(config.ListUsersDeadline),
		server.WithListUsersMaxResults(config.ListUsersMaxResults),
		server.WithMaxConcurrentReadsForListObjects(config.MaxConcurrentReadsForListObjects),
		server.WithMaxConcurrentReadsForCheck(config.MaxConcurrentReadsForCheck),
		server.WithMaxConcurrentReadsForListUsers(config.MaxConcurrentReadsForListUsers),
		server.WithCheckQueryCacheEnabled(config.CheckQueryCache.Enabled),
		server.WithCheckQueryCacheLimit(config.CheckQueryCache.Limit),
		server.WithCheckQueryCacheTTL(config.CheckQueryCache.TTL),
		server.WithRequestDurationByQueryHistogramBuckets(convertStringArrayToUintArray(config.RequestDurationDatastoreQueryCountBuckets)),
		server.WithRequestDurationByDispatchCountHistogramBuckets(convertStringArrayToUintArray(config.RequestDurationDispatchCountBuckets)),
		server.WithMaxAuthorizationModelSizeInBytes(config.MaxAuthorizationModelSizeInBytes),
		server.WithDispatchThrottlingCheckResolverEnabled(checkDispatchThrottlingConfig.Enabled),
		server.WithDispatchThrottlingCheckResolverFrequency(checkDispatchThrottlingConfig.Frequency),
		server.WithDispatchThrottlingCheckResolverThreshold(checkDispatchThrottlingConfig.Threshold),
		server.WithDispatchThrottlingCheckResolverMaxThreshold(checkDispatchThrottlingConfig.MaxThreshold),
		server.WithListObjectsDispatchThrottlingEnabled(config.ListObjectsDispatchThrottling.Enabled),
		server.WithListObjectsDispatchThrottlingFrequency(config.ListObjectsDispatchThrottling.Frequency),
		server.WithListObjectsDispatchThrottlingThreshold(config.ListObjectsDispatchThrottling.Threshold),
		server.WithListObjectsDispatchThrottlingMaxThreshold(config.ListObjectsDispatchThrottling.MaxThreshold),
		server.WithExperimentals(experimentals...),
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
		s.Logger.Info(fmt.Sprintf("🚀 starting gRPC server on '%s'...", config.GRPC.Addr))
		if err := grpcServer.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				s.Logger.Fatal("failed to start gRPC server", zap.Error(err))
			}
		}
		s.Logger.Info("gRPC server shut down.")
	}()

	var httpServer *http.Server
	if config.HTTP.Enabled {
		runtime.DefaultContextTimeout = serverconfig.DefaultContextTimeout(config)

		dialOpts := []grpc.DialOption{
			// nolint:staticcheck // ignoring gRPC deprecations
			grpc.WithBlock(),
		}
		if config.GRPC.TLS.Enabled {
			creds, err := credentials.NewClientTLSFromFile(config.GRPC.TLS.CertPath, "")
			if err != nil {
				s.Logger.Fatal("", zap.Error(err))
			}
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
		} else {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		// nolint:staticcheck // ignoring gRPC deprecations
		conn, err := grpc.DialContext(timeoutCtx, config.GRPC.Addr, dialOpts...)
		if err != nil {
			s.Logger.Fatal("", zap.Error(err))
		}
		defer conn.Close()

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
			runtime.WithHealthzEndpoint(healthv1pb.NewHealthClient(conn)),
			runtime.WithOutgoingHeaderMatcher(func(s string) (string, bool) { return s, true }),
		}
		mux := runtime.NewServeMux(muxOpts...)
		if err := openfgav1.RegisterOpenFGAServiceHandler(ctx, mux, conn); err != nil {
			return err
		}

		httpServer = &http.Server{
			Addr: config.HTTP.Addr,
			Handler: recovery.HTTPPanicRecoveryHandler(cors.New(cors.Options{
				AllowedOrigins:   config.HTTP.CORSAllowedOrigins,
				AllowCredentials: true,
				AllowedHeaders:   config.HTTP.CORSAllowedHeaders,
				AllowedMethods: []string{http.MethodGet, http.MethodPost,
					http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodPut},
			}).Handler(mux), s.Logger),
		}

		go func() {
			s.Logger.Info(fmt.Sprintf("🚀 starting HTTP server on '%s'...", httpServer.Addr))
			var err error
			if config.HTTP.TLS.Enabled {
				if config.HTTP.TLS.CertPath == "" || config.HTTP.TLS.KeyPath == "" {
					s.Logger.Fatal("'http.tls.cert' and 'http.tls.key' configs must be set")
				}
				err = httpServer.ListenAndServeTLS(config.HTTP.TLS.CertPath, config.HTTP.TLS.KeyPath)
			} else {
				s.Logger.Warn("HTTP TLS is disabled, serving connections using insecure plaintext")
				err = httpServer.ListenAndServe()
			}
			if err != http.ErrServerClosed {
				s.Logger.Fatal("HTTP server closed with unexpected error", zap.Error(err))
			}
			s.Logger.Info("HTTP server shut down.")
		}()
	}

	var playground *http.Server
	if config.Playground.Enabled {
		if !config.HTTP.Enabled {
			return errors.New("the HTTP server must be enabled to run the openfga playground")
		}

		authMethod := config.Authn.Method
		if !(authMethod == "none" || authMethod == "preshared") {
			return errors.New("the playground only supports authn methods 'none' and 'preshared'")
		}

		playgroundAddr := fmt.Sprintf(":%d", config.Playground.Port)
		s.Logger.Info(fmt.Sprintf("🛝 starting openfga playground on http://localhost%s/playground", playgroundAddr))

		tmpl, err := template.ParseFS(assets.EmbedPlayground, "playground/index.html")
		if err != nil {
			return fmt.Errorf("failed to parse playground index.html as Go template: %w", err)
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
			return fmt.Errorf("failed to establish playground connection to HTTP server: %w", err)
		}

		playgroundAPIToken := ""
		if authMethod == "preshared" {
			playgroundAPIToken = config.Authn.AuthnPresharedKeyConfig.Keys[0]
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

		playground = &http.Server{Addr: playgroundAddr, Handler: mux}

		go func() {
			err = playground.ListenAndServe()
			if err != http.ErrServerClosed {
				s.Logger.Fatal("failed to start the openfga playground server", zap.Error(err))
			}
			s.Logger.Info("playground shut down.")
		}()
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
	case <-ctx.Done():
	}
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

	svr.Close()

	authenticator.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := tracerProviderCloser(ctx); err != nil {
		s.Logger.Error("failed to shutdown tracing", zap.Error(err))
	}

	s.Logger.Info("server exited. goodbye 👋")

	return nil
}
