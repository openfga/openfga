package run

import (
	"github.com/openfga/openfga/cmd/util"
	"github.com/spf13/cobra"
)

// bindRunFlags binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindRunFlags(command *cobra.Command) {
	defaultConfig := DefaultConfig()
	flags := command.Flags()

	flags.StringSlice("experimentals", defaultConfig.Experimentals, "a list of experimental features to enable")
	util.MustBindPFlag("experimentals", flags.Lookup("experimentals"))
	util.MustBindEnv("experimentals", "OPENFGA_EXPERIMENTALS")

	flags.String("grpc-addr", defaultConfig.GRPC.Addr, "the host:port address to serve the grpc server on")
	util.MustBindPFlag("grpc.addr", flags.Lookup("grpc-addr"))
	util.MustBindEnv("grpc.addr", "OPENFGA_GRPC_ADDR")

	flags.Bool("grpc-tls-enabled", defaultConfig.GRPC.TLS.Enabled, "enable/disable transport layer security (TLS)")
	util.MustBindPFlag("grpc.tls.enabled", flags.Lookup("grpc-tls-enabled"))
	util.MustBindEnv("grpc.tls.enabled", "OPENFGA_GRPC_TLS_ENABLED")

	flags.String("grpc-tls-cert", defaultConfig.GRPC.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")
	util.MustBindPFlag("grpc.tls.cert", flags.Lookup("grpc-tls-cert"))
	util.MustBindEnv("grpc.tls.cert", "OPENFGA_GRPC_TLS_CERT")

	flags.String("grpc-tls-key", defaultConfig.GRPC.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")
	util.MustBindPFlag("grpc.tls.key", flags.Lookup("grpc-tls-key"))
	util.MustBindEnv("grpc.tls.key", "OPENFGA_GRPC_TLS_KEY")

	command.MarkFlagsRequiredTogether("grpc-tls-enabled", "grpc-tls-cert", "grpc-tls-key")

	flags.Bool("http-enabled", defaultConfig.HTTP.Enabled, "enable/disable the OpenFGA HTTP server")
	util.MustBindPFlag("http.enabled", flags.Lookup("http-enabled"))
	util.MustBindEnv("http.enabled", "OPENFGA_HTTP_ENABLED")

	flags.String("http-addr", defaultConfig.HTTP.Addr, "the host:port address to serve the HTTP server on")
	util.MustBindPFlag("http.addr", flags.Lookup("http-addr"))
	util.MustBindEnv("http.addr", "OPENFGA_HTTP_ADDR")

	flags.Bool("http-tls-enabled", defaultConfig.HTTP.TLS.Enabled, "enable/disable transport layer security (TLS)")
	util.MustBindPFlag("http.tls.enabled", flags.Lookup("http-tls-enabled"))
	util.MustBindEnv("http.tls.enabled", "OPENFGA_HTTP_TLS_ENABLED")

	flags.String("http-tls-cert", defaultConfig.HTTP.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")
	util.MustBindPFlag("http.tls.cert", flags.Lookup("http-tls-cert"))
	util.MustBindEnv("http.tls.cert", "OPENFGA_HTTP_TLS_CERT")

	flags.String("http-tls-key", defaultConfig.HTTP.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")
	util.MustBindPFlag("http.tls.key", flags.Lookup("http-tls-key"))
	util.MustBindEnv("http.tls.key", "OPENFGA_HTTP_TLS_KEY")

	command.MarkFlagsRequiredTogether("http-tls-enabled", "http-tls-cert", "http-tls-key")

	flags.Duration("http-upstream-timeout", defaultConfig.HTTP.UpstreamTimeout, "the timeout duration for proxying HTTP requests upstream to the grpc endpoint")
	util.MustBindPFlag("http.upstreamTimeout", flags.Lookup("http-upstream-timeout"))
	util.MustBindEnv("http.upstreamTimeout", "OPENFGA_HTTP_UPSTREAM_TIMEOUT", "OPENFGA_HTTP_UPSTREAMTIMEOUT")

	flags.StringSlice("http-cors-allowed-origins", defaultConfig.HTTP.CORSAllowedOrigins, "specifies the CORS allowed origins")
	util.MustBindPFlag("http.corsAllowedOrigins", flags.Lookup("http-cors-allowed-origins"))
	util.MustBindEnv("http.corsAllowedOrigins", "OPENFGA_HTTP_CORS_ALLOWED_ORIGINS", "OPENFGA_HTTP_CORSALLOWEDORIGINS")

	flags.StringSlice("http-cors-allowed-headers", defaultConfig.HTTP.CORSAllowedHeaders, "specifies the CORS allowed headers")
	util.MustBindPFlag("http.corsAllowedHeaders", flags.Lookup("http-cors-allowed-headers"))
	util.MustBindEnv("http.corsAllowedHeaders", "OPENFGA_HTTP_CORS_ALLOWED_HEADERS", "OPENFGA_HTTP_CORSALLOWEDHEADERS")

	flags.String("authn-method", defaultConfig.Authn.Method, "the authentication method to use")
	util.MustBindPFlag("authn.method", flags.Lookup("authn-method"))
	util.MustBindEnv("authn.method", "OPENFGA_AUTHN_METHOD")

	flags.StringSlice("authn-preshared-keys", defaultConfig.Authn.Keys, "one or more preshared keys to use for authentication")
	util.MustBindPFlag("authn.preshared.keys", flags.Lookup("authn-preshared-keys"))
	util.MustBindEnv("authn.preshared.keys", "OPENFGA_AUTHN_PRESHARED_KEYS")

	flags.String("authn-oidc-audience", defaultConfig.Authn.Audience, "the OIDC audience of the tokens being signed by the authorization server")
	util.MustBindPFlag("authn.oidc.audience", flags.Lookup("authn-oidc-audience"))
	util.MustBindEnv("authn.oidc.audience", "OPENFGA_AUTHN_OIDC_AUDIENCE")

	flags.String("authn-oidc-issuer", defaultConfig.Authn.Issuer, "the OIDC issuer (authorization server) signing the tokens")
	util.MustBindPFlag("authn.oidc.issuer", flags.Lookup("authn-oidc-issuer"))
	util.MustBindEnv("authn.oidc.issuer", "OPENFGA_AUTHN_OIDC_ISSUER")

	flags.String("datastore-engine", defaultConfig.Datastore.Engine, "the datastore engine that will be used for persistence")
	util.MustBindPFlag("datastore.engine", flags.Lookup("datastore-engine"))
	util.MustBindEnv("datastore.engine", "OPENFGA_DATASTORE_ENGINE")

	flags.String("datastore-uri", defaultConfig.Datastore.URI, "the connection uri to use to connect to the datastore (for any engine other than 'memory')")
	util.MustBindPFlag("datastore.uri", flags.Lookup("datastore-uri"))
	util.MustBindEnv("datastore.uri", "OPENFGA_DATASTORE_URI")

	flags.Int("datastore-max-cache-size", defaultConfig.Datastore.MaxCacheSize, "the maximum number of cache keys that the storage cache can store before evicting old keys")
	util.MustBindPFlag("datastore.maxCacheSize", flags.Lookup("datastore-max-cache-size"))
	util.MustBindEnv("datastore.maxCacheSize", "OPENFGA_DATASTORE_MAX_CACHE_SIZE", "OPENFGA_DATASTORE_MAXCACHESIZE")

	flags.Int("datastore-max-open-conns", defaultConfig.Datastore.MaxOpenConns, "the maximum number of open connections to the datastore")
	util.MustBindPFlag("datastore.maxOpenConns", flags.Lookup("datastore-max-open-conns"))
	util.MustBindEnv("datastore.maxOpenConns", "OPENFGA_DATASTORE_MAX_OPEN_CONNS", "OPENFGA_DATASTORE_MAXOPENCONNS")

	flags.Int("datastore-max-idle-conns", defaultConfig.Datastore.MaxIdleConns, "the maximum number of connections to the datastore in the idle connection pool")
	util.MustBindPFlag("datastore.maxIdleConns", flags.Lookup("datastore-max-idle-conns"))
	util.MustBindEnv("datastore.maxIdleConns", "OPENFGA_DATASTORE_MAX_IDLE_CONNS", "OPENFGA_DATASTORE_MAXIDLECONNS")

	flags.Duration("datastore-conn-max-idle-time", defaultConfig.Datastore.ConnMaxIdleTime, "the maximum amount of time a connection to the datastore may be idle")
	util.MustBindPFlag("datastore.connMaxIdleTime", flags.Lookup("datastore-conn-max-idle-time"))
	util.MustBindEnv("datastore.connMaxIdleTime", "OPENFGA_DATASTORE_CONN_MAX_IDLE_TIME", "OPENFGA_DATASTORE_CONNMAXIDLETIME")

	flags.Duration("datastore-conn-max-lifetime", defaultConfig.Datastore.ConnMaxLifetime, "the maximum amount of time a connection to the datastore may be reused")
	util.MustBindPFlag("datastore.connMaxLifetime", flags.Lookup("datastore-conn-max-lifetime"))
	util.MustBindEnv("datastore.connMaxLifetime", "OPENFGA_DATASTORE_CONN_MAX_LIFETIME", "OPENFGA_DATASTORE_CONNMAXLIFETIME")

	flags.Bool("playground-enabled", defaultConfig.Playground.Enabled, "enable/disable the OpenFGA Playground")
	util.MustBindPFlag("playground.enabled", flags.Lookup("playground-enabled"))
	util.MustBindEnv("playground.enabled", "OPENFGA_PLAYGROUND_ENABLED")

	flags.Int("playground-port", defaultConfig.Playground.Port, "the port to serve the local OpenFGA Playground on")
	util.MustBindPFlag("playground.port", flags.Lookup("playground-port"))
	util.MustBindEnv("playground.port", "OPENFGA_PLAYGROUND_PORT")

	flags.Bool("profiler-enabled", defaultConfig.Profiler.Enabled, "enable/disable pprof profiling")
	util.MustBindPFlag("profiler.enabled", flags.Lookup("profiler-enabled"))
	util.MustBindEnv("profiler.enabled", "OPENFGA_PROFILER_ENABLED")

	flags.String("profiler-addr", defaultConfig.Profiler.Addr, "the host:port address to serve the pprof profiler server on")
	util.MustBindPFlag("profiler.addr", flags.Lookup("profiler-addr"))
	util.MustBindEnv("profiler.addr", "OPENFGA_PROFILER_ADDRESS")

	flags.String("log-format", defaultConfig.Log.Format, "the log format to output logs in")
	util.MustBindPFlag("log.format", flags.Lookup("log-format"))
	util.MustBindEnv("log.format", "OPENFGA_LOG_FORMAT")

	flags.String("log-level", defaultConfig.Log.Level, "the log level to use")
	util.MustBindPFlag("log.level", flags.Lookup("log-level"))
	util.MustBindEnv("log.level", "OPENFGA_LOG_LEVEL")

	flags.Bool("trace-enabled", defaultConfig.Trace.Enabled, "enable tracing")
	util.MustBindPFlag("trace.enabled", flags.Lookup("trace-enabled"))
	util.MustBindEnv("trace.enabled", "OPENFGA_TRACE_ENABLED")

	flags.String("trace-otlp-endpoint", defaultConfig.Trace.OTLP.Endpoint, "the endpoint of the trace collector")
	util.MustBindPFlag("trace.otlp.endpoint", flags.Lookup("trace-otlp-endpoint"))
	util.MustBindEnv("trace.otlp.endpoint", "OPENFGA_TRACE_OTLP_ENDPOINT")

	flags.Float64("trace-sample-ratio", defaultConfig.Trace.SampleRatio, "the fraction of traces to sample. 1 means all, 0 means none.")
	util.MustBindPFlag("trace.sampleRatio", flags.Lookup("trace-sample-ratio"))
	util.MustBindEnv("trace.sampleRatio", "OPENFGA_TRACE_SAMPLE_RATIO")

	flags.Bool("metrics-enabled", defaultConfig.Metrics.Enabled, "enable/disable prometheus metrics on the '/metrics' endpoint")
	util.MustBindPFlag("metrics.enabled", flags.Lookup("metrics-enabled"))
	util.MustBindEnv("metrics.enabled", "OPENFGA_METRICS_ENABLED")

	flags.String("metrics-addr", defaultConfig.Metrics.Addr, "the host:port address to serve the prometheus metrics server on")
	util.MustBindPFlag("metrics.addr", flags.Lookup("metrics-addr"))
	util.MustBindEnv("metrics.addr", "OPENFGA_METRICS_ADDR")

	flags.Bool("metrics-enable-rpc-histograms", defaultConfig.Metrics.EnableRPCHistograms, "enables prometheus histogram metrics for RPC latency distributions")
	util.MustBindPFlag("metrics.enableRPCHistograms", flags.Lookup("metrics-enable-rpc-histograms"))
	util.MustBindEnv("metrics.enableRPCHistograms", "OPENFGA_METRICS_ENABLE_RPC_HISTOGRAMS")

	flags.Int("max-tuples-per-write", defaultConfig.MaxTuplesPerWrite, "the maximum allowed number of tuples per Write transaction")
	util.MustBindPFlag("maxTuplesPerWrite", flags.Lookup("max-tuples-per-write"))
	util.MustBindEnv("maxTuplesPerWrite", "OPENFGA_MAX_TUPLES_PER_WRITE", "OPENFGA_MAXTUPLESPERWRITE")

	flags.Int("max-types-per-authorization-model", defaultConfig.MaxTypesPerAuthorizationModel, "the maximum allowed number of type definitions per authorization model")
	util.MustBindPFlag("maxTypesPerAuthorizationModel", flags.Lookup("max-types-per-authorization-model"))
	util.MustBindEnv("maxTypesPerAuthorizationModel", "OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "OPENFGA_MAXTYPESPERAUTHORIZATIONMODEL")

	flags.Int("changelog-horizon-offset", defaultConfig.ChangelogHorizonOffset, "the offset (in minutes) from the current time. Changes that occur after this offset will not be included in the response of ReadChanges")
	util.MustBindPFlag("changelogHorizonOffset", flags.Lookup("changelog-horizon-offset"))
	util.MustBindEnv("changelogHorizonOffset", "OPENFGA_CHANGELOG_HORIZON_OFFSET", "OPENFGA_CHANGELOGHORIZONOFFSET")

	flags.Uint32("resolve-node-limit", defaultConfig.ResolveNodeLimit, "defines how deeply nested an authorization model can be")
	util.MustBindPFlag("resolveNodeLimit", flags.Lookup("resolve-node-limit"))
	util.MustBindEnv("resolveNodeLimit", "OPENFGA_RESOLVE_NODE_LIMIT", "OPENFGA_RESOLVENODELIMIT")

	flags.Duration("listObjects-deadline", defaultConfig.ListObjectsDeadline, "the timeout deadline for serving ListObjects requests")
	util.MustBindPFlag("listObjectsDeadline", flags.Lookup("listObjects-deadline"))
	util.MustBindEnv("listObjectsDeadline", "OPENFGA_LIST_OBJECTS_DEADLINE", "OPENFGA_LISTOBJECTSDEADLINE")

	flags.Uint32("listObjects-max-results", defaultConfig.ListObjectsMaxResults, "the maximum results to return in ListObjects responses")
	util.MustBindPFlag("listObjectsMaxResults", flags.Lookup("listObjects-max-results"))
	util.MustBindEnv("listObjectsMaxResults", "OPENFGA_LIST_OBJECTS_MAX_RESULTS", "OPENFGA_LISTOBJECTSMAXRESULTS")

	flags.Bool("allow-writing-1.0-models", defaultConfig.AllowWriting1_0Models, "allow writing of models with 1.0 schema")
	util.MustBindPFlag("allowWriting1.0Models", flags.Lookup("allow-writing-1.0-models"))
	util.MustBindEnv("allowWriting1.0Models", "OPENFGA_ALLOW_WRITING_1_0_MODELS")

	flags.Bool("allow-evaluating-1.0-models", defaultConfig.AllowEvaluating1_0Models, "allow evaluating of models with 1.0 schema")
	util.MustBindPFlag("allowEvaluating1.0Models", flags.Lookup("allow-evaluating-1.0-models"))
	util.MustBindEnv("allowEvaluating1.0Models", "OPENFGA_ALLOW_EVALUATING_1_0_MODELS")

}
