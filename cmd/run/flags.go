package run

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/openfga/openfga/cmd/util"
)

// bindRunFlags binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindRunFlagsFunc(flags *pflag.FlagSet) func(*cobra.Command, []string) {
	return func(command *cobra.Command, args []string) {
		util.MustBindPFlag("experimentals", flags.Lookup("experimentals"))
		util.MustBindEnv("experimentals", "OPENFGA_EXPERIMENTALS")

		util.MustBindPFlag("grpc.addr", flags.Lookup("grpc-addr"))
		util.MustBindEnv("grpc.addr", "OPENFGA_GRPC_ADDR")

		util.MustBindPFlag("grpc.tls.enabled", flags.Lookup("grpc-tls-enabled"))
		util.MustBindEnv("grpc.tls.enabled", "OPENFGA_GRPC_TLS_ENABLED")

		util.MustBindPFlag("grpc.tls.cert", flags.Lookup("grpc-tls-cert"))
		util.MustBindEnv("grpc.tls.cert", "OPENFGA_GRPC_TLS_CERT")

		util.MustBindPFlag("grpc.tls.key", flags.Lookup("grpc-tls-key"))
		util.MustBindEnv("grpc.tls.key", "OPENFGA_GRPC_TLS_KEY")

		command.MarkFlagsRequiredTogether("grpc-tls-enabled", "grpc-tls-cert", "grpc-tls-key")

		util.MustBindPFlag("http.enabled", flags.Lookup("http-enabled"))
		util.MustBindEnv("http.enabled", "OPENFGA_HTTP_ENABLED")

		util.MustBindPFlag("http.addr", flags.Lookup("http-addr"))
		util.MustBindEnv("http.addr", "OPENFGA_HTTP_ADDR")

		util.MustBindPFlag("http.tls.enabled", flags.Lookup("http-tls-enabled"))
		util.MustBindEnv("http.tls.enabled", "OPENFGA_HTTP_TLS_ENABLED")

		util.MustBindPFlag("http.tls.cert", flags.Lookup("http-tls-cert"))
		util.MustBindEnv("http.tls.cert", "OPENFGA_HTTP_TLS_CERT")

		util.MustBindPFlag("http.tls.key", flags.Lookup("http-tls-key"))
		util.MustBindEnv("http.tls.key", "OPENFGA_HTTP_TLS_KEY")

		command.MarkFlagsRequiredTogether("http-tls-enabled", "http-tls-cert", "http-tls-key")

		util.MustBindPFlag("http.upstreamTimeout", flags.Lookup("http-upstream-timeout"))
		util.MustBindEnv("http.upstreamTimeout", "OPENFGA_HTTP_UPSTREAM_TIMEOUT", "OPENFGA_HTTP_UPSTREAMTIMEOUT")

		util.MustBindPFlag("http.corsAllowedOrigins", flags.Lookup("http-cors-allowed-origins"))
		util.MustBindEnv("http.corsAllowedOrigins", "OPENFGA_HTTP_CORS_ALLOWED_ORIGINS", "OPENFGA_HTTP_CORSALLOWEDORIGINS")

		util.MustBindPFlag("http.corsAllowedHeaders", flags.Lookup("http-cors-allowed-headers"))
		util.MustBindEnv("http.corsAllowedHeaders", "OPENFGA_HTTP_CORS_ALLOWED_HEADERS", "OPENFGA_HTTP_CORSALLOWEDHEADERS")

		util.MustBindPFlag("authn.method", flags.Lookup("authn-method"))
		util.MustBindEnv("authn.method", "OPENFGA_AUTHN_METHOD")

		util.MustBindPFlag("authn.preshared.keys", flags.Lookup("authn-preshared-keys"))
		util.MustBindEnv("authn.preshared.keys", "OPENFGA_AUTHN_PRESHARED_KEYS")

		util.MustBindPFlag("authn.oidc.audience", flags.Lookup("authn-oidc-audience"))
		util.MustBindEnv("authn.oidc.audience", "OPENFGA_AUTHN_OIDC_AUDIENCE")

		util.MustBindPFlag("authn.oidc.issuer", flags.Lookup("authn-oidc-issuer"))
		util.MustBindEnv("authn.oidc.issuer", "OPENFGA_AUTHN_OIDC_ISSUER")

		util.MustBindPFlag("authn.oidc.issuerAliases", flags.Lookup("authn-oidc-issuer-aliases"))
		util.MustBindEnv("authn.oidc.issuerAliases", "OPENFGA_AUTHN_OIDC_ISSUER_ALIASES")

		util.MustBindPFlag("datastore.engine", flags.Lookup("datastore-engine"))
		util.MustBindEnv("datastore.engine", "OPENFGA_DATASTORE_ENGINE")

		util.MustBindPFlag("datastore.uri", flags.Lookup("datastore-uri"))
		util.MustBindEnv("datastore.uri", "OPENFGA_DATASTORE_URI")

		util.MustBindPFlag("datastore.username", flags.Lookup("datastore-username"))
		util.MustBindEnv("datastore.username", "OPENFGA_DATASTORE_USERNAME")

		util.MustBindPFlag("datastore.password", flags.Lookup("datastore-password"))
		util.MustBindEnv("datastore.password", "OPENFGA_DATASTORE_PASSWORD")

		util.MustBindPFlag("datastore.maxCacheSize", flags.Lookup("datastore-max-cache-size"))
		util.MustBindEnv("datastore.maxCacheSize", "OPENFGA_DATASTORE_MAX_CACHE_SIZE", "OPENFGA_DATASTORE_MAXCACHESIZE")

		util.MustBindPFlag("datastore.maxOpenConns", flags.Lookup("datastore-max-open-conns"))
		util.MustBindEnv("datastore.maxOpenConns", "OPENFGA_DATASTORE_MAX_OPEN_CONNS", "OPENFGA_DATASTORE_MAXOPENCONNS")

		util.MustBindPFlag("datastore.maxIdleConns", flags.Lookup("datastore-max-idle-conns"))
		util.MustBindEnv("datastore.maxIdleConns", "OPENFGA_DATASTORE_MAX_IDLE_CONNS", "OPENFGA_DATASTORE_MAXIDLECONNS")

		util.MustBindPFlag("datastore.connMaxIdleTime", flags.Lookup("datastore-conn-max-idle-time"))
		util.MustBindEnv("datastore.connMaxIdleTime", "OPENFGA_DATASTORE_CONN_MAX_IDLE_TIME", "OPENFGA_DATASTORE_CONNMAXIDLETIME")

		util.MustBindPFlag("datastore.connMaxLifetime", flags.Lookup("datastore-conn-max-lifetime"))
		util.MustBindEnv("datastore.connMaxLifetime", "OPENFGA_DATASTORE_CONN_MAX_LIFETIME", "OPENFGA_DATASTORE_CONNMAXLIFETIME")

		util.MustBindPFlag("datastore.metrics.enabled", flags.Lookup("datastore-metrics-enabled"))
		util.MustBindEnv("datastore.metrics.enabled", "OPENFGA_DATASTORE_METRICS_ENABLED")

		util.MustBindPFlag("playground.enabled", flags.Lookup("playground-enabled"))
		util.MustBindEnv("playground.enabled", "OPENFGA_PLAYGROUND_ENABLED")

		util.MustBindPFlag("playground.port", flags.Lookup("playground-port"))
		util.MustBindEnv("playground.port", "OPENFGA_PLAYGROUND_PORT")

		util.MustBindPFlag("profiler.enabled", flags.Lookup("profiler-enabled"))
		util.MustBindEnv("profiler.enabled", "OPENFGA_PROFILER_ENABLED")

		util.MustBindPFlag("profiler.addr", flags.Lookup("profiler-addr"))
		util.MustBindEnv("profiler.addr", "OPENFGA_PROFILER_ADDRESS")

		util.MustBindPFlag("log.format", flags.Lookup("log-format"))
		util.MustBindEnv("log.format", "OPENFGA_LOG_FORMAT")

		util.MustBindPFlag("log.level", flags.Lookup("log-level"))
		util.MustBindEnv("log.level", "OPENFGA_LOG_LEVEL")

		util.MustBindPFlag("log.timestampFormat", flags.Lookup("log-timestamp-format"))
		util.MustBindEnv("log.timestampFormat", "OPENFGA_LOG_TIMESTAMP_FORMAT")

		util.MustBindPFlag("trace.enabled", flags.Lookup("trace-enabled"))
		util.MustBindEnv("trace.enabled", "OPENFGA_TRACE_ENABLED")

		util.MustBindPFlag("trace.otlp.endpoint", flags.Lookup("trace-otlp-endpoint"))
		util.MustBindEnv("trace.otlp.endpoint", "OPENFGA_TRACE_OTLP_ENDPOINT")

		util.MustBindPFlag("trace.otlp.tls.enabled", flags.Lookup("trace-otlp-tls-enabled"))
		util.MustBindEnv("trace.otlp.tls.enabled", "OPENFGA_TRACE_OTLP_TLS_ENABLED")

		util.MustBindPFlag("trace.sampleRatio", flags.Lookup("trace-sample-ratio"))
		util.MustBindEnv("trace.sampleRatio", "OPENFGA_TRACE_SAMPLE_RATIO")

		util.MustBindPFlag("trace.serviceName", flags.Lookup("trace-service-name"))
		util.MustBindEnv("trace.serviceName", "OPENFGA_TRACE_SERVICE_NAME")

		util.MustBindPFlag("metrics.enabled", flags.Lookup("metrics-enabled"))
		util.MustBindEnv("metrics.enabled", "OPENFGA_METRICS_ENABLED")

		util.MustBindPFlag("metrics.addr", flags.Lookup("metrics-addr"))
		util.MustBindEnv("metrics.addr", "OPENFGA_METRICS_ADDR")

		util.MustBindPFlag("metrics.enableRPCHistograms", flags.Lookup("metrics-enable-rpc-histograms"))
		util.MustBindEnv("metrics.enableRPCHistograms", "OPENFGA_METRICS_ENABLE_RPC_HISTOGRAMS")

		util.MustBindPFlag("maxTuplesPerWrite", flags.Lookup("max-tuples-per-write"))
		util.MustBindEnv("maxTuplesPerWrite", "OPENFGA_MAX_TUPLES_PER_WRITE", "OPENFGA_MAXTUPLESPERWRITE")

		util.MustBindPFlag("maxTypesPerAuthorizationModel", flags.Lookup("max-types-per-authorization-model"))
		util.MustBindEnv("maxTypesPerAuthorizationModel", "OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "OPENFGA_MAXTYPESPERAUTHORIZATIONMODEL")

		util.MustBindPFlag("maxAuthorizationModelSizeInBytes", flags.Lookup("max-authorization-model-size-in-bytes"))
		util.MustBindEnv("maxAuthorizationModelSizeInBytes", "OPENFGA_MAX_AUTHORIZATION_MODEL_SIZE_IN_BYTES", "OPENFGA_MAXAUTHORIZATIONMODELSIZEINBYTES")

		util.MustBindPFlag("maxConcurrentReadsForListObjects", flags.Lookup("max-concurrent-reads-for-list-objects"))
		util.MustBindEnv("maxConcurrentReadsForListObjects", "OPENFGA_MAX_CONCURRENT_READS_FOR_LIST_OBJECTS", "OPENFGA_MAXCONCURRENTREADSFORLISTOBJECTS")

		util.MustBindPFlag("maxConcurrentReadsForCheck", flags.Lookup("max-concurrent-reads-for-check"))
		util.MustBindEnv("maxConcurrentReadsForCheck", "OPENFGA_MAX_CONCURRENT_READS_FOR_CHECK", "OPENFGA_MAXCONCURRENTREADSFORCHECK")

		util.MustBindPFlag("changelogHorizonOffset", flags.Lookup("changelog-horizon-offset"))
		util.MustBindEnv("changelogHorizonOffset", "OPENFGA_CHANGELOG_HORIZON_OFFSET", "OPENFGA_CHANGELOGHORIZONOFFSET")

		util.MustBindPFlag("resolveNodeLimit", flags.Lookup("resolve-node-limit"))
		util.MustBindEnv("resolveNodeLimit", "OPENFGA_RESOLVE_NODE_LIMIT", "OPENFGA_RESOLVENODELIMIT")

		util.MustBindPFlag("resolveNodeBreadthLimit", flags.Lookup("resolve-node-breadth-limit"))
		util.MustBindEnv("resolveNodeBreadthLimit", "OPENFGA_RESOLVE_NODE_BREADTH_LIMIT", "OPENFGA_RESOLVENODEBREADTHLIMIT")

		util.MustBindPFlag("listObjectsDeadline", flags.Lookup("listObjects-deadline"))
		util.MustBindEnv("listObjectsDeadline", "OPENFGA_LIST_OBJECTS_DEADLINE", "OPENFGA_LISTOBJECTSDEADLINE")

		util.MustBindPFlag("listObjectsMaxResults", flags.Lookup("listObjects-max-results"))
		util.MustBindEnv("listObjectsMaxResults", "OPENFGA_LIST_OBJECTS_MAX_RESULTS", "OPENFGA_LISTOBJECTSMAXRESULTS")

		util.MustBindPFlag("checkQueryCache.enabled", flags.Lookup("check-query-cache-enabled"))
		util.MustBindEnv("checkQueryCache.enabled", "OPENFGA_CHECK_QUERY_CACHE_ENABLED")

		util.MustBindPFlag("checkQueryCache.limit", flags.Lookup("check-query-cache-limit"))
		util.MustBindEnv("checkQueryCache.limit", "OPENFGA_CHECK_QUERY_CACHE_LIMIT")

		util.MustBindPFlag("checkQueryCache.ttl", flags.Lookup("check-query-cache-ttl"))
		util.MustBindEnv("checkQueryCache.ttl", "OPENFGA_CHECK_QUERY_CACHE_TTL")

		util.MustBindPFlag("requestDurationDatastoreQueryCountBuckets", flags.Lookup("request-duration-datastore-query-count-buckets"))
		util.MustBindEnv("requestDurationDatastoreQueryCountBuckets", "OPENFGA_REQUEST_DURATION_DATASTORE_QUERY_COUNT_BUCKETS")

		util.MustBindPFlag("requestDurationDispatchCountBuckets", flags.Lookup("request-duration-dispatch-count-buckets"))
		util.MustBindEnv("requestDurationDispatchCountBuckets", "OPENFGA_REQUEST_DURATION_DISPATCH_COUNT_BUCKETS")

		util.MustBindPFlag("dispatchThrottling.enabled", flags.Lookup("dispatch-throttling-enabled"))
		util.MustBindEnv("dispatchThrottling.enabled", "OPENFGA_DISPATCH_THROTTLING_ENABLED")

		util.MustBindPFlag("dispatchThrottling.frequency", flags.Lookup("dispatch-throttling-frequency"))
		util.MustBindEnv("dispatchThrottling.frequency", "OPENFGA_DISPATCH_THROTTLING_FREQUENCY")

		util.MustBindPFlag("dispatchThrottling.threshold", flags.Lookup("dispatch-throttling-threshold"))
		util.MustBindEnv("dispatchThrottling.threshold", "OPENFGA_DISPATCH_THROTTLING_THRESHOLD")

		util.MustBindPFlag("dispatchThrottling.maxThreshold", flags.Lookup("dispatch-throttling-max-threshold"))
		util.MustBindEnv("dispatchThrottling.maxThreshold", "OPENFGA_DISPATCH_THROTTLING_MAX_THRESHOLD")

		util.MustBindPFlag("requestTimeout", flags.Lookup("request-timeout"))
		util.MustBindEnv("requestTimeout", "OPENFGA_REQUEST_TIMEOUT")
	}
}
