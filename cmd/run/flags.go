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

		util.MustBindPFlag("accessControl.enabled", flags.Lookup("access-control-enabled"))
		util.MustBindEnv("accessControl.enabled", "OPENFGA_ACCESS_CONTROL_ENABLED")

		util.MustBindPFlag("accessControl.storeId", flags.Lookup("access-control-store-id"))
		util.MustBindEnv("accessControl.storeId", "OPENFGA_ACCESS_CONTROL_STORE_ID")

		util.MustBindPFlag("accessControl.modelId", flags.Lookup("access-control-model-id"))
		util.MustBindEnv("accessControl.modelId", "OPENFGA_ACCESS_CONTROL_MODEL_ID")

		command.MarkFlagsRequiredTogether("access-control-enabled", "access-control-store-id", "access-control-model-id")

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

		util.MustBindPFlag("authn.oidc.subjects", flags.Lookup("authn-oidc-subjects"))
		util.MustBindEnv("authn.oidc.subjects", "OPENFGA_AUTHN_OIDC_SUBJECTS")

		util.MustBindPFlag("authn.oidc.clientIdClaims", flags.Lookup("authn-oidc-client-id-claims"))
		util.MustBindEnv("authn.oidc.clientIdClaims", "OPENFGA_AUTHN_OIDC_CLIENT_ID_CLAIMS")

		util.MustBindPFlag("datastore.engine", flags.Lookup("datastore-engine"))
		util.MustBindEnv("datastore.engine", "OPENFGA_DATASTORE_ENGINE")

		util.MustBindPFlag("datastore.uri", flags.Lookup("datastore-uri"))
		util.MustBindEnv("datastore.uri", "OPENFGA_DATASTORE_URI")

		util.MustBindPFlag("datastore.secondaryUri", flags.Lookup("datastore-secondary-uri"))
		util.MustBindEnv("datastore.secondaryUri", "OPENFGA_DATASTORE_SECONDARY_URI")

		util.MustBindPFlag("datastore.secondaryUsername", flags.Lookup("datastore-secondary-username"))
		util.MustBindEnv("datastore.secondaryUsername", "OPENFGA_DATASTORE_SECONDARY_USERNAME")

		util.MustBindPFlag("datastore.secondaryPassword", flags.Lookup("datastore-secondary-password"))
		util.MustBindEnv("datastore.secondaryPassword", "OPENFGA_DATASTORE_SECONDARY_PASSWORD")

		util.MustBindPFlag("datastore.username", flags.Lookup("datastore-username"))
		util.MustBindEnv("datastore.username", "OPENFGA_DATASTORE_USERNAME")

		util.MustBindPFlag("datastore.password", flags.Lookup("datastore-password"))
		util.MustBindEnv("datastore.password", "OPENFGA_DATASTORE_PASSWORD")

		util.MustBindPFlag("datastore.maxCacheSize", flags.Lookup("datastore-max-cache-size"))
		util.MustBindEnv("datastore.maxCacheSize", "OPENFGA_DATASTORE_MAX_CACHE_SIZE", "OPENFGA_DATASTORE_MAXCACHESIZE")

		util.MustBindPFlag("datastore.minOpenConns", flags.Lookup("datastore-min-open-conns"))
		util.MustBindEnv("datastore.minOpenConns", "OPENFGA_DATASTORE_MIN_OPEN_CONNS")

		util.MustBindPFlag("datastore.maxOpenConns", flags.Lookup("datastore-max-open-conns"))
		util.MustBindEnv("datastore.maxOpenConns", "OPENFGA_DATASTORE_MAX_OPEN_CONNS", "OPENFGA_DATASTORE_MAXOPENCONNS")

		util.MustBindPFlag("datastore.minIdleConns", flags.Lookup("datastore-min-idle-conns"))
		util.MustBindEnv("datastore.minIdleConns", "OPENFGA_DATASTORE_MIN_IDLE_CONNS")

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

		util.MustBindPFlag("maxChecksPerBatchCheck", flags.Lookup("max-checks-per-batch-check"))
		util.MustBindEnv("maxChecksPerBatchCheck", "OPENFGA_MAX_CHECKS_PER_BATCH_CHECK")

		util.MustBindPFlag("maxConcurrentChecksPerBatchCheck", flags.Lookup("max-concurrent-checks-per-batch-check"))
		util.MustBindEnv("maxConcurrentChecksPerBatchCheck", "OPENFGA_MAX_CONCURRENT_CHECKS_PER_BATCH_CHECK")

		util.MustBindPFlag("maxTuplesPerWrite", flags.Lookup("max-tuples-per-write"))
		util.MustBindEnv("maxTuplesPerWrite", "OPENFGA_MAX_TUPLES_PER_WRITE", "OPENFGA_MAXTUPLESPERWRITE")

		util.MustBindPFlag("maxTypesPerAuthorizationModel", flags.Lookup("max-types-per-authorization-model"))
		util.MustBindEnv("maxTypesPerAuthorizationModel", "OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "OPENFGA_MAXTYPESPERAUTHORIZATIONMODEL")

		util.MustBindPFlag("maxAuthorizationModelSizeInBytes", flags.Lookup("max-authorization-model-size-in-bytes"))
		util.MustBindEnv("maxAuthorizationModelSizeInBytes", "OPENFGA_MAX_AUTHORIZATION_MODEL_SIZE_IN_BYTES", "OPENFGA_MAXAUTHORIZATIONMODELSIZEINBYTES")

		util.MustBindPFlag("maxConcurrentReadsForListObjects", flags.Lookup("max-concurrent-reads-for-list-objects"))
		util.MustBindEnv("maxConcurrentReadsForListObjects", "OPENFGA_MAX_CONCURRENT_READS_FOR_LIST_OBJECTS", "OPENFGA_MAXCONCURRENTREADSFORLISTOBJECTS")

		util.MustBindPFlag("maxConcurrentReadsForListUsers", flags.Lookup("max-concurrent-reads-for-list-users"))
		util.MustBindEnv("maxConcurrentReadsForListUsers", "OPENFGA_MAX_CONCURRENT_READS_FOR_LIST_USERS", "OPENFGA_MAXCONCURRENTREADSFORLISTUSERS")

		util.MustBindPFlag("maxConcurrentReadsForCheck", flags.Lookup("max-concurrent-reads-for-check"))
		util.MustBindEnv("maxConcurrentReadsForCheck", "OPENFGA_MAX_CONCURRENT_READS_FOR_CHECK", "OPENFGA_MAXCONCURRENTREADSFORCHECK")

		util.MustBindPFlag("maxConditionEvaluationCost", flags.Lookup("max-condition-evaluation-cost"))
		util.MustBindEnv("maxConditionEvaluationCost", "OPENFGA_MAX_CONDITION_EVALUATION_COST", "OPENFGA_MAXCONDITIONEVALUATIONCOST")

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

		util.MustBindPFlag("listUsersDeadline", flags.Lookup("listUsers-deadline"))
		util.MustBindEnv("listUsersDeadline", "OPENFGA_LIST_USERS_DEADLINE", "OPENFGA_LISTUSERSDEADLINE")

		util.MustBindPFlag("listUsersMaxResults", flags.Lookup("listUsers-max-results"))
		util.MustBindEnv("listUsersMaxResults", "OPENFGA_LIST_USERS_MAX_RESULTS", "OPENFGA_LISTUSERSMAXRESULTS")

		util.MustBindPFlag("checkCache.limit", flags.Lookup("check-cache-limit"))
		util.MustBindEnv("checkCache.limit", "OPENFGA_CHECK_CACHE_LIMIT")

		// The below configuration is deprecated in favour of OPENFGA_CHECK_CACHE_LIMIT
		util.MustBindPFlag("cache.limit", flags.Lookup("check-query-cache-limit"))
		util.MustBindEnv("cache.limit", "OPENFGA_CHECK_QUERY_CACHE_LIMIT")

		util.MustBindPFlag("cacheController.enabled", flags.Lookup("cache-controller-enabled"))
		util.MustBindEnv("cacheController.enabled", "OPENFGA_CACHE_CONTROLLER_ENABLED")

		util.MustBindPFlag("cacheController.ttl", flags.Lookup("cache-controller-ttl"))
		util.MustBindEnv("cacheController.ttl", "OPENFGA_CACHE_CONTROLLER_TTL")

		util.MustBindPFlag("checkIteratorCache.enabled", flags.Lookup("check-iterator-cache-enabled"))
		util.MustBindEnv("checkIteratorCache.enabled", "OPENFGA_CHECK_ITERATOR_CACHE_ENABLED")

		util.MustBindPFlag("checkIteratorCache.maxResults", flags.Lookup("check-iterator-cache-max-results"))
		util.MustBindEnv("checkIteratorCache.maxResults", "OPENFGA_CHECK_ITERATOR_CACHE_MAX_RESULTS")

		util.MustBindPFlag("checkIteratorCache.ttl", flags.Lookup("check-iterator-cache-ttl"))
		util.MustBindEnv("checkIteratorCache.ttl", "OPENFGA_CHECK_ITERATOR_CACHE_TTL")

		util.MustBindPFlag("checkQueryCache.enabled", flags.Lookup("check-query-cache-enabled"))
		util.MustBindEnv("checkQueryCache.enabled", "OPENFGA_CHECK_QUERY_CACHE_ENABLED")

		util.MustBindPFlag("checkQueryCache.ttl", flags.Lookup("check-query-cache-ttl"))
		util.MustBindEnv("checkQueryCache.ttl", "OPENFGA_CHECK_QUERY_CACHE_TTL")

		util.MustBindPFlag("listObjectsIteratorCache.enabled", flags.Lookup("list-objects-iterator-cache-enabled"))
		util.MustBindEnv("listObjectsIteratorCache.enabled", "OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_ENABLED")

		util.MustBindPFlag("listObjectsIteratorCache.maxResults", flags.Lookup("list-objects-iterator-cache-max-results"))
		util.MustBindEnv("listObjectsIteratorCache.maxResults", "OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_MAX_RESULTS")

		util.MustBindPFlag("listObjectsIteratorCache.ttl", flags.Lookup("list-objects-iterator-cache-ttl"))
		util.MustBindEnv("listObjectsIteratorCache.ttl", "OPENFGA_LIST_OBJECTS_ITERATOR_CACHE_TTL")

		util.MustBindPFlag("sharedIterator.enabled", flags.Lookup("shared-iterator-enabled"))
		util.MustBindEnv("sharedIterator.enabled", "OPENFGA_SHARED_ITERATOR_ENABLED")

		util.MustBindPFlag("sharedIterator.limit", flags.Lookup("shared-iterator-limit"))
		util.MustBindEnv("sharedIterator.limit", "OPENFGA_SHARED_ITERATOR_LIMIT")

		util.MustBindPFlag("requestDurationDatastoreQueryCountBuckets", flags.Lookup("request-duration-datastore-query-count-buckets"))
		util.MustBindEnv("requestDurationDatastoreQueryCountBuckets", "OPENFGA_REQUEST_DURATION_DATASTORE_QUERY_COUNT_BUCKETS")

		util.MustBindPFlag("requestDurationDispatchCountBuckets", flags.Lookup("request-duration-dispatch-count-buckets"))
		util.MustBindEnv("requestDurationDispatchCountBuckets", "OPENFGA_REQUEST_DURATION_DISPATCH_COUNT_BUCKETS")

		util.MustBindPFlag("contextPropagationToDatastore", flags.Lookup("context-propagation-to-datastore"))
		util.MustBindEnv("contextPropagationToDatastore", "OPENFGA_CONTEXT_PROPAGATION_TO_DATASTORE")

		util.MustBindPFlag("checkDatastoreThrottle.threshold", flags.Lookup("check-datastore-throttle-threshold"))
		util.MustBindEnv("checkDatastoreThrottle.threshold", "OPENFGA_CHECK_DATASTORE_THROTTLE_THRESHOLD")

		util.MustBindPFlag("checkDatastoreThrottle.duration", flags.Lookup("check-datastore-throttle-duration"))
		util.MustBindEnv("checkDatastoreThrottle.duration", "OPENFGA_CHECK_DATASTORE_THROTTLE_DURATION")

		util.MustBindPFlag("listObjectsDatastoreThrottle.threshold", flags.Lookup("listObjects-datastore-throttle-threshold"))
		util.MustBindEnv("listObjectsDatastoreThrottle.threshold", "OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_THRESHOLD")

		util.MustBindPFlag("listObjectsDatastoreThrottle.duration", flags.Lookup("listObjects-datastore-throttle-duration"))
		util.MustBindEnv("listObjectsDatastoreThrottle.duration", "OPENFGA_LIST_OBJECTS_DATASTORE_THROTTLE_DURATION")

		util.MustBindPFlag("listUsersDatastoreThrottle.threshold", flags.Lookup("listUsers-datastore-throttle-threshold"))
		util.MustBindEnv("listUsersDatastoreThrottle.threshold", "OPENFGA_LIST_USERS_DATASTORE_THROTTLE_THRESHOLD")

		util.MustBindPFlag("listUsersDatastoreThrottle.duration", flags.Lookup("listUsers-datastore-throttle-duration"))
		util.MustBindEnv("listUsersDatastoreThrottle.duration", "OPENFGA_LIST_USERS_DATASTORE_THROTTLE_DURATION")

		util.MustBindPFlag("requestTimeout", flags.Lookup("request-timeout"))
		util.MustBindEnv("requestTimeout", "OPENFGA_REQUEST_TIMEOUT")

		// these are irrelevant unless the check-experimental flag is enabled at the current time
		util.MustBindPFlag("planner.evictionThreshold", flags.Lookup("planner-eviction-threshold"))
		util.MustBindEnv("planner.evictionThreshold", "OPENFGA_PLANNER_EVICTION_THRESHOLD")
		util.MustBindPFlag("planner.cleanupInterval", flags.Lookup("planner-cleanup-interval"))
		util.MustBindEnv("planner.cleanupInterval", "OPENFGA_PLANNER_CLEANUP_INTERVAL")
	}
}
