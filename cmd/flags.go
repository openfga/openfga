package cmd

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// mustBindPFlag attempts to bind a specific key to a pflag (as used by cobra) and panics
// if the binding fails with a non-nil error.
func mustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic("failed to bind pflag: " + err.Error())
	}
}

func mustBindEnv(input ...string) {
	if err := viper.BindEnv(input...); err != nil {
		panic("failed to bind env key: " + err.Error())
	}
}

// BindRunFlags binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindRunFlags(command *cobra.Command) {
	defaultConfig := DefaultConfig()
	flags := command.Flags()

	flags.StringSlice("experimentals", defaultConfig.Experimentals, "a list of experimental features to enable")
	mustBindPFlag("experimentals", flags.Lookup("experimentals"))
	mustBindEnv("experimentals", "OPENFGA_EXPERIMENTALS")

	flags.String("grpc-addr", defaultConfig.GRPC.Addr, "the host:port address to serve the grpc server on")
	mustBindPFlag("grpc.addr", flags.Lookup("grpc-addr"))
	mustBindEnv("grpc.addr", "OPENFGA_GRPC_ADDR")

	flags.Bool("grpc-tls-enabled", defaultConfig.GRPC.TLS.Enabled, "enable/disable transport layer security (TLS)")
	mustBindPFlag("grpc.tls.enabled", flags.Lookup("grpc-tls-enabled"))
	mustBindEnv("grpc.tls.enabled", "OPENFGA_GRPC_TLS_ENABLED")

	flags.String("grpc-tls-cert", defaultConfig.GRPC.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")
	mustBindPFlag("grpc.tls.cert", flags.Lookup("grpc-tls-cert"))
	mustBindEnv("grpc.tls.cert", "OPENFGA_GRPC_TLS_CERT")

	flags.String("grpc-tls-key", defaultConfig.GRPC.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")
	mustBindPFlag("grpc.tls.key", flags.Lookup("grpc-tls-key"))
	mustBindEnv("grpc.tls.key", "OPENFGA_GRPC_TLS_KEY")

	command.MarkFlagsRequiredTogether("grpc-tls-enabled", "grpc-tls-cert", "grpc-tls-key")

	flags.Bool("http-enabled", defaultConfig.HTTP.Enabled, "enable/disable the OpenFGA HTTP server")
	mustBindPFlag("http.enabled", flags.Lookup("http-enabled"))
	mustBindEnv("http.enabled", "OPENFGA_HTTP_ENABLED")

	flags.String("http-addr", defaultConfig.HTTP.Addr, "the host:port address to serve the HTTP server on")
	mustBindPFlag("http.addr", flags.Lookup("http-addr"))
	mustBindEnv("http.addr", "OPENFGA_HTTP_ADDR")

	flags.Bool("http-tls-enabled", defaultConfig.HTTP.TLS.Enabled, "enable/disable transport layer security (TLS)")
	mustBindPFlag("http.tls.enabled", flags.Lookup("http-tls-enabled"))
	mustBindEnv("http.tls.enabled", "OPENFGA_HTTP_TLS_ENABLED")

	flags.String("http-tls-cert", defaultConfig.HTTP.TLS.CertPath, "the (absolute) file path of the certificate to use for the TLS connection")
	mustBindPFlag("http.tls.cert", flags.Lookup("http-tls-cert"))
	mustBindEnv("http.tls.cert", "OPENFGA_HTTP_TLS_CERT")

	flags.String("http-tls-key", defaultConfig.HTTP.TLS.KeyPath, "the (absolute) file path of the TLS key that should be used for the TLS connection")
	mustBindPFlag("http.tls.key", flags.Lookup("http-tls-key"))
	mustBindEnv("http.tls.key", "OPENFGA_HTTP_TLS_KEY")

	command.MarkFlagsRequiredTogether("http-tls-enabled", "http-tls-cert", "http-tls-key")

	flags.Duration("http-upstream-timeout", defaultConfig.HTTP.UpstreamTimeout, "the timeout duration for proxying HTTP requests upstream to the grpc endpoint")
	mustBindPFlag("http.upstreamTimeout", flags.Lookup("http-upstream-timeout"))
	mustBindEnv("http.upstreamTimeout", "OPENFGA_HTTP_UPSTREAM_TIMEOUT", "OPENFGA_HTTP_UPSTREAMTIMEOUT")

	flags.StringSlice("http-cors-allowed-origins", defaultConfig.HTTP.CORSAllowedOrigins, "specifies the CORS allowed origins")
	mustBindPFlag("http.corsAllowedOrigins", flags.Lookup("http-cors-allowed-origins"))
	mustBindEnv("http.corsAllowedOrigins", "OPENFGA_HTTP_CORS_ALLOWED_ORIGINS", "OPENFGA_HTTP_CORSALLOWEDORIGINS")

	flags.StringSlice("http-cors-allowed-headers", defaultConfig.HTTP.CORSAllowedHeaders, "specifies the CORS allowed headers")
	mustBindPFlag("http.corsAllowedHeaders", flags.Lookup("http-cors-allowed-headers"))
	mustBindEnv("http.corsAllowedHeaders", "OPENFGA_HTTP_CORS_ALLOWED_HEADERS", "OPENFGA_HTTP_CORSALLOWEDHEADERS")

	flags.String("authn-method", defaultConfig.Authn.Method, "the authentication method to use")
	mustBindPFlag("authn.method", flags.Lookup("authn-method"))
	mustBindEnv("authn.method", "OPENFGA_AUTHN_METHOD")

	flags.StringSlice("authn-preshared-keys", defaultConfig.Authn.Keys, "one or more preshared keys to use for authentication")
	mustBindPFlag("authn.preshared.keys", flags.Lookup("authn-preshared-keys"))
	mustBindEnv("authn.preshared.keys", "OPENFGA_AUTHN_PRESHARED_KEYS")

	flags.String("authn-oidc-audience", defaultConfig.Authn.Audience, "the OIDC audience of the tokens being signed by the authorization server")
	mustBindPFlag("authn.oidc.audience", flags.Lookup("authn-oidc-audience"))
	mustBindEnv("authn.oidc.audience", "OPENFGA_AUTHN_OIDC_AUDIENCE")

	flags.String("authn-oidc-issuer", defaultConfig.Authn.Issuer, "the OIDC issuer (authorization server) signing the tokens")
	mustBindPFlag("authn.oidc.issuer", flags.Lookup("authn-oidc-issuer"))
	mustBindEnv("authn.oidc.issuer", "OPENFGA_AUTHN_OIDC_ISSUER")

	flags.String("datastore-engine", defaultConfig.Datastore.Engine, "the datastore engine that will be used for persistence")
	mustBindPFlag("datastore.engine", flags.Lookup("datastore-engine"))
	mustBindEnv("datastore.engine", "OPENFGA_DATASTORE_ENGINE")

	flags.String("datastore-uri", defaultConfig.Datastore.URI, "the connection uri to use to connect to the datastore (for any engine other than 'memory')")
	mustBindPFlag("datastore.uri", flags.Lookup("datastore-uri"))
	mustBindEnv("datastore.uri", "OPENFGA_DATASTORE_URI")

	flags.Int("datastore-max-cache-size", defaultConfig.Datastore.MaxCacheSize, "the maximum number of cache keys that the storage cache can store before evicting old keys")
	mustBindPFlag("datastore.maxCacheSize", flags.Lookup("datastore-max-cache-size"))
	mustBindEnv("datastore.maxCacheSize", "OPENFGA_DATASTORE_MAX_CACHE_SIZE", "OPENFGA_DATASTORE_MAXCACHESIZE")

	flags.Int("datastore-max-open-conns", defaultConfig.Datastore.MaxOpenConns, "the maximum number of open connections to the datastore")
	mustBindPFlag("datastore.maxOpenConns", flags.Lookup("datastore-max-open-conns"))
	mustBindEnv("datastore.maxOpenConns", "OPENFGA_DATASTORE_MAX_OPEN_CONNS", "OPENFGA_DATASTORE_MAXOPENCONNS")

	flags.Int("datastore-max-idle-conns", defaultConfig.Datastore.MaxIdleConns, "the maximum number of connections to the datastore in the idle connection pool")
	mustBindPFlag("datastore.maxIdleConns", flags.Lookup("datastore-max-idle-conns"))
	mustBindEnv("datastore.maxIdleConns", "OPENFGA_DATASTORE_MAX_IDLE_CONNS", "OPENFGA_DATASTORE_MAXIDLECONNS")

	flags.Duration("datastore-conn-max-idle-time", defaultConfig.Datastore.ConnMaxIdleTime, "the maximum amount of time a connection to the datastore may be idle")
	mustBindPFlag("datastore.connMaxIdleTime", flags.Lookup("datastore-conn-max-idle-time"))
	mustBindEnv("datastore.connMaxIdleTime", "OPENFGA_DATASTORE_CONN_MAX_IDLE_TIME", "OPENFGA_DATASTORE_CONNMAXIDLETIME")

	flags.Duration("datastore-conn-max-lifetime", defaultConfig.Datastore.ConnMaxLifetime, "the maximum amount of time a connection to the datastore may be reused")
	mustBindPFlag("datastore.connMaxLifetime", flags.Lookup("datastore-conn-max-lifetime"))
	mustBindEnv("datastore.connMaxLifetime", "OPENFGA_DATASTORE_CONN_MAX_LIFETIME", "OPENFGA_DATASTORE_CONNMAXLIFETIME")

	flags.Bool("playground-enabled", defaultConfig.Playground.Enabled, "enable/disable the OpenFGA Playground")
	mustBindPFlag("playground.enabled", flags.Lookup("playground-enabled"))
	mustBindEnv("playground.enabled", "OPENFGA_PLAYGROUND_ENABLED")

	flags.Int("playground-port", defaultConfig.Playground.Port, "the port to serve the local OpenFGA Playground on")
	mustBindPFlag("playground.port", flags.Lookup("playground-port"))
	mustBindEnv("playground.port", "OPENFGA_PLAYGROUND_PORT")

	flags.Bool("profiler-enabled", defaultConfig.Profiler.Enabled, "enable/disable pprof profiling")
	mustBindPFlag("profiler.enabled", flags.Lookup("profiler-enabled"))
	mustBindEnv("profiler.enabled", "OPENFGA_PROFILER_ENABLED")

	flags.String("log-format", defaultConfig.Log.Format, "the log format to output logs in")
	mustBindPFlag("log.format", flags.Lookup("log-format"))
	mustBindEnv("log.format", "OPENFGA_LOG_FORMAT")

	flags.String("log-level", defaultConfig.Log.Level, "the log level to use")
	mustBindPFlag("log.level", flags.Lookup("log-level"))
	mustBindEnv("log.level", "OPENFGA_LOG_LEVEL")

	flags.Int("max-tuples-per-write", defaultConfig.MaxTuplesPerWrite, "the maximum allowed number of tuples per Write transaction")
	mustBindPFlag("maxTuplesPerWrite", flags.Lookup("max-tuples-per-write"))
	mustBindEnv("maxTuplesPerWrite", "OPENFGA_MAX_TUPLES_PER_WRITE", "OPENFGA_MAXTUPLESPERWRITE")

	flags.Int("max-types-per-authorization-model", defaultConfig.MaxTypesPerAuthorizationModel, "the maximum allowed number of type definitions per authorization model")
	mustBindPFlag("maxTypesPerAuthorizationModel", flags.Lookup("max-types-per-authorization-model"))
	mustBindEnv("maxTypesPerAuthorizationModel", "OPENFGA_MAX_TYPES_PER_AUTHORIZATION_MODEL", "OPENFGA_MAXTYPESPERAUTHORIZATIONMODEL")

	flags.Int("changelog-horizon-offset", defaultConfig.ChangelogHorizonOffset, "the offset (in minutes) from the current time. Changes that occur after this offset will not be included in the response of ReadChanges")
	mustBindPFlag("changelogHorizonOffset", flags.Lookup("changelog-horizon-offset"))
	mustBindEnv("changelogHorizonOffset", "OPENFGA_CHANGELOG_HORIZON_OFFSET", "OPENFGA_CHANGELOGHORIZONOFFSET")

	flags.Uint32("resolve-node-limit", defaultConfig.ResolveNodeLimit, "defines how deeply nested an authorization model can be")
	mustBindPFlag("resolveNodeLimit", flags.Lookup("resolve-node-limit"))
	mustBindEnv("resolveNodeLimit", "OPENFGA_RESOLVE_NODE_LIMIT", "OPENFGA_RESOLVENODELIMIT")

	flags.Duration("listObjects-deadline", defaultConfig.ListObjectsDeadline, "the timeout deadline for serving ListObjects requests")
	mustBindPFlag("listObjectsDeadline", flags.Lookup("listObjects-deadline"))
	mustBindEnv("listObjectsDeadline", "OPENFGA_LIST_OBJECTS_DEADLINE", "OPENFGA_LISTOBJECTSDEADLINE")

	flags.Uint32("listObjects-max-results", defaultConfig.ListObjectsMaxResults, "the maximum results to return in ListObjects responses")
	mustBindPFlag("listObjectsMaxResults", flags.Lookup("listObjects-max-results"))
	mustBindEnv("listObjectsMaxResults", "OPENFGA_LIST_OBJECTS_MAX_RESULTS", "OPENFGA_LISTOBJECTSMAXRESULTS")

	flags.String("otel-metrics-endpoint", defaultConfig.OpenTelemetry.Endpoint, "OpenTelemetry collector endpoint to use")
	mustBindPFlag("otel.metrics.endpoint", flags.Lookup("otel-metrics-endpoint"))
	mustBindEnv("otel.metrics.endpoint", "OPENFGA_OTEL_METRICS_ENDPOINT")

	flags.String("otel-metrics-protocol", defaultConfig.OpenTelemetry.Protocol, "OpenTelemetry protocol to use to send OTLP metrics")
	mustBindPFlag("otel.metrics.protocol", flags.Lookup("otel-metrics-protocol"))
	mustBindEnv("otel.metrics.protocol", "OPENFGA_OTEL_METRICS_PROTOCOL")
}
