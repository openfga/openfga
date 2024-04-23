// Package config contains all knobs and defaults used to configure features of
// OpenFGA when running as a standalone server.
package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"time"
)

const (
	DefaultMaxRPCMessageSizeInBytes         = 512 * 1_204 // 512 KB
	DefaultMaxTuplesPerWrite                = 100
	DefaultMaxTypesPerAuthorizationModel    = 100
	DefaultMaxAuthorizationModelSizeInBytes = 256 * 1_024
	DefaultChangelogHorizonOffset           = 0
	DefaultResolveNodeLimit                 = 25
	DefaultResolveNodeBreadthLimit          = 100
	DefaultListObjectsDeadline              = 3 * time.Second
	DefaultListObjectsMaxResults            = 1000
	DefaultMaxConcurrentReadsForCheck       = math.MaxUint32
	DefaultMaxConcurrentReadsForListObjects = math.MaxUint32

	DefaultWriteContextByteLimit = 32 * 1_024 // 32KB
	DefaultCheckQueryCacheLimit  = 10000
	DefaultCheckQueryCacheTTL    = 10 * time.Second
	DefaultCheckQueryCacheEnable = false

	// care should be taken here - decreasing can cause API compatibility problems with Conditions
	DefaultMaxConditionEvaluationCost = 100
	DefaultInterruptCheckFrequency    = 100

	DefaultCheckDispatchThrottlingEnabled   = false
	DefaultCheckDispatchThrottlingFrequency = 10 * time.Microsecond
	DefaultCheckDispatchThrottlingThreshold = 100

	DefaultListObjectsDispatchThrottlingEnabled   = false
	DefaultListObjectsDispatchThrottlingFrequency = 10 * time.Microsecond
	DefaultListObjectsDispatchThrottlingThreshold = 100
)

type DatastoreMetricsConfig struct {
	// Enabled enables export of the Datastore metrics.
	Enabled bool
}

// DatastoreConfig defines OpenFGA server configurations for datastore specific settings.
type DatastoreConfig struct {
	// Engine is the datastore engine to use (e.g. 'memory', 'postgres', 'mysql')
	Engine   string
	URI      string
	Username string
	Password string

	// MaxCacheSize is the maximum number of cache keys that the storage cache can store before
	// evicting
	// old keys. The storage cache is used to cache query results for various static resources
	// such as type definitions.
	MaxCacheSize int

	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns int

	// MaxIdleConns is the maximum number of connections to the datastore in the idle connection
	// pool.
	MaxIdleConns int

	// ConnMaxIdleTime is the maximum amount of time a connection to the datastore may be idle.
	ConnMaxIdleTime time.Duration

	// ConnMaxLifetime is the maximum amount of time a connection to the datastore may be reused.
	ConnMaxLifetime time.Duration

	// Metrics is configuration for the Datastore metrics.
	Metrics DatastoreMetricsConfig
}

// GRPCConfig defines OpenFGA server configurations for grpc server specific settings.
type GRPCConfig struct {
	Addr string
	TLS  *TLSConfig
}

// HTTPConfig defines OpenFGA server configurations for HTTP server specific settings.
type HTTPConfig struct {
	Enabled bool
	Addr    string
	TLS     *TLSConfig

	// UpstreamTimeout is the timeout duration for proxying HTTP requests upstream
	// to the grpc endpoint. It cannot be smaller than Config.ListObjectsDeadline.
	UpstreamTimeout time.Duration

	CORSAllowedOrigins []string
	CORSAllowedHeaders []string
}

// TLSConfig defines configuration specific to Transport Layer Security (TLS) settings.
type TLSConfig struct {
	Enabled  bool
	CertPath string `mapstructure:"cert"`
	KeyPath  string `mapstructure:"key"`
}

// AuthnConfig defines OpenFGA server configurations for authentication specific settings.
type AuthnConfig struct {

	// Method is the authentication method that should be enforced (e.g. 'none', 'preshared',
	// 'oidc')
	Method                   string
	*AuthnOIDCConfig         `mapstructure:"oidc"`
	*AuthnPresharedKeyConfig `mapstructure:"preshared"`
}

// AuthnOIDCConfig defines configurations for the 'oidc' method of authentication.
type AuthnOIDCConfig struct {
	Issuer        string
	IssuerAliases []string
	Audience      string
}

// AuthnPresharedKeyConfig defines configurations for the 'preshared' method of authentication.
type AuthnPresharedKeyConfig struct {
	// Keys define the preshared keys to verify authn tokens against.
	Keys []string
}

// LogConfig defines OpenFGA server configurations for log specific settings. For production we
// recommend using the 'json' log format.
type LogConfig struct {
	// Format is the log format to use in the log output (e.g. 'text' or 'json')
	Format string

	// Level is the log level to use in the log output (e.g. 'none', 'debug', or 'info')
	Level string

	// Format of the timestamp in the log output (e.g. 'Unix'(default) or 'ISO8601')
	TimestampFormat string
}

type TraceConfig struct {
	Enabled     bool
	OTLP        OTLPTraceConfig `mapstructure:"otlp"`
	SampleRatio float64
	ServiceName string
}

type OTLPTraceConfig struct {
	Endpoint string
	TLS      OTLPTraceTLSConfig
}

type OTLPTraceTLSConfig struct {
	Enabled bool
}

// PlaygroundConfig defines OpenFGA server configurations for the Playground specific settings.
type PlaygroundConfig struct {
	Enabled bool
	Port    int
}

// ProfilerConfig defines server configurations specific to pprof profiling.
type ProfilerConfig struct {
	Enabled bool
	Addr    string
}

// MetricConfig defines configurations for serving custom metrics from OpenFGA.
type MetricConfig struct {
	Enabled             bool
	Addr                string
	EnableRPCHistograms bool
}

// CheckQueryCache defines configuration for caching when resolving check
type CheckQueryCache struct {
	Enabled bool
	Limit   uint32 // (in items)
	TTL     time.Duration
}

// DispatchThrottlingConfig defines configurations for dispatch throttling
type DispatchThrottlingConfig struct {
	Enabled   bool
	Frequency time.Duration
	Threshold uint32
}

type Config struct {
	// If you change any of these settings, please update the documentation at
	// https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx

	// ListObjectsDeadline defines the maximum amount of time to accumulate ListObjects results
	// before the server will respond. This is to protect the server from misuse of the
	// ListObjects endpoints. It cannot be larger than HTTPConfig.UpstreamTimeout.
	ListObjectsDeadline time.Duration

	// ListObjectsMaxResults defines the maximum number of results to accumulate
	// before the non-streaming ListObjects API will respond to the client.
	// This is to protect the server from misuse of the ListObjects endpoints.
	ListObjectsMaxResults uint32

	// MaxTuplesPerWrite defines the maximum number of tuples per Write endpoint.
	MaxTuplesPerWrite int

	// MaxTypesPerAuthorizationModel defines the maximum number of type definitions per
	// authorization model for the WriteAuthorizationModel endpoint.
	MaxTypesPerAuthorizationModel int

	// MaxAuthorizationModelSizeInBytes defines the maximum size in bytes allowed for
	// persisting an Authorization Model.
	MaxAuthorizationModelSizeInBytes int

	// MaxConcurrentReadsForListObjects defines the maximum number of concurrent database reads
	// allowed in ListObjects queries
	MaxConcurrentReadsForListObjects uint32

	// MaxConcurrentReadsForCheck defines the maximum number of concurrent database reads allowed in
	// Check queries
	MaxConcurrentReadsForCheck uint32

	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur
	// after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int

	// Experimentals is a list of the experimental features to enable in the OpenFGA server.
	Experimentals []string

	// ResolveNodeLimit indicates how deeply nested an authorization model can be before a query
	// errors out.
	ResolveNodeLimit uint32

	// ResolveNodeBreadthLimit indicates how many nodes on a given level can be evaluated
	// concurrently in a query
	ResolveNodeBreadthLimit uint32

	Datastore                     DatastoreConfig
	GRPC                          GRPCConfig
	HTTP                          HTTPConfig
	Authn                         AuthnConfig
	Log                           LogConfig
	Trace                         TraceConfig
	Playground                    PlaygroundConfig
	Profiler                      ProfilerConfig
	Metrics                       MetricConfig
	CheckQueryCache               CheckQueryCache
	CheckDispatchThrottling       DispatchThrottlingConfig
	ListObjectsDispatchThrottling DispatchThrottlingConfig

	RequestDurationDatastoreQueryCountBuckets []string
	RequestDurationDispatchCountBuckets       []string
}

func (cfg *Config) Verify() error {
	if cfg.ListObjectsDeadline > cfg.HTTP.UpstreamTimeout {
		return fmt.Errorf(
			"config 'http.upstreamTimeout' (%s) cannot be lower than 'listObjectsDeadline' config (%s)",
			cfg.HTTP.UpstreamTimeout,
			cfg.ListObjectsDeadline,
		)
	}

	if cfg.Log.Format != "text" && cfg.Log.Format != "json" {
		return fmt.Errorf("config 'log.format' must be one of ['text', 'json']")
	}

	if cfg.Log.Level != "none" &&
		cfg.Log.Level != "debug" &&
		cfg.Log.Level != "info" &&
		cfg.Log.Level != "warn" &&
		cfg.Log.Level != "error" &&
		cfg.Log.Level != "panic" &&
		cfg.Log.Level != "fatal" {
		return fmt.Errorf(
			"config 'log.level' must be one of ['none', 'debug', 'info', 'warn', 'error', 'panic', 'fatal']",
		)
	}

	if cfg.Log.TimestampFormat != "Unix" && cfg.Log.TimestampFormat != "ISO8601" {
		return fmt.Errorf("config 'log.TimestampFormat' must be one of ['Unix', 'ISO8601']")
	}

	if cfg.Playground.Enabled {
		if !cfg.HTTP.Enabled {
			return errors.New("the HTTP server must be enabled to run the openfga playground")
		}

		if !(cfg.Authn.Method == "none" || cfg.Authn.Method == "preshared") {
			return errors.New("the playground only supports authn methods 'none' and 'preshared'")
		}
	}

	if cfg.HTTP.TLS.Enabled {
		if cfg.HTTP.TLS.CertPath == "" || cfg.HTTP.TLS.KeyPath == "" {
			return errors.New("'http.tls.cert' and 'http.tls.key' configs must be set")
		}
	}

	if cfg.GRPC.TLS.Enabled {
		if cfg.GRPC.TLS.CertPath == "" || cfg.GRPC.TLS.KeyPath == "" {
			return errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
		}
	}

	if len(cfg.RequestDurationDatastoreQueryCountBuckets) == 0 {
		return errors.New("request duration datastore query count buckets must not be empty")
	}
	for _, val := range cfg.RequestDurationDatastoreQueryCountBuckets {
		valInt, err := strconv.Atoi(val)
		if err != nil || valInt < 0 {
			return errors.New(
				"request duration datastore query count bucket items must be non-negative integer",
			)
		}
	}

	if len(cfg.RequestDurationDispatchCountBuckets) == 0 {
		return errors.New("request duration datastore dispatch count buckets must not be empty")
	}
	for _, val := range cfg.RequestDurationDispatchCountBuckets {
		valInt, err := strconv.Atoi(val)
		if err != nil || valInt < 0 {
			return errors.New(
				"request duration dispatch count bucket items must be non-negative integer",
			)
		}
	}

	if cfg.CheckDispatchThrottling.Enabled {
		if cfg.CheckDispatchThrottling.Frequency <= 0 {
			return errors.New("dispatch throttling frequency must be non-negative time duration")
		}
		if cfg.CheckDispatchThrottling.Threshold <= 0 {
			return errors.New("dispatch throttling threshold must be non-negative integer")
		}
	}

	if cfg.ListObjectsDispatchThrottling.Enabled {
		if cfg.ListObjectsDispatchThrottling.Frequency <= 0 {
			return errors.New("dispatch throttling frequency must be non-negative time duration")
		}
		if cfg.ListObjectsDispatchThrottling.Threshold <= 0 {
			return errors.New("dispatch throttling threshold must be non-negative integer")
		}
	}

	return nil
}

// DefaultConfig is the OpenFGA server default configurations.
func DefaultConfig() *Config {
	return &Config{
		MaxTuplesPerWrite:                         DefaultMaxTuplesPerWrite,
		MaxTypesPerAuthorizationModel:             DefaultMaxTypesPerAuthorizationModel,
		MaxAuthorizationModelSizeInBytes:          DefaultMaxAuthorizationModelSizeInBytes,
		MaxConcurrentReadsForCheck:                DefaultMaxConcurrentReadsForCheck,
		MaxConcurrentReadsForListObjects:          DefaultMaxConcurrentReadsForListObjects,
		ChangelogHorizonOffset:                    DefaultChangelogHorizonOffset,
		ResolveNodeLimit:                          DefaultResolveNodeLimit,
		ResolveNodeBreadthLimit:                   DefaultResolveNodeBreadthLimit,
		Experimentals:                             []string{},
		ListObjectsDeadline:                       DefaultListObjectsDeadline,
		ListObjectsMaxResults:                     DefaultListObjectsMaxResults,
		RequestDurationDatastoreQueryCountBuckets: []string{"50", "200"},
		RequestDurationDispatchCountBuckets:       []string{"50", "200"},
		Datastore: DatastoreConfig{
			Engine:       "memory",
			MaxCacheSize: 100000,
			MaxIdleConns: 10,
			MaxOpenConns: 30,
		},
		GRPC: GRPCConfig{
			Addr: "0.0.0.0:8081",
			TLS:  &TLSConfig{Enabled: false},
		},
		HTTP: HTTPConfig{
			Enabled:            true,
			Addr:               "0.0.0.0:8080",
			TLS:                &TLSConfig{Enabled: false},
			UpstreamTimeout:    5 * time.Second,
			CORSAllowedOrigins: []string{"*"},
			CORSAllowedHeaders: []string{"*"},
		},
		Authn: AuthnConfig{
			Method:                  "none",
			AuthnPresharedKeyConfig: &AuthnPresharedKeyConfig{},
			AuthnOIDCConfig:         &AuthnOIDCConfig{},
		},
		Log: LogConfig{
			Format:          "text",
			Level:           "info",
			TimestampFormat: "Unix",
		},
		Trace: TraceConfig{
			Enabled: false,
			OTLP: OTLPTraceConfig{
				Endpoint: "0.0.0.0:4317",
				TLS: OTLPTraceTLSConfig{
					Enabled: false,
				},
			},
			SampleRatio: 0.2,
			ServiceName: "openfga",
		},
		Playground: PlaygroundConfig{
			Enabled: true,
			Port:    3000,
		},
		Profiler: ProfilerConfig{
			Enabled: false,
			Addr:    ":3001",
		},
		Metrics: MetricConfig{
			Enabled:             true,
			Addr:                "0.0.0.0:2112",
			EnableRPCHistograms: false,
		},
		CheckQueryCache: CheckQueryCache{
			Enabled: DefaultCheckQueryCacheEnable,
			Limit:   DefaultCheckQueryCacheLimit,
			TTL:     DefaultCheckQueryCacheTTL,
		},
		CheckDispatchThrottling: DispatchThrottlingConfig{
			Enabled:   DefaultCheckDispatchThrottlingEnabled,
			Frequency: DefaultCheckDispatchThrottlingFrequency,
			Threshold: DefaultCheckDispatchThrottlingThreshold,
		},
		ListObjectsDispatchThrottling: DispatchThrottlingConfig{
			Enabled:   DefaultCheckDispatchThrottlingEnabled,
			Frequency: DefaultCheckDispatchThrottlingFrequency,
			Threshold: DefaultCheckDispatchThrottlingThreshold,
		},
	}
}

// MustDefaultConfig returns default server config with the playground, tracing and metrics turned off.
func MustDefaultConfig() *Config {
	config := DefaultConfig()

	config.Playground.Enabled = false
	config.Metrics.Enabled = false

	return config
}

// MustDefaultConfigWithRandomPorts returns default server config but with random ports for the grpc and http addresses
// and with the playground, tracing and metrics turned off.
// This function may panic if somehow a random port cannot be chosen.
func MustDefaultConfigWithRandomPorts() *Config {
	config := MustDefaultConfig()

	httpPort, httpPortReleaser := TCPRandomPort()
	defer httpPortReleaser()
	grpcPort, grpcPortReleaser := TCPRandomPort()
	defer grpcPortReleaser()

	config.GRPC.Addr = fmt.Sprintf("0.0.0.0:%d", grpcPort)
	config.HTTP.Addr = fmt.Sprintf("0.0.0.0:%d", httpPort)

	return config
}

// TCPRandomPort tries to find a random TCP Port. If it can't find one, it panics. Else, it returns the port and a function that releases the port.
// It is the responsibility of the caller to call the release function right before trying to listen on the given port.
func TCPRandomPort() (int, func()) {
	l, err := net.Listen("tcp", "")
	if err != nil {
		panic(err)
	}
	return l.Addr().(*net.TCPAddr).Port, func() {
		l.Close()
	}
}
