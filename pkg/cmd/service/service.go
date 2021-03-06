package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/server"
	"github.com/openfga/openfga/server/authn"
	"github.com/openfga/openfga/server/authn/oidc"
	"github.com/openfga/openfga/server/authn/presharedkey"
	"github.com/openfga/openfga/server/middleware"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/caching"
	"github.com/openfga/openfga/storage/memory"
	"github.com/openfga/openfga/storage/postgres"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

var (
	ErrInvalidGRPCTLSConfig = errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
	ErrInvalidHTTPTLSConfig = errors.New("'http.tls.cert' and 'http.tls.key' configs must be set")
)

// DatastoreConfig defines OpenFGA server configurations for datastore specific settings.
type DatastoreConfig struct {

	// Engine is the datastore engine to use (e.g. 'memory', 'postgres')
	Engine string
	URI    string

	// MaxCacheSize is the maximum number of cache keys that the storage cache can store before evicting
	// old keys. The storage cache is used to cache query results for various static resources
	// such as type definitions.
	MaxCacheSize int
}

// GRPCConfig defines OpenFGA server configurations for grpc server specific settings.
type GRPCConfig struct {
	Enabled bool
	Addr    string
	TLS     TLSConfig
}

// HTTPConfig defines OpenFGA server configurations for HTTP server specific settings.
type HTTPConfig struct {
	Enabled bool
	Addr    string
	TLS     TLSConfig

	// UpstreamTimeout is the timeout duration for proxying HTTP requests upstream
	// to the grpc endpoint.
	UpstreamTimeout time.Duration

	CORSAllowedOrigins []string `default:"*" split_words:"true"`
	CORSAllowedHeaders []string `default:"*" split_words:"true"`
}

// TLSConfig defines configuration specific to Transport Layer Security (TLS) settings.
type TLSConfig struct {
	Enabled  bool
	CertPath string
	KeyPath  string
}

// AuthnConfig defines OpenFGA server configurations for authentication specific settings.
type AuthnConfig struct {

	// Method is the authentication method that should be enforced (e.g. 'none', 'preshared', 'oidc')
	Method                   string
	*AuthnOIDCConfig         `mapstructure:"oidc"`
	*AuthnPresharedKeyConfig `mapstructure:"preshared"`
}

// AuthnOIDCConfig defines configurations for the 'oidc' method of authentication.
type AuthnOIDCConfig struct {
	Issuer   string
	Audience string
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
}

// PlaygroundConfig defines OpenFGA server configurations for the Playground specific settings.
type PlaygroundConfig struct {
	Enabled bool
	Port    int
}

// OpenFGAConfig defines server configurations specific to the OpenFGA server itself.
type OpenFGAConfig struct {

	// MaxTuplesPerWrite defines the maximum number of tuples per Write endpoint.
	MaxTuplesPerWrite int

	// MaxTypesPerAuthorizationModel defines the maximum number of type definitions per authorization model for the WriteAuthorizationModel endpoint.
	MaxTypesPerAuthorizationModel int

	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int

	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32
}

// ProfilerConfig defines server configurations specific to pprof profiling.
type ProfilerConfig struct {
	Enabled bool
	Addr    string
}

type Config struct {
	// If you change any of these settings, please update the documentation at https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx

	OpenFGA    OpenFGAConfig
	Datastore  DatastoreConfig
	GRPC       GRPCConfig
	HTTP       HTTPConfig
	Authn      AuthnConfig
	Log        LogConfig
	Playground PlaygroundConfig
	Profiler   ProfilerConfig
}

// DefaultConfig returns the OpenFGA server default configurations.
func DefaultConfig() *Config {
	return &Config{
		OpenFGA: OpenFGAConfig{
			MaxTuplesPerWrite:             100,
			MaxTypesPerAuthorizationModel: 100,
			ChangelogHorizonOffset:        0,
			ResolveNodeLimit:              25,
		},
		Datastore: DatastoreConfig{
			Engine:       "memory",
			MaxCacheSize: 100000,
		},
		GRPC: GRPCConfig{
			Enabled: true,
			Addr:    ":8081",
			TLS:     TLSConfig{Enabled: false},
		},
		HTTP: HTTPConfig{
			Enabled:            true,
			Addr:               ":8080",
			TLS:                TLSConfig{Enabled: false},
			UpstreamTimeout:    3 * time.Second,
			CORSAllowedOrigins: []string{"*"},
			CORSAllowedHeaders: []string{"*"},
		},
		Authn: AuthnConfig{
			Method:                  "none",
			AuthnPresharedKeyConfig: &AuthnPresharedKeyConfig{},
			AuthnOIDCConfig:         &AuthnOIDCConfig{},
		},
		Log: LogConfig{
			Format: "text",
		},
		Playground: PlaygroundConfig{
			Enabled: true,
			Port:    3000,
		},
		Profiler: ProfilerConfig{
			Enabled: false,
			Addr:    ":3001",
		},
	}
}

// GetServiceConfig returns the OpenFGA server configuration based on the values provided in the server's 'config.yaml' file.
// The 'config.yaml' file is loaded from '/etc/openfga', '$HOME/.openfga', or the current working directory. If no configuration
// file is present, the default values are returned.
func GetServiceConfig() (*Config, error) {

	config := DefaultConfig()

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}
	viper.SetEnvPrefix("OPENFGA")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		_, ok := err.(viper.ConfigFileNotFoundError)
		if !ok {
			return nil, fmt.Errorf("failed to load server config: %w", err)
		}
	}

	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server config: %w", err)
	}

	return config, nil
}

type service struct {
	server        *server.Server
	datastore     storage.OpenFGADatastore
	authenticator authn.Authenticator
}

func (s *service) Close(ctx context.Context) error {
	s.authenticator.Close()

	return s.datastore.Close(ctx)
}

func (s *service) Run(ctx context.Context) error {
	return s.server.Run(ctx)
}

func BuildService(config *Config, logger logger.Logger) (*service, error) {
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewTokenEncoder(encrypter.NewNoopEncrypter(), encoder.NewBase64Encoder())

	var datastore storage.OpenFGADatastore
	var err error
	switch config.Datastore.Engine {
	case "memory":
		datastore = memory.New(tracer, config.OpenFGA.MaxTuplesPerWrite, config.OpenFGA.MaxTypesPerAuthorizationModel)
	case "postgres":
		opts := []postgres.PostgresOption{
			postgres.WithLogger(logger),
			postgres.WithTracer(tracer),
		}

		datastore, err = postgres.NewPostgresDatastore(config.Datastore.URI, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize postgres datastore: %v", err)
		}
	default:
		return nil, fmt.Errorf("storage engine '%s' is unsupported", config.Datastore.Engine)
	}

	logger.Info(fmt.Sprintf("using '%v' storage engine", config.Datastore.Engine))

	var grpcTLSConfig *server.TLSConfig
	if config.GRPC.TLS.Enabled {
		if config.GRPC.TLS.CertPath == "" || config.GRPC.TLS.KeyPath == "" {
			return nil, ErrInvalidGRPCTLSConfig
		}
		grpcTLSConfig = &server.TLSConfig{
			CertPath: config.GRPC.TLS.CertPath,
			KeyPath:  config.GRPC.TLS.KeyPath,
		}
		logger.Info("grpc TLS is enabled, serving connections using the provided certificate")
	} else {
		logger.Warn("grpc TLS is disabled, serving connections using insecure plaintext")
	}

	var httpTLSConfig *server.TLSConfig
	if config.HTTP.TLS.Enabled {
		if config.HTTP.TLS.CertPath == "" || config.HTTP.TLS.KeyPath == "" {
			return nil, ErrInvalidHTTPTLSConfig
		}
		httpTLSConfig = &server.TLSConfig{
			CertPath: config.HTTP.TLS.CertPath,
			KeyPath:  config.HTTP.TLS.KeyPath,
		}
		logger.Info("HTTP TLS is enabled, serving HTTP connections using the provided certificate")
	} else {
		logger.Warn("HTTP TLS is disabled, serving connections using insecure plaintext")
	}

	var authenticator authn.Authenticator
	switch config.Authn.Method {
	case "none":
		logger.Warn("authentication is disabled")
		authenticator = authn.NoopAuthenticator{}
	case "preshared":
		logger.Info("using 'preshared' authentication")
		authenticator, err = presharedkey.NewPresharedKeyAuthenticator(config.Authn.Keys)
	case "oidc":
		logger.Info("using 'oidc' authentication")
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.Authn.Issuer, config.Authn.Audience)
	default:
		return nil, fmt.Errorf("unsupported authentication method '%v'", config.Authn.Method)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authenticator: %v", err)
	}

	interceptors := []grpc.UnaryServerInterceptor{
		grpc_auth.UnaryServerInterceptor(middleware.AuthFunc(authenticator)),
	}

	openFgaServer, err := server.New(&server.Dependencies{
		Datastore:    caching.NewCachedOpenFGADatastore(datastore, config.Datastore.MaxCacheSize),
		Tracer:       tracer,
		Logger:       logger,
		Meter:        meter,
		TokenEncoder: tokenEncoder,
	}, &server.Config{
		GRPCServer: server.GRPCServerConfig{
			Addr:      config.GRPC.Addr,
			TLSConfig: grpcTLSConfig,
		},
		HTTPServer: server.HTTPServerConfig{
			Enabled:            config.HTTP.Enabled,
			Addr:               config.HTTP.Addr,
			TLSConfig:          httpTLSConfig,
			UpstreamTimeout:    config.HTTP.UpstreamTimeout,
			CORSAllowedOrigins: config.HTTP.CORSAllowedOrigins,
			CORSAllowedHeaders: config.HTTP.CORSAllowedHeaders,
		},
		ResolveNodeLimit:       config.OpenFGA.ResolveNodeLimit,
		ChangelogHorizonOffset: config.OpenFGA.ChangelogHorizonOffset,
		UnaryInterceptors:      interceptors,
		MuxOptions:             nil,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to initialize openfga server: %v", err)
	}

	return &service{
		server:        openFgaServer,
		datastore:     datastore,
		authenticator: authenticator,
	}, nil
}
