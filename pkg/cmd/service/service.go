package service

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"

	"github.com/go-errors/errors"
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
	"github.com/openfga/openfga/storage/mysql"
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

	// Engine is the datastore engine to use (e.g. 'memory', 'postgres', 'mysql')
	Engine string
	URI    string

	// MaxCacheSize is the maximum number of cache keys that the storage cache can store before evicting
	// old keys. The storage cache is used to cache query results for various static resources
	// such as type definitions.
	MaxCacheSize int
}

// GRPCConfig defines OpenFGA server configurations for grpc server specific settings.
type GRPCConfig struct {
	Addr string
	TLS  TLSConfig
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
	CertPath string `mapstructure:"cert"`
	KeyPath  string `mapstructure:"key"`
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

// ProfilerConfig defines server configurations specific to pprof profiling.
type ProfilerConfig struct {
	Enabled bool
	Addr    string
}

type Config struct {
	// If you change any of these settings, please update the documentation at https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx

	// ListObjectsDeadline defines the maximum amount of time to accumulate ListObjects results
	// before the server will respond. This is to protect the server from misuse of the
	// ListObjects endpoints.
	ListObjectsDeadline time.Duration

	// ListObjectsMaxResults defines the maximum number of ListObjects results to accumulate
	// before the server will respond. This is to protect the server from misuse of the
	// ListObjects endpoints.
	ListObjectsMaxResults uint32

	// MaxTuplesPerWrite defines the maximum number of tuples per Write endpoint.
	MaxTuplesPerWrite int

	// MaxTypesPerAuthorizationModel defines the maximum number of type definitions per authorization model for the WriteAuthorizationModel endpoint.
	MaxTypesPerAuthorizationModel int

	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int

	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32

	Datastore  DatastoreConfig
	GRPC       GRPCConfig
	HTTP       HTTPConfig
	Authn      AuthnConfig
	Log        LogConfig
	Playground PlaygroundConfig
	Profiler   ProfilerConfig
}

func DefaultConfigWithRandomPorts() (*Config, error) {
	config := DefaultConfig()

	l, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	defer l.Close()
	httpPort := l.Addr().(*net.TCPAddr).Port

	l, err = net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	defer l.Close()
	grpcPort := l.Addr().(*net.TCPAddr).Port

	config.GRPC.Addr = fmt.Sprintf("0.0.0.0:%d", grpcPort)
	config.HTTP.Addr = fmt.Sprintf("0.0.0.0:%d", httpPort)

	return config, nil
}

// DefaultConfig returns the OpenFGA server default configurations.
func DefaultConfig() *Config {
	return &Config{
		MaxTuplesPerWrite:             100,
		MaxTypesPerAuthorizationModel: 100,
		ChangelogHorizonOffset:        0,
		ResolveNodeLimit:              25,
		ListObjectsDeadline:           3 * time.Second, // there is a 3-second timeout elsewhere
		ListObjectsMaxResults:         1000,
		Datastore: DatastoreConfig{
			Engine:       "memory",
			MaxCacheSize: 100000,
		},
		GRPC: GRPCConfig{
			Addr: "0.0.0.0:8081",
			TLS:  TLSConfig{Enabled: false},
		},
		HTTP: HTTPConfig{
			Enabled:            true,
			Addr:               "0.0.0.0:8080",
			TLS:                TLSConfig{Enabled: false},
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
			return nil, errors.Errorf("failed to load server config: %w", err)
		}
	}

	if err := viper.Unmarshal(config); err != nil {
		return nil, errors.Errorf("failed to unmarshal server config: %w", err)
	}

	return config, nil
}

type service struct {
	server        *server.Server
	grpcAddr      netip.AddrPort
	httpAddr      netip.AddrPort
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

// GetHTTPAddrPort returns the configured or auto-assigned port that the underlying HTTP service is running on.
func (s *service) GetHTTPAddrPort() netip.AddrPort {
	return s.httpAddr
}

// GetGRPCAddrPort returns the configured or auto-assigned port that the underlying grpc service is running on.
func (s *service) GetGRPCAddrPort() netip.AddrPort {
	return s.grpcAddr
}

func BuildService(config *Config, logger logger.Logger) (*service, error) {
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewTokenEncoder(encrypter.NewNoopEncrypter(), encoder.NewBase64Encoder())

	var datastore storage.OpenFGADatastore
	var err error
	switch config.Datastore.Engine {
	case "memory":
		datastore = memory.New(tracer, config.MaxTuplesPerWrite, config.MaxTypesPerAuthorizationModel)
	case "mysql":
		opts := []mysql.MySQLOption{
			mysql.WithLogger(logger),
			mysql.WithTracer(tracer),
		}

		datastore, err = mysql.NewMySQLDatastore(config.Datastore.URI, opts...)
		if err != nil {
			return nil, errors.Errorf("failed to initialize mysql datastore: %v", err)
		}
	case "postgres":
		opts := []postgres.PostgresOption{
			postgres.WithLogger(logger),
			postgres.WithTracer(tracer),
		}

		datastore, err = postgres.NewPostgresDatastore(config.Datastore.URI, opts...)
		if err != nil {
			return nil, errors.Errorf("failed to initialize postgres datastore: %v", err)
		}
	default:
		return nil, errors.Errorf("storage engine '%s' is unsupported", config.Datastore.Engine)
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
		return nil, errors.Errorf("unsupported authentication method '%v'", config.Authn.Method)
	}
	if err != nil {
		return nil, errors.Errorf("failed to initialize authenticator: %v", err)
	}

	unaryServerInterceptors := []grpc.UnaryServerInterceptor{
		grpc_auth.UnaryServerInterceptor(middleware.AuthFunc(authenticator)),
		middleware.NewErrorLoggingInterceptor(logger),
	}

	streamingServerInterceptors := []grpc.StreamServerInterceptor{
		grpc_auth.StreamServerInterceptor(middleware.AuthFunc(authenticator)),
		middleware.NewStreamingErrorLoggingInterceptor(logger),
	}

	grpcHostAddr, grpcHostPort, err := net.SplitHostPort(config.GRPC.Addr)
	if err != nil {
		return nil, errors.Errorf("`grpc.addr` config must be in the form [host]:port")
	}

	if grpcHostAddr == "" {
		grpcHostAddr = "0.0.0.0"
	}

	grpcAddr, err := netip.ParseAddrPort(fmt.Sprintf("%s:%s", grpcHostAddr, grpcHostPort))
	if err != nil {
		return nil, errors.Errorf("failed to parse the 'grpc.addr' config: %v", err)
	}

	httpHostAddr, httpHostPort, err := net.SplitHostPort(config.HTTP.Addr)
	if err != nil {
		return nil, errors.Errorf("`http.addr` config must be in the form [host]:port")
	}

	if httpHostAddr == "" {
		httpHostAddr = "0.0.0.0"
	}

	httpAddr, err := netip.ParseAddrPort(fmt.Sprintf("%s:%s", httpHostAddr, httpHostPort))
	if err != nil {
		return nil, errors.Errorf("failed to parse the 'http.addr' config: %v", err)
	}

	openFgaServer, err := server.New(&server.Dependencies{
		Datastore:    caching.NewCachedOpenFGADatastore(datastore, config.Datastore.MaxCacheSize),
		Tracer:       tracer,
		Logger:       logger,
		Meter:        meter,
		TokenEncoder: tokenEncoder,
	}, &server.Config{
		GRPCServer: server.GRPCServerConfig{
			Addr:      grpcAddr,
			TLSConfig: grpcTLSConfig,
		},
		HTTPServer: server.HTTPServerConfig{
			Enabled:            config.HTTP.Enabled,
			Addr:               httpAddr,
			TLSConfig:          httpTLSConfig,
			UpstreamTimeout:    config.HTTP.UpstreamTimeout,
			CORSAllowedOrigins: config.HTTP.CORSAllowedOrigins,
			CORSAllowedHeaders: config.HTTP.CORSAllowedHeaders,
		},
		ResolveNodeLimit:       config.ResolveNodeLimit,
		ChangelogHorizonOffset: config.ChangelogHorizonOffset,
		ListObjectsDeadline:    config.ListObjectsDeadline,
		ListObjectsMaxResults:  config.ListObjectsMaxResults,
		UnaryInterceptors:      unaryServerInterceptors,
		StreamingInterceptors:  streamingServerInterceptors,
		MuxOptions:             nil,
	})
	if err != nil {
		return nil, errors.Errorf("failed to initialize openfga server: %v", err)
	}

	return &service{
		server:        openFgaServer,
		grpcAddr:      grpcAddr,
		httpAddr:      httpAddr,
		datastore:     datastore,
		authenticator: authenticator,
	}, nil
}
