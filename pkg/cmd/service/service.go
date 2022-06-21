package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/server"
	"github.com/openfga/openfga/server/authentication"
	"github.com/openfga/openfga/server/authentication/oidc"
	"github.com/openfga/openfga/server/authentication/presharedkey"
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

type DatabaseConfig struct {
	Engine string
	URI    string
}

type GRPCConfig struct {
	Enabled bool
	Port    int
	TLS     TLSConfig
}

type HTTPConfig struct {
	Enabled bool
	Port    int
	TLS     TLSConfig
}

type TLSConfig struct {
	Enabled  bool
	CertPath string
	KeyPath  string
}

type AuthnConfig struct {
	Method                   string
	*AuthnOIDCConfig         `mapstructure:"oidc"`
	*AuthnPresharedKeyConfig `mapstructure:"preshared"`
}

type AuthnOIDCConfig struct {
	Issuer   string
	Audience string
}

type AuthnPresharedKeyConfig struct {
	Keys []string
}

type LogConfig struct {
	Format string
}

type Config struct {
	// If you change any of these settings, please update the documentation at https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx

	DatabaseConfig `mapstructure:"database"`
	GRPCConfig     `mapstructure:"grpc"`
	HTTPConfig     `mapstructure:"http"`
	AuthnConfig    `mapstructure:"authn"`
	LogConfig      `mapstructure:"log"`

	DatastoreMaxCacheSize int `default:"100000" split_words:"true"`

	MaxTuplesPerWrite             int `default:"100" split_words:"true"`
	MaxTypesPerAuthorizationModel int `default:"100" split_words:"true"`
	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int `default:"0" split_words:"true" `
	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32 `default:"25" split_words:"true"`
	// RequestTimeout is a limit on the time a request may take. If the value is 0, then there is no timeout.
	RequestTimeout time.Duration `default:"0s" split_words:"true"`
}

func GetServiceConfig() Config {

	config := Config{
		DatabaseConfig: DatabaseConfig{
			Engine: "memory",
		},
		GRPCConfig: GRPCConfig{
			Enabled: true,
			Port:    8081,
			TLS:     TLSConfig{Enabled: false},
		},
		HTTPConfig: HTTPConfig{
			Enabled: true,
			Port:    8080,
			TLS:     TLSConfig{Enabled: false},
		},
		AuthnConfig: AuthnConfig{
			Method: "none",
		},
		LogConfig: LogConfig{
			Format: "text",
		},
	}

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}
	viper.AutomaticEnv()

	err := viper.ReadInConfig()
	if err != nil {
		_, ok := err.(viper.ConfigFileNotFoundError)
		if ok {
			//logger.Fatal(fmt.Sprintf("config not found in paths [%s]", strings.Join(configPaths, ",")))
		}

		//logger.Fatal(fmt.Sprintf("failed to load server config: %v", err))
	}

	if err := viper.Unmarshal(&config); err != nil {
		// handle error
		panic(err)
	}

	return config
}

type service struct {
	server        *server.Server
	datastore     storage.OpenFGADatastore
	authenticator authentication.Authenticator
}

func (s *service) Close(ctx context.Context) error {
	s.authenticator.Close()

	return s.datastore.Close(ctx)
}

func (s *service) Run(ctx context.Context) error {
	return s.server.Run(ctx)
}

func BuildService(config Config, logger logger.Logger) (*service, error) {
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewBase64Encoder()

	var datastore storage.OpenFGADatastore
	var err error
	switch config.DatabaseConfig.Engine {
	case "memory":
		datastore = memory.New(tracer, config.MaxTuplesPerWrite, config.MaxTypesPerAuthorizationModel)
	case "postgres":
		opts := []postgres.PostgresOption{
			postgres.WithLogger(logger),
			postgres.WithTracer(tracer),
		}

		datastore, err = postgres.NewPostgresDatastore(config.DatabaseConfig.URI, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize postgres datastore: %v", err)
		}
	default:
		return nil, fmt.Errorf("storage engine '%s' is unsupported", config.DatabaseConfig.Engine)
	}

	logger.Info(fmt.Sprintf("using '%v' storage engine", config.DatabaseConfig.Engine))

	var grpcTLSConfig *server.TLSConfig
	if config.GRPCConfig.TLS.Enabled {
		if config.GRPCConfig.TLS.CertPath == "" || config.GRPCConfig.TLS.KeyPath == "" {
			return nil, ErrInvalidGRPCTLSConfig
		}
		grpcTLSConfig = &server.TLSConfig{
			CertPath: config.GRPCConfig.TLS.CertPath,
			KeyPath:  config.GRPCConfig.TLS.KeyPath,
		}
		logger.Info("grpc TLS is enabled, serving connections using the provided certificate")
	} else {
		logger.Warn("grpc TLS is disabled, serving connections using insecure plaintext")
	}

	var httpTLSConfig *server.TLSConfig
	if config.HTTPConfig.TLS.Enabled {
		if config.HTTPConfig.TLS.CertPath == "" || config.HTTPConfig.TLS.KeyPath == "" {
			return nil, ErrInvalidHTTPTLSConfig
		}
		httpTLSConfig = &server.TLSConfig{
			CertPath: config.HTTPConfig.TLS.CertPath,
			KeyPath:  config.HTTPConfig.TLS.KeyPath,
		}
		logger.Info("HTTP TLS enabled, serving connections using the provided certificate")
	} else {
		logger.Warn("HTTP TLS is disabled, serving connections using insecure plaintext")
	}

	var authenticator authentication.Authenticator
	switch config.AuthnConfig.Method {
	case "none":
		logger.Warn("authentication is disabled")
		authenticator = authentication.NoopAuthenticator{}
	case "preshared":
		logger.Info("using 'preshared' authentication")
		authenticator, err = presharedkey.NewPresharedKeyAuthenticator(config.AuthnConfig.Keys)
	case "oidc":
		logger.Info("using 'oidc' authentication")
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.AuthnConfig.Issuer, config.AuthnConfig.Audience)
	default:
		return nil, fmt.Errorf("unsupported authentication method '%v'", config.AuthnConfig.Method)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authenticator: %v", err)
	}

	interceptors := []grpc.UnaryServerInterceptor{
		middleware.NewAuthenticationInterceptor(authenticator),
	}

	openFgaServer, err := server.New(&server.Dependencies{
		Datastore:    caching.NewCachedOpenFGADatastore(datastore, config.DatastoreMaxCacheSize),
		Tracer:       tracer,
		Logger:       logger,
		Meter:        meter,
		TokenEncoder: tokenEncoder,
	}, &server.Config{
		GRPCServer: server.GRPCServerConfig{
			Port:      config.GRPCConfig.Port,
			TLSConfig: grpcTLSConfig,
		},
		HTTPServer: server.HTTPServerConfig{
			Port:      config.HTTPConfig.Port,
			TLSConfig: httpTLSConfig,
		},
		ResolveNodeLimit:       config.ResolveNodeLimit,
		ChangelogHorizonOffset: config.ChangelogHorizonOffset,
		UnaryInterceptors:      interceptors,
		MuxOptions:             nil,
		RequestTimeout:         config.RequestTimeout,
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
