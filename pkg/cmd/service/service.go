package service

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"github.com/kelseyhightower/envconfig"
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
	"google.golang.org/grpc"
)

var (
	ErrInvalidGRPCTLSConfig = errors.New("'OPENFGA_GRPC_TLS_CERT_PATH' and 'OPENFGA_GRPC_TLS_KEY_PATH' env variables must be set")
	ErrInvalidHTTPTLSConfig = errors.New("'OPENFGA_HTTP_GATEWAY_TLS_CERT_PATH' and 'OPENFGA_HTTP_GATEWAY_TLS_KEY_PATH' env variables must be set")
)

type Config struct {
	// If you change any of these settings, please update the documentation at https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx
	DatastoreEngine               string `default:"memory" split_words:"true"`
	DatastoreConnectionURI        string `split_words:"true"`
	DatastoreMaxCacheSize         int    `default:"100000" split_words:"true"`
	ServiceName                   string `default:"openfga" split_words:"true"`
	HTTPAddr                      string `default:":8080" split_words:"true"`
	GRPCAddr                      string `default:":8081" split_words:"true"`
	MaxTuplesPerWrite             int    `default:"100" split_words:"true"`
	MaxTypesPerAuthorizationModel int    `default:"100" split_words:"true"`
	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int `default:"0" split_words:"true" `
	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32 `default:"25" split_words:"true"`
	// RequestTimeout is a limit on the time a request may take. If the value is 0, then there is no timeout.
	RequestTimeout time.Duration `default:"0s" split_words:"true"`

	GRPCTLSEnabled  bool   `default:"false" envconfig:"OPENFGA_GRPC_TLS_ENABLED"`
	GRPCTLSCertPath string `envconfig:"OPENFGA_GRPC_TLS_CERT_PATH"`
	GRPCTLSKeyPath  string `envconfig:"OPENFGA_GRPC_TLS_KEY_PATH"`

	HTTPEnabled     bool   `default:"true" envconfig:"OPENFGA_HTTP_ENABLED"`
	HTTPTLSEnabled  bool   `default:"false" envconfig:"OPENFGA_HTTP_TLS_ENABLED"`
	HTTPTLSCertPath string `envconfig:"OPENFGA_HTTP_TLS_CERT_PATH"`
	HTTPTLSKeyPath  string `envconfig:"OPENFGA_HTTP_TLS_KEY_PATH"`

	// Authentication. Possible options: none,preshared,oidc
	AuthMethod string `default:"none" split_words:"true"`

	// Shared key authentication
	AuthPresharedKeys []string `default:"" split_words:"true"`

	// OIDC authentication
	AuthOIDCIssuer   string `default:"" split_words:"true"`
	AuthOIDCAudience string `default:"" split_words:"true"`

	// Logging. Possible options: text,json
	LogFormat string `default:"text" split_words:"true"`

	PlaygroundEnabled bool `default:"true" split_words:"true"`
	PlaygroundPort    int  `default:"3000" split_words:"true"`

	// CORS configuration
	CORSAllowedOrigins []string `default:"*" split_words:"true"`
	CORSAllowedHeaders []string `default:"*" split_words:"true"`
}

func GetServiceConfig() Config {
	var config Config

	if err := envconfig.Process("OPENFGA", &config); err != nil {
		log.Fatalf("failed to process server config: %v", err)
	}
	return config
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

func BuildService(config Config, logger logger.Logger) (*service, error) {
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewTokenEncoder(encrypter.NewNoopEncrypter(), encoder.NewBase64Encoder())

	var datastore storage.OpenFGADatastore
	var err error
	switch config.DatastoreEngine {
	case "memory":
		datastore = memory.New(tracer, config.MaxTuplesPerWrite, config.MaxTypesPerAuthorizationModel)
	case "postgres":
		opts := []postgres.PostgresOption{
			postgres.WithLogger(logger),
			postgres.WithTracer(tracer),
		}

		datastore, err = postgres.NewPostgresDatastore(config.DatastoreConnectionURI, opts...)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize postgres datastore: %v", err)
		}
	default:
		return nil, fmt.Errorf("storage engine '%s' is unsupported", config.DatastoreEngine)
	}

	logger.Info(fmt.Sprintf("using '%v' storage engine", config.DatastoreEngine))

	var grpcTLSConfig *server.TLSConfig
	if config.GRPCTLSEnabled {
		if config.GRPCTLSCertPath == "" || config.GRPCTLSKeyPath == "" {
			return nil, ErrInvalidGRPCTLSConfig
		}
		grpcTLSConfig = &server.TLSConfig{
			CertPath: config.GRPCTLSCertPath,
			KeyPath:  config.GRPCTLSKeyPath,
		}
		logger.Info("grpc TLS is enabled, serving grpc connections using the provided certificate")
	} else {
		logger.Warn("grpc TLS is disabled, serving grpc connections using insecure plaintext")
	}

	var httpTLSConfig *server.TLSConfig
	if config.HTTPTLSEnabled {
		if config.HTTPTLSCertPath == "" || config.HTTPTLSKeyPath == "" {
			return nil, ErrInvalidHTTPTLSConfig
		}
		httpTLSConfig = &server.TLSConfig{
			CertPath: config.HTTPTLSCertPath,
			KeyPath:  config.HTTPTLSKeyPath,
		}
		logger.Info("HTTP TLS is enabled, serving HTTP connections using the provided certificate")
	} else {
		logger.Warn("HTTP TLS is disabled, serving HTTP connections using insecure plaintext")
	}

	var authenticator authn.Authenticator
	switch config.AuthMethod {
	case "none":
		logger.Warn("using 'none' authentication")
		authenticator = authn.NoopAuthenticator{}
	case "preshared":
		logger.Info("using 'preshared' authentication")
		authenticator, err = presharedkey.NewPresharedKeyAuthenticator(config.AuthPresharedKeys)
	case "oidc":
		logger.Info("using 'oidc' authentication")
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.AuthOIDCIssuer, config.AuthOIDCAudience)
	default:
		return nil, fmt.Errorf("unsupported authenticator type: %v", config.AuthMethod)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authenticator: %v", err)
	}

	interceptors := []grpc.UnaryServerInterceptor{
		grpc_auth.UnaryServerInterceptor(middleware.AuthFunc(authenticator)),
	}

	openFgaServer, err := server.New(&server.Dependencies{
		Datastore:    caching.NewCachedOpenFGADatastore(datastore, config.DatastoreMaxCacheSize),
		Tracer:       tracer,
		Logger:       logger,
		Meter:        meter,
		TokenEncoder: tokenEncoder,
	}, &server.Config{
		ServiceName: config.ServiceName,
		GRPCServer: server.GRPCServerConfig{
			Addr:      config.GRPCAddr,
			TLSConfig: grpcTLSConfig,
		},
		HTTPServer: server.HTTPServerConfig{
			Enabled:            config.HTTPEnabled,
			Addr:               config.HTTPAddr,
			TLSConfig:          httpTLSConfig,
			CORSAllowedOrigins: config.CORSAllowedOrigins,
			CORSAllowedHeaders: config.CORSAllowedHeaders,
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
