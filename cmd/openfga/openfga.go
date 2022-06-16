package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/kelseyhightower/envconfig"
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
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"

	errInvalidGRPCTLSConfig = errors.New("'OPENFGA_GRPC_TLS_CERT_PATH' and 'OPENFGA_GRPC_TLS_KEY_PATH' env variables must be set")
	errInvalidHTTPTLSConfig = errors.New("'OPENFGA_HTTP_GATEWAY_TLS_CERT_PATH' and 'OPENFGA_HTTP_GATEWAY_TLS_KEY_PATH' env variables must be set")
)

type service struct {
	server        *server.Server
	datastore     storage.OpenFGADatastore
	authenticator authentication.Authenticator
}

func (s *service) Close(ctx context.Context) error {
	s.authenticator.Close()
	s.server.Close()

	return s.datastore.Close(ctx)
}

type svcConfig struct {
	// If you change any of these settings, please update the documentation at https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx
	DatastoreEngine               string `default:"memory" split_words:"true"`
	DatastoreConnectionURI        string `split_words:"true"`
	DatastoreMaxCacheSize         int    `default:"100000" split_words:"true"`
	ServiceName                   string `default:"openfga" split_words:"true"`
	HTTPPort                      int    `default:"8080" split_words:"true"`
	RPCPort                       int    `default:"8081" split_words:"true"`
	MaxTuplesPerWrite             int    `default:"100" split_words:"true"`
	MaxTypesPerAuthorizationModel int    `default:"100" split_words:"true"`
	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int `default:"0" split_words:"true" `
	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32 `default:"25" split_words:"true"`
	// RequestTimeout is a limit on the time a request may take. If the value is 0, then there is no timeout.
	RequestTimeout time.Duration `default:"0s" split_words:"true"`

	GRPCTLSEnabled  bool   `default:"false" envconfig:"GRPC_TLS_ENABLED"`
	GRPCTLSCertPath string `envconfig:"GRPC_TLS_CERT_PATH"`
	GRPCTLSKeyPath  string `envconfig:"GRPC_TLS_KEY_PATH"`

	HTTPTLSEnabled  bool   `default:"false" envconfig:"HTTP_TLS_ENABLED"`
	HTTPTLSCertPath string `envconfig:"HTTP_TLS_CERT_PATH"`
	HTTPTLSKeyPath  string `envconfig:"HTTP_TLS_KEY_PATH"`

	// Authentication. Possible options: none,preshared,oidc
	AuthMethod string `default:"none" split_words:"true"`

	// Shared key authentication
	AuthPresharedKeys []string `default:"" split_words:"true"`

	// OIDC authentication
	AuthOIDCIssuer   string `default:"" split_words:"true"`
	AuthOIDCAudience string `default:"" split_words:"true"`

	// Logging. Possible options: text,json
	LogFormat string `default:"text" split_words:"true"`
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger, err := buildLogger()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	service, err := buildService(logger)
	if err != nil {
		logger.Fatal("failed to initialize openfga server", zap.Error(err))
	}

	g, ctx := errgroup.WithContext(ctx)

	logger.Info(
		"🚀 starting openfga service...",
		zap.String("version", version),
		zap.String("date", date),
		zap.String("commit", commit),
		zap.String("go-version", runtime.Version()),
	)

	g.Go(func() error {
		return service.server.Run(ctx)
	})

	if err := g.Wait(); err != nil {
		logger.Error("failed to run openfga server", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := service.Close(ctx); err != nil {
		logger.Error("failed to gracefully shutdown the service", zap.Error(err))
	}

	logger.Info("Server exiting. Goodbye 👋")
}

func getServiceConfig() svcConfig {
	var config svcConfig

	if err := envconfig.Process("OPENFGA", &config); err != nil {
		log.Fatalf("failed to process server config: %v", err)
	}
	return config
}

func buildService(logger logger.Logger) (*service, error) {
	var err error
	var datastore storage.OpenFGADatastore
	config := getServiceConfig()

	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewBase64Encoder()

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
			return nil, errInvalidGRPCTLSConfig
		}
		grpcTLSConfig = &server.TLSConfig{
			CertPath: config.GRPCTLSCertPath,
			KeyPath:  config.GRPCTLSKeyPath,
		}
		logger.Info("GRPC TLS enabled, serving connections using the provided certificate")
	} else {
		logger.Warn("GRPC TLS is disabled, falling back to insecure plaintext")
	}

	var httpTLSConfig *server.TLSConfig
	if config.HTTPTLSEnabled {
		if config.HTTPTLSCertPath == "" || config.HTTPTLSKeyPath == "" {
			return nil, errInvalidHTTPTLSConfig
		}
		httpTLSConfig = &server.TLSConfig{
			CertPath: config.HTTPTLSCertPath,
			KeyPath:  config.HTTPTLSKeyPath,
		}
		logger.Info("HTTP TLS enabled, serving connections using the provided certificate")
	} else {
		logger.Warn("HTTP TLS is disabled, falling back to insecure plaintext")
	}

	var authenticator authentication.Authenticator
	switch config.AuthMethod {
	case "none":
		authenticator = authentication.NoopAuthenticator{}
	case "preshared":
		authenticator, err = presharedkey.NewPresharedKeyAuthenticator(config.AuthPresharedKeys)
	case "oidc":
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.AuthOIDCIssuer, config.AuthOIDCAudience)
	default:
		return nil, fmt.Errorf("unsupported authenticator type: %v", config.AuthMethod)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to initialize authenticator: %v", err)
	}

	logger.Info(fmt.Sprintf("using '%v' authentication", config.AuthMethod))

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
		ServiceName: config.ServiceName,
		GRPCServer: server.GRPCServerConfig{
			Addr:      config.RPCPort,
			TLSConfig: grpcTLSConfig,
		},
		HTTPServer: server.HTTPServerConfig{
			Addr:      config.HTTPPort,
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

func buildLogger() (logger.Logger, error) {
	openfgaLogger, err := logger.NewTextLogger()
	if err != nil {
		return nil, err
	}

	config := getServiceConfig()
	if config.LogFormat == "json" {
		openfgaLogger, err = logger.NewJSONLogger()
		if err != nil {
			return nil, err
		}
		openfgaLogger.With(
			zap.String("build.version", version),
			zap.String("build.commit", commit),
		)
	}

	return openfgaLogger, err
}
