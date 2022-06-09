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

	errFailedToSetTLSVariables = errors.New("please set TLS_CERT_PATH and TLS_KEY_PATH to enable TLS")
)

type service struct {
	openFgaServer *server.Server
	datastore     storage.OpenFGADatastore
	authenticator authentication.Authenticator
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

	EnableTLS   bool   `default:"false" split_words:"true"`
	TLSCertPath string `split_words:"true"`
	TLSKeyPath  string `split_words:"true"`

	// Authentication. Possible options: none,preshared,oidc
	AuthMethod string `default:"none" split_words:"true"`

	// Shared key authentication
	AuthPresharedKeys []string `default:"" split_words:"true"`

	// OIDC authentication
	AuthOIDCIssuer   string `default:"" split_words:"true"`
	AuthOIDCAudience string `default:"" split_words:"true"`
}

func main() {

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger, err := logger.NewZapLogger()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	logger.With(
		zap.String("build.version", version),
		zap.String("build.commit", commit),
	)

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
		return service.openFgaServer.Run(ctx)
	})

	if err := g.Wait(); err != nil {
		logger.Error("failed to run openfga server", zap.Error(err))
	}

	defer service.authenticator.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := service.openFgaServer.Close(); err != nil {
		logger.Error("failed to gracefully shutdown openfga server", zap.Error(err))
	}

	if err := service.datastore.Close(ctx); err != nil {
		logger.Error("failed to gracefully shutdown openfga datastore", zap.Error(err))
	}

	logger.Info("Server exiting. Goodbye 👋")
}

func buildService(logger logger.Logger) (*service, error) {
	var config svcConfig
	var err error
	var datastore storage.OpenFGADatastore

	if err := envconfig.Process("OPENFGA", &config); err != nil {
		return nil, fmt.Errorf("failed to process server config: %v", err)
	}

	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewBase64Encoder()

	switch config.DatastoreEngine {
	case "memory":
		logger.Info("using 'memory' storage engine")

		datastore = memory.New(tracer, config.MaxTuplesPerWrite, config.MaxTypesPerAuthorizationModel)
	case "postgres":
		logger.Info("using 'postgres' storage engine")

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

	var tlsConfig *server.TLSConfig
	if config.EnableTLS {
		if config.TLSCertPath == "" || config.TLSKeyPath == "" {
			return nil, errFailedToSetTLSVariables
		}
		tlsConfig = &server.TLSConfig{
			CertPath: config.TLSCertPath,
			KeyPath:  config.TLSKeyPath,
		}
		logger.Info("will serve TLS")
	} else {
		logger.Info("will serve plaintext")
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
		ServiceName:            config.ServiceName,
		RPCPort:                config.RPCPort,
		HTTPPort:               config.HTTPPort,
		TLSConfig:              tlsConfig,
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
		openFgaServer: openFgaServer,
		datastore:     datastore,
		authenticator: authenticator,
	}, nil
}
