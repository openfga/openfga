package main

import (
	"context"
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
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	"github.com/openfga/openfga/storage/postgres"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

type svcConfig struct {
	// Optional configuration
	DatastoreEngine               string `default:"memory" split_words:"true" required:"true"`
	DatastoreConnectionURI        string `split_words:"true"`
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

	var config svcConfig
	if err := envconfig.Process("OPENFGA", &config); err != nil {
		logger.Fatal("failed to process server config", zap.Error(err))
	}

	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewBase64Encoder()

	var datastore storage.OpenFGADatastore
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
			logger.Fatal("failed to initialize postgres datastore", zap.Error(err))
		}
	default:
		logger.Fatal(fmt.Sprintf("storage engine '%s' is unsupported", config.DatastoreEngine))
	}

	openFgaServer, err := server.New(&server.Dependencies{
		AuthorizationModelBackend: datastore,
		TypeDefinitionReadBackend: datastore,
		TupleBackend:              datastore,
		ChangelogBackend:          datastore,
		AssertionsBackend:         datastore,
		StoresBackend:             datastore,
		Tracer:                    tracer,
		Logger:                    logger,
		Meter:                     meter,
		TokenEncoder:              tokenEncoder,
	}, &server.Config{
		ServiceName:            config.ServiceName,
		RPCPort:                config.RPCPort,
		HTTPPort:               config.HTTPPort,
		ResolveNodeLimit:       config.ResolveNodeLimit,
		ChangelogHorizonOffset: config.ChangelogHorizonOffset,
		UnaryInterceptors:      nil,
		MuxOptions:             nil,
		RequestTimeout:         config.RequestTimeout,
	})
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
		return openFgaServer.Run(ctx)
	})

	if err := g.Wait(); err != nil {
		logger.Error("failed to run openfga server", zap.Error(err))
	}

	if err := openFgaServer.Close(context.Background()); err != nil {
		logger.Error("failed to gracefully shutdown openfga server", zap.Error(err))
	}

	if err := datastore.Close(context.Background()); err != nil {
		logger.Error("failed to gracefully shutdown openfga datastore", zap.Error(err))
	}

	logger.Info("Server exiting. Goodbye 👋")
}
