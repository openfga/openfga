package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/go-errors/errors"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/kelseyhightower/envconfig"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/server"
	"github.com/openfga/openfga/storage"
	"github.com/openfga/openfga/storage/memory"
	"github.com/openfga/openfga/storage/postgres"
	"golang.org/x/sync/errgroup"
)

type svcConfig struct {
	// Optional configuration
	DatastoreEngine               string `default:"memory" split_words:"true" required:"true"`
	DatastoreConnectionURI        string `split_words:"true"`
	ServiceName                   string `default:"openfga" split_words:"true"`
	HttpPort                      int    `default:"8080" split_words:"true"`
	RpcPort                       int    `default:"8081" split_words:"true"`
	MaxTuplesPerWrite             int    `default:"100" split_words:"true"`
	MaxTypesPerAuthorizationModel int    `default:"100" split_words:"true"`
	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int `default:"0" split_words:"true" `
	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32 `default:"25" split_words:"true"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := runServer(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("Server exiting")
}

func runServer(ctx context.Context) error {
	var config svcConfig
	if err := envconfig.Process("OPENFGA", &config); err != nil {
		return err
	}
	tracer := telemetry.NewNoopTracer()
	meter := telemetry.NewNoopMeter()
	tokenEncoder := encoder.NewBase64Encoder()
	zapLogger, err := logger.NewZapLogger()
	if err != nil {
		return err
	}

	var backend storage.OpenFGADatastore
	switch config.DatastoreEngine {
	case "memory":
		backend = memory.New(tracer, config.MaxTuplesPerWrite, config.MaxTypesPerAuthorizationModel)
	case "postgres":
		pool, err := pgxpool.Connect(context.Background(), config.DatastoreConnectionURI)
		if err != nil {
			return err
		}
		backend = postgres.New(pool, tracer, zapLogger)
	default:
		return errors.Errorf("Unsupported backend type: %s", config.DatastoreEngine)
	}
	openFgaServer, err := server.New(&server.Dependencies{
		AuthorizationModelBackend: backend,
		TypeDefinitionReadBackend: backend,
		TupleBackend:              backend,
		ChangelogBackend:          backend,
		AssertionsBackend:         backend,
		StoresBackend:             backend,
		Tracer:                    tracer,
		Logger:                    zapLogger,
		Meter:                     meter,
		TokenEncoder:              tokenEncoder,
	}, &server.Config{
		ServiceName:            config.ServiceName,
		HttpPort:               config.HttpPort,
		RpcPort:                config.RpcPort,
		ResolveNodeLimit:       config.ResolveNodeLimit,
		ChangelogHorizonOffset: config.ChangelogHorizonOffset,
		UnaryInterceptors:      nil,
		MuxOptions:             nil,
	})
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return openFgaServer.Run(ctx)
	})

	return g.Wait()
}
