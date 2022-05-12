package main

import (
	"context"
	"golang.org/x/sync/errgroup"
	"log"
	"os/signal"
	"syscall"

	"github.com/openfga/openfga/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := runServer(ctx); err != nil {
		log.Fatal(err)
	}

	log.Println("Server exiting")
}

func runServer(ctx context.Context) error {
	//TODO right dependencies
	openFgaServer, err := server.New(&server.Dependencies{
		AuthorizationModelBackend: nil,
		TypeDefinitionReadBackend: nil,
		TupleBackend:              nil,
		ChangelogBackend:          nil,
		AssertionsBackend:         nil,
		StoresBackend:             nil,
		Tracer:                    nil,
		Meter:                     nil,
		Logger:                    nil,
		TokenEncoder:              nil,
	}, &server.Config{
		ServiceName:            "",
		RpcPort:                0,
		HttpPort:               0,
		ResolveNodeLimit:       0,
		ChangelogHorizonOffset: 0,
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
