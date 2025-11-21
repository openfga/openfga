// Package main provides a standalone gRPC storage server backed by PostgreSQL.
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	grpcstorage "github.com/openfga/openfga/pkg/storage/grpc"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
)

func main() {
	port := flag.Int("port", 50051, "port to bind the gRPC storage server")
	host := flag.String("host", "0.0.0.0", "host to bind the gRPC storage server")
	datastoreURI := flag.String("datastore-uri", "", "PostgreSQL connection URI (required)")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")

	flag.Parse()

	if *datastoreURI == "" {
		fmt.Fprintf(os.Stderr, "Error: --datastore-uri is required\n")
		flag.Usage()
		os.Exit(1)
	}

	log := logger.MustNewLogger("text", *logLevel, "ISO8601")

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer stop()

	grpcAddr := fmt.Sprintf("%s:%d", *host, *port)
	if err := run(ctx, grpcAddr, *datastoreURI, log); err != nil {
		log.Error(fmt.Sprintf("server error: %v", err))
	}
}

func run(ctx context.Context, grpcAddr, datastoreURI string, log logger.Logger) error {
	log.Info("initializing postgres datastore: " + datastoreURI)

	// Use sensible defaults for testing
	datastoreOptions := []sqlcommon.DatastoreOption{
		sqlcommon.WithLogger(log),
	}

	dsCfg := sqlcommon.NewConfig(datastoreOptions...)

	var datastore storage.OpenFGADatastore
	datastore, err := postgres.New(datastoreURI, dsCfg)
	if err != nil {
		return fmt.Errorf("failed to initialize postgres datastore: %w", err)
	}

	log.Info("datastore initialized successfully")

	// Check datastore readiness
	readiness, err := datastore.IsReady(ctx)
	if err != nil {
		return fmt.Errorf("datastore readiness check failed: %w", err)
	}
	if !readiness.IsReady {
		return fmt.Errorf("datastore is not ready: %s", readiness.Message)
	}
	log.Info("datastore is ready")

	// Create gRPC server
	grpcServer := grpc.NewServer()

	// Register the storage service
	storageServer := grpcstorage.NewServer(datastore)
	storagev1.RegisterStorageServiceServer(grpcServer, storageServer)

	// Enable reflection for easier debugging with grpcurl
	reflection.Register(grpcServer)

	// Start listening
	listener, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", grpcAddr, err)
	}

	log.Info("starting gRPC storage server on " + grpcAddr)

	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			errChan <- fmt.Errorf("gRPC server error: %w", err)
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-ctx.Done():
		log.Info("shutting down gRPC storage server...")
		grpcServer.GracefulStop()
		log.Info("server stopped")
		return nil
	case err := <-errChan:
		return err
	}
}
