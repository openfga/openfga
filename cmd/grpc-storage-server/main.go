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
	port := flag.Int("port", 50051, "port to bind the gRPC storage server (ignored if --socket is set)")
	host := flag.String("host", "0.0.0.0", "host to bind the gRPC storage server (ignored if --socket is set)")
	socketPath := flag.String("socket", "", "Unix domain socket path to bind the gRPC storage server (e.g., /var/run/openfga.sock)")
	datastoreURI := flag.String("datastore-uri", "", "PostgreSQL connection URI (required)")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")

	flag.Parse()

	if *datastoreURI == "" {
		fmt.Fprintf(os.Stderr, "Error: --datastore-uri is required\n")
		flag.Usage()
		os.Exit(1)
	}

	log := logger.MustNewLogger("text", *logLevel, "ISO8601")

	if err := startServer(*socketPath, *host, *port, *datastoreURI, log); err != nil {
		log.Error(fmt.Sprintf("server error: %v", err))
		os.Exit(1)
	}
}

func startServer(socketPath, host string, port int, datastoreURI string, log logger.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM)
	defer stop()

	var listenConfig ListenConfig
	if socketPath != "" {
		listenConfig = ListenConfig{
			Network: "unix",
			Address: socketPath,
		}
	} else {
		listenConfig = ListenConfig{
			Network: "tcp",
			Address: fmt.Sprintf("%s:%d", host, port),
		}
	}

	return run(ctx, listenConfig, datastoreURI, log)
}

// ListenConfig specifies how the server should listen for connections.
type ListenConfig struct {
	// Network is the network type: "tcp" or "unix".
	Network string
	// Address is the address to listen on.
	// For TCP: "host:port" format (e.g., "0.0.0.0:50051").
	// For Unix: socket file path (e.g., "/var/run/openfga.sock").
	Address string
}

func run(ctx context.Context, listenConfig ListenConfig, datastoreURI string, log logger.Logger) error {
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
	defer datastore.Close()

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

	// For Unix sockets, remove existing socket file if present
	if listenConfig.Network == "unix" {
		if err := os.Remove(listenConfig.Address); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove existing socket file %s: %w", listenConfig.Address, err)
		}
	}

	// Start listening
	listener, err := net.Listen(listenConfig.Network, listenConfig.Address)
	if err != nil {
		return fmt.Errorf("failed to listen on %s %s: %w", listenConfig.Network, listenConfig.Address, err)
	}

	// Log the appropriate message based on network type
	if listenConfig.Network == "unix" {
		log.Info("starting gRPC storage server on unix socket " + listenConfig.Address)
	} else {
		log.Info("starting gRPC storage server on " + listenConfig.Address)
	}

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
