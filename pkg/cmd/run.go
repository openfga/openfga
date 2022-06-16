package cmd

import (
	"context"
	"log"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/openfga/openfga/pkg/cmd/service"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func NewRunCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "run the OpenFGA server",
		Long:  "run the OpenFGA server",
		Run:   run,
	}
}

func run(_ *cobra.Command, _ []string) {
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

	service, err := service.BuildService(logger)
	if err != nil {
		logger.Fatal("failed to initialize openfga server", zap.Error(err))
	}

	logger.Info(
		"🚀 starting openfga service...",
		zap.String("version", version),
		zap.String("date", date),
		zap.String("commit", commit),
		zap.String("go-version", runtime.Version()),
	)

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return service.Run(ctx)
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
