package cmd

import (
	"context"
	"log"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/cmd/service"
	"github.com/openfga/openfga/pkg/logger"
	_ "github.com/pressly/goose"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
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

	config := service.GetServiceConfig()

	logger, err := buildLogger(config.LogFormat)
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}

	service, err := service.BuildService(config, logger)
	if err != nil {
		logger.Fatal("failed to initialize openfga server", zap.Error(err))
	}

	logger.Info(
		"🚀 starting openfga service...",
		zap.String("version", build.Version),
		zap.String("date", build.Date),
		zap.String("commit", build.Commit),
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

func buildLogger(logFormat string) (logger.Logger, error) {
	openfgaLogger, err := logger.NewTextLogger()
	if err != nil {
		return nil, err
	}

	if logFormat == "json" {
		openfgaLogger, err = logger.NewJSONLogger()
		if err != nil {
			return nil, err
		}
		openfgaLogger.With(
			zap.String("build.version", build.Version),
			zap.String("build.commit", build.Commit),
		)
	}

	return openfgaLogger, err
}
