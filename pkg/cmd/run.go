package cmd

import (
	"context"
	"fmt"
	"html/template"
	"log"
	"net"
	"net/http"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/cmd/service"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func NewRunCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the OpenFGA server",
		Long:  "Run the OpenFGA server.",
		Run:   run,
	}
}

func run(_ *cobra.Command, _ []string) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	config, err := service.GetServiceConfig()
	if err != nil {
		log.Fatal(err)
	}

	logger, err := buildLogger(config.Log.Format)
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

	var playground *http.Server
	if config.Playground.Enabled {
		if !config.HTTP.Enabled {
			logger.Fatal("the HTTP server must be enabled to run the OpenFGA Playground")
		}

		playgroundPort := config.Playground.Port
		playgroundAddr := fmt.Sprintf(":%d", playgroundPort)

		logger.Info(fmt.Sprintf("🛝 starting openfga playground on http://localhost:%d/playground", playgroundPort))

		tmpl, err := template.ParseFiles("./static/playground/index.html")
		if err != nil {
			logger.Fatal("failed to parse Playground index.html as Go template", zap.Error(err))
		}

		fileServer := http.FileServer(http.Dir("./static"))

		policy := backoff.NewExponentialBackOff()
		policy.MaxElapsedTime = 3 * time.Second

		var conn net.Conn
		err = backoff.Retry(
			func() error {
				conn, err = net.Dial("tcp", config.HTTP.Addr)
				return err
			},
			policy,
		)
		if err != nil {
			logger.Fatal("failed to establish Playground connection to HTTP server", zap.Error(err))
		}

		mux := http.NewServeMux()
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if strings.HasPrefix(r.URL.Path, "/playground") {
				if r.URL.Path == "/playground" || r.URL.Path == "/playground/index.html" {
					err = tmpl.Execute(w, struct {
						HTTPServerURL string
					}{
						HTTPServerURL: conn.RemoteAddr().String(),
					})
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
						logger.Error("failed to execute/render the Playground web template", zap.Error(err))
					}

					return
				}

				fileServer.ServeHTTP(w, r)
				return
			}

			http.NotFound(w, r)
		}))

		playground = &http.Server{Addr: playgroundAddr, Handler: mux}

		g.Go(func() error {

			var errCh chan (error)

			go func() {
				err = playground.ListenAndServe()
				if err != http.ErrServerClosed {
					errCh <- err
				}
			}()

			select {
			case err := <-errCh:
				logger.Fatal("failed to start or shutdown playground server", zap.Error(err))
			case <-ctx.Done():
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		logger.Error("failed to run openfga server", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if playground != nil {
		if err := playground.Shutdown(ctx); err != nil {
			logger.Error("failed to gracefully shutdown playground server", zap.Error(err))
		}
	}

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
