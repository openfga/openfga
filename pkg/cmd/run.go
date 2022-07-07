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
	cmdutil "github.com/openfga/openfga/pkg/cmd/util"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func NewRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the OpenFGA server",
		Long:  "Run the OpenFGA server.",
		Run:   run,
		Args:  cobra.NoArgs,
	}

	bindFlags(cmd)

	return cmd
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

// bindFlags binds the cobra cmd flags to the equivalent config value being managed
// by viper. This bridges the config between cobra flags and viper flags.
func bindFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("grpc-enabled", true, "enable/disable the OpenFGA grpc server")
	cmdutil.MustBindPFlag("grpc.enabled", cmd.Flags().Lookup("grpc-enabled"))

	cmd.Flags().String("grpc-addr", ":8081", "the host:port address to serve the grpc server on")
	cmdutil.MustBindPFlag("grpc.addr", cmd.Flags().Lookup("grpc-addr"))

	cmd.Flags().Bool("grpc-tls-enabled", false, "enable/disable transport layer security (TLS)")
	cmdutil.MustBindPFlag("grpc.tls.enabled", cmd.Flags().Lookup("grpc-tls-enabled"))

	cmd.Flags().String("grpc-tls-cert", "", "the (absolute) file path of the certificate to use for the TLS connection")
	cmdutil.MustBindPFlag("grpc.tls.cert", cmd.Flags().Lookup("grpc-tls-cert"))

	cmd.Flags().String("grpc-tls-key", "", "the (absolute) file path of the TLS key that should be used for the TLS connection")
	cmdutil.MustBindPFlag("grpc.tls.key", cmd.Flags().Lookup("grpc-tls-key"))

	cmd.MarkFlagsRequiredTogether("grpc-tls-enabled", "grpc-tls-cert", "grpc-tls-key")

	cmd.Flags().Bool("http-enabled", true, "enable/disable the OpenFGA HTTP server")
	cmdutil.MustBindPFlag("http.enabled", cmd.Flags().Lookup("http-enabled"))

	cmd.Flags().String("http-addr", ":8080", "the host:port address to serve the HTTP server on")
	cmdutil.MustBindPFlag("http.addr", cmd.Flags().Lookup("http-addr"))

	cmd.Flags().Bool("http-tls-enabled", false, "enable/disable transport layer security (TLS)")
	cmdutil.MustBindPFlag("http.tls.enabled", cmd.Flags().Lookup("http-tls-enabled"))

	cmd.Flags().String("http-tls-cert", "", "the (absolute) file path of the certificate to use for the TLS connection")
	cmdutil.MustBindPFlag("http.tls.cert", cmd.Flags().Lookup("http-tls-cert"))

	cmd.Flags().String("http-tls-key", "", "the (absolute) file path of the TLS key that should be used for the TLS connection")
	cmdutil.MustBindPFlag("http.tls.key", cmd.Flags().Lookup("http-tls-key"))

	cmd.MarkFlagsRequiredTogether("http-tls-enabled", "http-tls-cert", "http-tls-key")

	cmd.Flags().String("authn-method", "none", "the authentication method to use")
	cmdutil.MustBindPFlag("authn.method", cmd.Flags().Lookup("authn-method"))
	cmd.Flags().StringSlice("authn-preshared-keys", nil, "one or more preshared keys to use for authentication")
	cmdutil.MustBindPFlag("authn.preshared.keys", cmd.Flags().Lookup("authn-preshared-keys"))
	cmd.Flags().String("authn-oidc-audience", "", "the OIDC audience of the tokens being signed by the authorization server")
	cmdutil.MustBindPFlag("authn.oidc.audience", cmd.Flags().Lookup("authn-oidc-audience"))
	cmd.Flags().String("authn-oidc-issuer", "", "the OIDC issuer (authorization server) signing the tokens")
	cmdutil.MustBindPFlag("authn.oidc.issuer", cmd.Flags().Lookup("authn-oidc-issuer"))

	cmd.Flags().String("database-engine", "memory", "the database engine that will be used for persistence")
	cmdutil.MustBindPFlag("database.engine", cmd.Flags().Lookup("database-engine"))
	cmd.Flags().String("database-uri", "", "the connection uri to use to connect to the database (for any engine other than 'memory')")
	cmdutil.MustBindPFlag("database.uri", cmd.Flags().Lookup("database-uri"))

	cmd.Flags().Bool("playground-enabled", false, "enable/disable the OpenFGA Playground")
	cmdutil.MustBindPFlag("playground.enabled", cmd.Flags().Lookup("playground-enabled"))
	cmd.Flags().Int("playground-port", 3000, "the port to serve the local OpenFGA Playground on")
	cmdutil.MustBindPFlag("playground.port", cmd.Flags().Lookup("playground-port"))

	cmd.Flags().String("log-format", "text", "the log format to output logs in")
	cmdutil.MustBindPFlag("log.format", cmd.Flags().Lookup("log-format"))
}
