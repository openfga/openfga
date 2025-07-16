package postgres

import (
	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/cenkalti/backoff/v4"
	"github.com/pressly/goose/v3"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage"
)

// PostgresMigrationProvider implements MigrationProvider for PostgreSQL.
type PostgresMigrationProvider struct{}

// NewPostgresMigrationProvider creates a new PostgreSQL migration provider.
func NewPostgresMigrationProvider() *PostgresMigrationProvider {
	return &PostgresMigrationProvider{}
}

// GetSupportedEngine returns the database engine this provider supports.
func (p *PostgresMigrationProvider) GetSupportedEngine() string {
	return "postgres"
}

// RunMigrations executes PostgreSQL database migrations.
func (p *PostgresMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	uri, err := p.prepareURI(config)
	if err != nil {
		return err
	}

	db, err := goose.OpenDBWithDriver("pgx", uri)
	if err != nil {
		return fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer db.Close()

	// Test connection with backoff
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = config.Timeout
	err = backoff.Retry(func() error {
		return db.PingContext(ctx)
	}, policy)
	if err != nil {
		return fmt.Errorf("failed to initialize postgres connection: %w", err)
	}

	// Create provider instance with PostgreSQL dialect and embedded migrations
	provider, err := goose.NewProvider(goose.DialectPostgres, db, assets.EmbedMigrations)
	if err != nil {
		return fmt.Errorf("failed to create goose provider: %w", err)
	}

	return p.executeMigrations(ctx, provider, config)
}

// GetCurrentVersion returns the current migration version.
func (p *PostgresMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
	uri, err := p.prepareURI(config)
	if err != nil {
		return 0, err
	}

	db, err := goose.OpenDBWithDriver("pgx", uri)
	if err != nil {
		return 0, fmt.Errorf("failed to open postgres connection: %w", err)
	}
	defer db.Close()

	// Create provider instance with PostgreSQL dialect and embedded migrations
	provider, err := goose.NewProvider(goose.DialectPostgres, db, assets.EmbedMigrations)
	if err != nil {
		return 0, fmt.Errorf("failed to create goose provider: %w", err)
	}

	return provider.GetDBVersion(ctx)
}

// prepareURI processes the database URI with username/password overrides.
func (p *PostgresMigrationProvider) prepareURI(config storage.MigrationConfig) (string, error) {
	dbURI, err := url.Parse(config.URI)
	if err != nil {
		return "", fmt.Errorf("invalid postgres database uri: %v", err)
	}

	var username, password string
	if config.Username != "" {
		username = config.Username
	} else if dbURI.User != nil {
		username = dbURI.User.Username()
	}

	if config.Password != "" {
		password = config.Password
	} else if dbURI.User != nil {
		password, _ = dbURI.User.Password()
	}

	dbURI.User = url.UserPassword(username, password)
	return dbURI.String(), nil
}

// executeMigrations runs the actual migration commands.
func (p *PostgresMigrationProvider) executeMigrations(ctx context.Context, provider *goose.Provider, config storage.MigrationConfig) error {
	currentVersion, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get postgres db version: %w", err)
	}

	log.Printf("postgres current version %d", currentVersion)

	if config.TargetVersion == 0 {
		log.Println("running all postgres migrations")
		_, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("failed to run postgres migrations: %w", err)
		}
		log.Println("postgres migration done")
		return nil
	}

	log.Printf("migrating postgres to %d", config.TargetVersion)
	targetInt64Version := int64(config.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		_, err := provider.DownTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run postgres migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		_, err := provider.UpTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run postgres migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		log.Println("postgres nothing to do")
		return nil
	}

	log.Println("postgres migration done")
	return nil
}
