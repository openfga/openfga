package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"

	"github.com/cenkalti/backoff/v4"
	"github.com/pressly/goose/v3"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage"
)

// PostgresMigrationProvider implements MigrationProvider for PostgreSQL
type PostgresMigrationProvider struct{}

// NewPostgresMigrationProvider creates a new PostgreSQL migration provider
func NewPostgresMigrationProvider() *PostgresMigrationProvider {
	return &PostgresMigrationProvider{}
}

// GetSupportedEngine returns the database engine this provider supports
func (p *PostgresMigrationProvider) GetSupportedEngine() string {
	return "postgres"
}

// RunMigrations executes PostgreSQL database migrations
func (p *PostgresMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	goose.SetLogger(goose.NopLogger())
	goose.SetVerbose(config.Verbose)

	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("failed to set postgres dialect: %w", err)
	}

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

	goose.SetBaseFS(assets.EmbedMigrations)

	return p.executeMigrations(db, config)
}

// GetCurrentVersion returns the current migration version
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

	goose.SetBaseFS(assets.EmbedMigrations)
	return goose.GetDBVersion(db)
}

// prepareURI processes the database URI with username/password overrides
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

// executeMigrations runs the actual migration commands
func (p *PostgresMigrationProvider) executeMigrations(db *sql.DB, config storage.MigrationConfig) error {
	migrationsPath := assets.PostgresMigrationDir

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get postgres db version: %w", err)
	}

	log.Printf("postgres current version %d", currentVersion)

	if config.TargetVersion == 0 {
		log.Println("running all postgres migrations")
		if err := goose.Up(db, migrationsPath); err != nil {
			return fmt.Errorf("failed to run postgres migrations: %w", err)
		}
		log.Println("postgres migration done")
		return nil
	}

	log.Printf("migrating postgres to %d", config.TargetVersion)
	targetInt64Version := int64(config.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		if err := goose.DownTo(db, migrationsPath, targetInt64Version); err != nil {
			return fmt.Errorf("failed to run postgres migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		if err := goose.UpTo(db, migrationsPath, targetInt64Version); err != nil {
			return fmt.Errorf("failed to run postgres migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		log.Println("postgres nothing to do")
		return nil
	}

	log.Println("postgres migration done")
	return nil
}
