package sqlite

import (
	"context"
	"fmt"
	"io/fs"
	"log"

	"github.com/cenkalti/backoff/v4"
	"github.com/pressly/goose/v3"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage"
)

// SQLiteMigrationProvider implements MigrationProvider for SQLite.
type SQLiteMigrationProvider struct{}

// NewSQLiteMigrationProvider creates a new SQLite migration provider.
func NewSQLiteMigrationProvider() *SQLiteMigrationProvider {
	return &SQLiteMigrationProvider{}
}

// GetSupportedEngine returns the database engine this provider supports.
func (s *SQLiteMigrationProvider) GetSupportedEngine() string {
	return "sqlite"
}

// RunMigrations executes SQLite database migrations.
func (s *SQLiteMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	uri, err := s.prepareURI(config)
	if err != nil {
		return err
	}

	db, err := goose.OpenDBWithDriver("sqlite", uri)
	if err != nil {
		return fmt.Errorf("failed to open sqlite connection: %w", err)
	}
	defer db.Close()

	// Test connection with backoff
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = config.Timeout
	err = backoff.Retry(func() error {
		return db.PingContext(ctx)
	}, policy)
	if err != nil {
		return fmt.Errorf("failed to initialize sqlite connection: %w", err)
	}

	// Create provider instance with SQLite dialect and embedded migrations
	migrationsFS, err := fs.Sub(assets.EmbedMigrations, assets.SqliteMigrationDir)
	if err != nil {
		return fmt.Errorf("failed to create sqlite migrations filesystem: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationsFS)
	if err != nil {
		return fmt.Errorf("failed to create goose provider: %w", err)
	}

	return s.executeMigrations(ctx, provider, config)
}

// GetCurrentVersion returns the current migration version.
func (s *SQLiteMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
	uri, err := s.prepareURI(config)
	if err != nil {
		return 0, err
	}

	db, err := goose.OpenDBWithDriver("sqlite", uri)
	if err != nil {
		return 0, fmt.Errorf("failed to open sqlite connection: %w", err)
	}
	defer db.Close()

	// Create provider instance with SQLite dialect and embedded migrations
	migrationsFS, err := fs.Sub(assets.EmbedMigrations, assets.SqliteMigrationDir)
	if err != nil {
		return 0, fmt.Errorf("failed to create sqlite migrations filesystem: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectSQLite3, db, migrationsFS)
	if err != nil {
		return 0, fmt.Errorf("failed to create goose provider: %w", err)
	}

	return provider.GetDBVersion(ctx)
}

// prepareURI processes the database URI.
func (s *SQLiteMigrationProvider) prepareURI(config storage.MigrationConfig) (string, error) {
	return PrepareDSN(config.URI)
}

// executeMigrations runs the actual migration commands.
func (s *SQLiteMigrationProvider) executeMigrations(ctx context.Context, provider *goose.Provider, config storage.MigrationConfig) error {
	currentVersion, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get sqlite db version: %w", err)
	}

	log.Printf("sqlite current version %d", currentVersion)

	if config.TargetVersion == 0 {
		log.Println("running all sqlite migrations")
		_, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("failed to run sqlite migrations: %w", err)
		}
		log.Println("sqlite migration done")
		return nil
	}

	log.Printf("migrating sqlite to %d", config.TargetVersion)
	targetInt64Version := int64(config.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		_, err := provider.DownTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run sqlite migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		_, err := provider.UpTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run sqlite migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		log.Println("sqlite nothing to do")
		return nil
	}

	log.Println("sqlite migration done")
	return nil
}
