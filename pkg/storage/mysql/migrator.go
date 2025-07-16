package mysql

import (
	"context"
	"fmt"
	"io/fs"
	"log"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/pressly/goose/v3"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage"
)

// MySQLMigrationProvider implements MigrationProvider for MySQL.
type MySQLMigrationProvider struct{}

// NewMySQLMigrationProvider creates a new MySQL migration provider.
func NewMySQLMigrationProvider() *MySQLMigrationProvider {
	return &MySQLMigrationProvider{}
}

// GetSupportedEngine returns the database engine this provider supports.
func (m *MySQLMigrationProvider) GetSupportedEngine() string {
	return "mysql"
}

// RunMigrations executes MySQL database migrations.
func (m *MySQLMigrationProvider) RunMigrations(ctx context.Context, config storage.MigrationConfig) error {
	uri, err := m.prepareURI(config)
	if err != nil {
		return err
	}

	db, err := goose.OpenDBWithDriver("mysql", uri)
	if err != nil {
		return fmt.Errorf("failed to open mysql connection: %w", err)
	}
	defer db.Close()

	// Test connection with backoff
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = config.Timeout
	err = backoff.Retry(func() error {
		return db.PingContext(ctx)
	}, policy)
	if err != nil {
		return fmt.Errorf("failed to initialize mysql connection: %w", err)
	}

	// Create provider instance with MySQL dialect and embedded migrations
	migrationsFS, err := fs.Sub(assets.EmbedMigrations, assets.MySQLMigrationDir)
	if err != nil {
		return fmt.Errorf("failed to create mysql migrations filesystem: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectMySQL, db, migrationsFS)
	if err != nil {
		return fmt.Errorf("failed to create goose provider: %w", err)
	}

	return m.executeMigrations(ctx, provider, config)
}

// GetCurrentVersion returns the current migration version.
func (m *MySQLMigrationProvider) GetCurrentVersion(ctx context.Context, config storage.MigrationConfig) (int64, error) {
	uri, err := m.prepareURI(config)
	if err != nil {
		return 0, err
	}

	db, err := goose.OpenDBWithDriver("mysql", uri)
	if err != nil {
		return 0, fmt.Errorf("failed to open mysql connection: %w", err)
	}
	defer db.Close()

	// Create provider instance with MySQL dialect and embedded migrations
	migrationsFS, err := fs.Sub(assets.EmbedMigrations, assets.MySQLMigrationDir)
	if err != nil {
		return 0, fmt.Errorf("failed to create mysql migrations filesystem: %w", err)
	}

	provider, err := goose.NewProvider(goose.DialectMySQL, db, migrationsFS)
	if err != nil {
		return 0, fmt.Errorf("failed to create goose provider: %w", err)
	}

	return provider.GetDBVersion(ctx)
}

// prepareURI processes the database URI with username/password overrides.
func (m *MySQLMigrationProvider) prepareURI(config storage.MigrationConfig) (string, error) {
	dsn, err := mysql.ParseDSN(config.URI)
	if err != nil {
		return "", fmt.Errorf("invalid mysql database uri: %v", err)
	}

	if config.Username != "" {
		dsn.User = config.Username
	}
	if config.Password != "" {
		dsn.Passwd = config.Password
	}

	return dsn.FormatDSN(), nil
}

// executeMigrations runs the actual migration commands.
func (m *MySQLMigrationProvider) executeMigrations(ctx context.Context, provider *goose.Provider, config storage.MigrationConfig) error {
	currentVersion, err := provider.GetDBVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get mysql db version: %w", err)
	}

	log.Printf("mysql current version %d", currentVersion)

	if config.TargetVersion == 0 {
		log.Println("running all mysql migrations")
		_, err := provider.Up(ctx)
		if err != nil {
			return fmt.Errorf("failed to run mysql migrations: %w", err)
		}
		log.Println("mysql migration done")
		return nil
	}

	log.Printf("migrating mysql to %d", config.TargetVersion)
	targetInt64Version := int64(config.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		_, err := provider.DownTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run mysql migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		_, err := provider.UpTo(ctx, targetInt64Version)
		if err != nil {
			return fmt.Errorf("failed to run mysql migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		log.Println("mysql nothing to do")
		return nil
	}

	log.Println("mysql migration done")
	return nil
}
