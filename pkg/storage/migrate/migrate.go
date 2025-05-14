package migrate

import (
	"time"

	"context"
	"fmt"
	"log"
	"net/url"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/pkg/storage/sqlite"
	"github.com/pressly/goose/v3"
)

type MigrationConfig struct {
	Engine        string
	URI           string
	TargetVersion uint
	Timeout       time.Duration
	Verbose       bool
	Username      string
	Password      string
}

// RunMigration runs the migrations for the given config. This function is exposed to allow embedding openFGA
// into applications and manage OpenFGA's database schema migrations directly. When OpenFGA is used as a library,
// the embedding application may have its own migration system that differs from OpenFGA's use of goose.
// By exposing this function, applications can:
// 1. Explicitly control when OpenFGA migrations run
// 2. Integrate OpenFGA's schema updates into their own migration workflows
// 3. Perform versioned upgrades of the schema as needed
// The function handles migrations for multiple database engines (postgres, mysql, sqlite) and supports
// both upgrading and downgrading to specific versions.
func RunMigration(cfg MigrationConfig) error {
	goose.SetLogger(goose.NopLogger())
	goose.SetVerbose(cfg.Verbose)

	var driver, migrationsPath string
	var uri string
	// We set uri based on engine
	uri = cfg.URI
	switch cfg.Engine {
	case "memory":
		log.Println("no migrations to run for `memory` datastore")
		return nil
	case "mysql":
		driver = "mysql"
		migrationsPath = assets.MySQLMigrationDir

		// Parse the database uri with the mysql drivers function for it and update username/password, if set via flags
		dsn, err := mysql.ParseDSN(uri)
		if err != nil {
			return fmt.Errorf("invalid database uri: %v", err)
		}
		if cfg.Username != "" {
			dsn.User = cfg.Username
		}
		if cfg.Password != "" {
			dsn.Passwd = cfg.Password
		}
		uri = dsn.FormatDSN()

	case "postgres":
		driver = "pgx"
		migrationsPath = assets.PostgresMigrationDir
		var username, password string

		// Parse the database uri with url.Parse() and update username/password, if set via flags
		dbURI, err := url.Parse(uri)
		if err != nil {
			return fmt.Errorf("invalid database uri: %v", err)
		}
		// if username not set
		if cfg.Username == "" && dbURI.User != nil {
			username = dbURI.User.Username()
		}
		if cfg.Password == "" && dbURI.User != nil {
			password, _ = dbURI.User.Password()
		}
		dbURI.User = url.UserPassword(username, password)

		// Replace CLI uri with the one we just updated.
		uri = dbURI.String()
	case "sqlite":
		driver = "sqlite"
		migrationsPath = assets.SqliteMigrationDir

		var err error
		uri, err = sqlite.PrepareDSN(uri)
		if err != nil {
			return err
		}
	case "":
		return fmt.Errorf("missing datastore engine type")
	default:
		return fmt.Errorf("unknown datastore engine type: %s", cfg.Engine)
	}

	db, err := goose.OpenDBWithDriver(driver, uri)
	if err != nil {
		return fmt.Errorf("failed to open a connection to the datastore: %w", err)
	}
	defer db.Close()

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = cfg.Timeout
	err = backoff.Retry(func() error {
		return db.PingContext(context.Background())
	}, policy)
	if err != nil {
		return fmt.Errorf("failed to initialize database connection: %w", err)
	}

	goose.SetBaseFS(assets.EmbedMigrations)

	currentVersion, err := goose.GetDBVersion(db)
	if err != nil {
		return fmt.Errorf("failed to get db version: %w", err)
	}

	log.Printf("current version %d", currentVersion)

	if cfg.TargetVersion == 0 {
		log.Println("running all migrations")
		if err := goose.Up(db, migrationsPath); err != nil {
			return fmt.Errorf("failed to run migrations: %w", err)
		}
		log.Println("migration done")
		return nil
	}

	log.Printf("migrating to %d", cfg.TargetVersion)
	targetInt64Version := int64(cfg.TargetVersion)

	switch {
	case targetInt64Version < currentVersion:
		if err := goose.DownTo(db, migrationsPath, targetInt64Version); err != nil {
			return fmt.Errorf("failed to run migrations down to %v: %w", targetInt64Version, err)
		}
	case targetInt64Version > currentVersion:
		if err := goose.UpTo(db, migrationsPath, targetInt64Version); err != nil {
			return fmt.Errorf("failed to run migrations up to %v: %w", targetInt64Version, err)
		}
	default:
		log.Println("nothing to do")
		return nil
	}

	log.Println("migration done")
	return nil
}
