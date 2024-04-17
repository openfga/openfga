package migrate

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pressly/goose/v3"
)

type migrationFunc = func(ctx context.Context, tx *sql.Tx) error

type Migration struct {
	Version  int64
	Forward  migrationFunc
	Backward migrationFunc
}

type Registry struct {
	dialect goose.Dialect

	Migrations map[int64]*Migration
}

func (r *Registry) MustRegister(m *Migration) {
	if _, ok := r.Migrations[m.Version]; ok {
		panic(fmt.Errorf("migration with version %d already registered", m.Version))
	}
	r.Migrations[m.Version] = m
}

type runConfig struct {
	verbose       bool
	targetVersion int64
}

type runOption func(*runConfig)

func WithVerbose(v bool) runOption {
	return func(c *runConfig) {
		c.verbose = v
	}
}

func WithTargetVersion(v int64) runOption {
	return func(c *runConfig) {
		c.targetVersion = v
	}
}

func (r *Registry) Run(ctx context.Context, db *sql.DB, opts ...runOption) (int64, error) {
	var (
		migrations []*goose.Migration
	)

	cfg := &runConfig{
		targetVersion: 0,
		verbose:       false,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	for _, m := range r.Migrations {
		migrations = append(migrations, goose.NewGoMigration(
			m.Version, &goose.GoFunc{RunTx: m.Forward}, &goose.GoFunc{RunTx: m.Backward}),
		)
	}

	provider, err := goose.NewProvider(r.dialect, db, nil,
		goose.WithDisableGlobalRegistry(true),
		goose.WithVerbose(cfg.verbose),
		goose.WithGoMigrations(migrations...),
	)
	if err != nil {
		return 0, err
	}

	var results []*goose.MigrationResult

	if cfg.targetVersion == 0 {
		results, err = provider.Up(ctx)
		if err != nil {
			return 0, err
		}
	} else {
		currentVersion, err := provider.GetDBVersion(ctx)
		if err != nil {
			return 0, err
		}

		if cfg.targetVersion < currentVersion {
			results, err = provider.DownTo(ctx, cfg.targetVersion)
			if err != nil {
				return 0, err
			}
		} else if cfg.targetVersion > currentVersion {
			results, err = provider.UpTo(ctx, cfg.targetVersion)
			if err != nil {
				return 0, err
			}
		}
	}

	if len(results) > 0 {
		if lastResult := results[len(results)-1]; lastResult != nil {
			return lastResult.Source.Version, nil
		}
	}

	return 0, nil
}

func NewRegistry(engine string) *Registry {
	var dialect goose.Dialect

	switch engine {
	case "mysql":
		dialect = goose.DialectMySQL
	case "postgres":
		dialect = goose.DialectPostgres
	default:
		panic(fmt.Errorf("unknown datastore engine type: %s", engine))
	}

	return &Registry{
		dialect:    dialect,
		Migrations: make(map[int64]*Migration, 0),
	}
}
