package mssql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	"github.com/cenkalti/backoff/v4"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"go.uber.org/zap"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

type Datastore struct {
	stbl             sq.StatementBuilderType
	db               *sql.DB
	dbInfo           *sqlcommon.DBInfo
	logger           logger.Logger
	dbStatsCollector prometheus.Collector
}

var _ storage.OpenFGADatastore = (*Datastore)(nil)

func New(uri string, cfg *sqlcommon.Config) (*Datastore, error) {
	if cfg.Username != "" || cfg.Password != "" {
		parsed, err := url.Parse(uri)
		if err != nil {
			return nil, fmt.Errorf("parse mssql uri: %w", err)
		}

		user := parsed.User.Username()
		pass, _ := parsed.User.Password()

		if cfg.Username != "" {
			user = cfg.Username
		}
		if cfg.Password != "" {
			pass = cfg.Password
		}
		parsed.User = url.UserPassword(user, pass)
		uri = parsed.String()
	}

	db, err := sql.Open("sqlserver", uri)
	if err != nil {
		return nil, fmt.Errorf("open mssql connection: %w", err)
	}

	return NewWithDB(db, cfg)
}

func NewWithDB(db *sql.DB, cfg *sqlcommon.Config) (*Datastore, error) {
	if cfg.MaxOpenConns != 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns != 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxIdleTime != 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
	if cfg.ConnMaxLifetime != 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 1 * time.Minute
	attempt := 1
	err := backoff.Retry(func() error {
		err := db.PingContext(context.Background())
		if err != nil {
			cfg.Logger.Info("waiting for MSSQL", zap.Int("attempt", attempt))
			attempt++
			return err
		}
		return nil
	}, policy)
	if err != nil {
		return nil, fmt.Errorf("ping mssql: %w", err)
	}

	var collector prometheus.Collector
	if cfg.ExportMetrics {
		collector = collectors.NewDBStatsCollector(db, "openfga")
		if err := prometheus.Register(collector); err != nil {
			return nil, fmt.Errorf("initialize metrics: %w", err)
		}
	}

	stbl := sq.StatementBuilder.PlaceholderFormat(sq.Question).RunWith(db)
	dbInfo := sqlcommon.NewDBInfo(db, stbl, HandleSQLError, "mssql")

	return &Datastore{
		stbl:             stbl,
		db:               db,
		dbInfo:           dbInfo,
		logger:           cfg.Logger,
		dbStatsCollector: collector,
	}, nil
}

func (s *Datastore) Close() {
	if s.dbStatsCollector != nil {
		prometheus.Unregister(s.dbStatsCollector)
	}
	s.db.Close()
}

func (s *Datastore) MaxTuplesPerWrite() int {
	return 100
}

func (s *Datastore) MaxTypesPerAuthorizationModel() int {
	return 100
}

func (s *Datastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	return sqlcommon.IsReady(ctx, s.db)
}

func (s *Datastore) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes) error {
	return sqlcommon.Write(ctx, s.dbInfo, store, deletes, writes, time.Now().UTC())
}

func (s *Datastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	return sqlcommon.WriteAuthorizationModel(ctx, s.dbInfo, store, model)
}

func (s *Datastore) ReadAuthorizationModel(ctx context.Context, store, modelID string) (*openfgav1.AuthorizationModel, error) {
	return sqlcommon.ReadAuthorizationModel(ctx, s.dbInfo, store, modelID)
}

func (s *Datastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	return sqlcommon.FindLatestAuthorizationModel(ctx, s.dbInfo, store)
}

func HandleSQLError(err error, args ...interface{}) error {
	if errors.Is(err, sql.ErrNoRows) {
		return storage.ErrNotFound
	}
	if strings.Contains(err.Error(), "Violation of PRIMARY KEY constraint") {
		return storage.ErrCollision
	}
	return fmt.Errorf("mssql error: %w", err)
}