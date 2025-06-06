package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
)

type sqliteTestContainer struct {
	path    string
	version int64
}

// NewSqliteTestContainer returns an implementation of the DatastoreTestContainer interface
// for SQLite.
func NewSqliteTestContainer() *sqliteTestContainer {
	return &sqliteTestContainer{}
}

func (m *sqliteTestContainer) GetDatabaseSchemaVersion() int64 {
	return m.version
}

// RunSqliteTestContainer creates a sqlite database file, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// Sqlite datastore engine.
func (m *sqliteTestContainer) RunSqliteTestDatabase(t testing.TB) DatastoreTestContainer {
	dbDir, err := os.MkdirTemp("", "openfga-test-sqlite-*")
	require.NoError(t, err)

	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dbDir)) })

	m.path = filepath.Join(dbDir, "database.db")

	uri := m.GetConnectionURI(true)

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("sqlite", uri)
	require.NoError(t, err)
	defer db.Close()

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.SqliteMigrationDir)
	require.NoError(t, err)
	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	m.version = version

	err = db.Close()
	require.NoError(t, err)

	return m
}

// GetConnectionURI returns the sqlite connection uri for the running sqlite test container.
func (m *sqliteTestContainer) GetConnectionURI(includeCredentials bool) string {
	return fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(100)", m.path)
}

func (m *sqliteTestContainer) GetUsername() string {
	return ""
}

func (m *sqliteTestContainer) GetPassword() string {
	return ""
}

func (m *sqliteTestContainer) CreateSecondary(t testing.TB) error {
	return nil
}

func (m *sqliteTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}
