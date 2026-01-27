package storage

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver.
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/internal/dsql"
)

type dsqlTestContainer struct {
	clusterEndpoint string
	region          string
	version         int64
}

// NewDSQLTestContainer returns an implementation of the DatastoreTestContainer interface
// for Aurora DSQL.
func NewDSQLTestContainer() *dsqlTestContainer {
	return &dsqlTestContainer{}
}

func (d *dsqlTestContainer) GetDatabaseSchemaVersion() int64 {
	return d.version
}

// RunDSQLTestContainer connects to a DSQL cluster, runs migrations, and returns a
// bootstrapped implementation of the DatastoreTestContainer interface wired up for the
// DSQL datastore engine.
func (d *dsqlTestContainer) RunDSQLTestContainer(t testing.TB) DatastoreTestContainer {
	clusterEndpoint := os.Getenv("OPENFGA_DSQL_CLUSTER_ENDPOINT")
	if clusterEndpoint == "" {
		t.Skip("OPENFGA_DSQL_CLUSTER_ENDPOINT not set, skipping DSQL tests")
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = os.Getenv("AWS_DEFAULT_REGION")
	}
	if region == "" {
		t.Skip("AWS_REGION not set, skipping DSQL tests")
	}

	d.clusterEndpoint = clusterEndpoint
	d.region = region

	// Prepare the postgres URI with IAM token
	pgURI, err := dsql.PreparePostgresURI(d.GetConnectionURI(false))
	require.NoError(t, err, "failed to prepare DSQL URI")

	goose.SetLogger(goose.NopLogger())

	db, err := goose.OpenDBWithDriver("pgx", pgURI)
	require.NoError(t, err)

	// DSQL doesn't support SERIAL/IDENTITY, so create custom goose table.
	// Use retry logic for test resilience.
	err = ensureGooseTableWithRetry(db)
	require.NoError(t, err, "failed to create goose table")

	goose.SetBaseFS(assets.EmbedMigrations)

	err = goose.Up(db, assets.DSQLMigrationDir)
	require.NoError(t, err, "failed to run DSQL migrations")

	version, err := goose.GetDBVersion(db)
	require.NoError(t, err)
	d.version = version

	err = db.Close()
	require.NoError(t, err)

	t.Cleanup(func() {
		pgURI, err := dsql.PreparePostgresURI(d.GetConnectionURI(false))
		if err != nil {
			t.Logf("failed to prepare DSQL URI for cleanup: %v", err)
			return
		}

		db, err := goose.OpenDBWithDriver("pgx", pgURI)
		if err != nil {
			t.Logf("failed to connect for cleanup: %v", err)
			return
		}
		defer db.Close()

		tables := []string{"changelog", "tuple", "assertion", "authorization_model", "store"}
		for _, table := range tables {
			if _, err := db.Exec("DELETE FROM " + table); err != nil {
				t.Logf("failed to clean up table %s: %v", table, err)
			}
		}

		t.Log("DSQL test cleanup complete")
	})

	return d
}

// GetConnectionURI returns the DSQL connection URI.
func (d *dsqlTestContainer) GetConnectionURI(includeCredentials bool) string {
	return fmt.Sprintf("dsql://admin@%s/postgres?region=%s", d.clusterEndpoint, d.region)
}

func (d *dsqlTestContainer) GetUsername() string {
	return "admin"
}

func (d *dsqlTestContainer) GetPassword() string {
	return ""
}

func (d *dsqlTestContainer) CreateSecondary(t testing.TB) error {
	return fmt.Errorf("secondary datastores not supported for DSQL")
}

func (d *dsqlTestContainer) GetSecondaryConnectionURI(includeCredentials bool) string {
	return ""
}

// ensureGooseTableWithRetry wraps dsql.EnsureGooseTable with retry logic for test resilience.
func ensureGooseTableWithRetry(db *sql.DB) error {
	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second

	return backoff.Retry(func() error {
		return dsql.EnsureGooseTable(db)
	}, backoffPolicy)
}
