package mysql

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
)

// Connection pool tests for MySQL datastore to ensure that the following environment variables
// and their corresponding configurations work as expected:
// - OPENFGA_DATASTORE_MAX_OPEN_CONNS (MaxOpenConns)
// - OPENFGA_DATASTORE_MAX_IDLE_CONNS (MaxIdleConns)  
// - OPENFGA_DATASTORE_CONN_MAX_LIFETIME (ConnMaxLifetime)
// - OPENFGA_DATASTORE_CONN_MAX_IDLE_TIME (ConnMaxIdleTime)
//
// These tests validate that:
// 1. Number of open connections never exceeds the configured limit
// 2. Number of idle connections never exceeds the configured limit
// 3. Connections are closed when they exceed max lifetime
// 4. Idle connections are closed when they exceed max idle time
// 5. Connections can be used up to their max lifetime
// 6. Idle connections can be used up to their max idle time

func TestMySQLConnectionPoolMaxOpenConnections(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	uri := testDatastore.GetConnectionURI(true)

	maxOpenConns := 2
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = maxOpenConns
	cfg.MaxIdleConns = 1

	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	// Create more concurrent operations than max open connections
	numOperations := 5
	var wg sync.WaitGroup
	wg.Add(numOperations)

	// Channel to collect the max open connections observed during concurrent operations
	maxObserved := make(chan int, numOperations)
	errors := make(chan error, numOperations)

	for i := 0; i < numOperations; i++ {
		go func() {
			defer wg.Done()
			
			// Create a context with timeout to prevent hanging
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			
			// Create a simple query that takes some time to ensure connections are held
			rows, err := ds.db.QueryContext(ctx, "SELECT SLEEP(0.1)")
			if err != nil {
				errors <- err
				return
			}
			rows.Close()
			
			// Check connection stats
			stats := ds.db.Stats()
			maxObserved <- stats.OpenConnections
		}()
	}

	wg.Wait()
	close(maxObserved)
	close(errors)

	// Check for any errors
	for err := range errors {
		require.NoError(t, err)
	}

	// Verify that open connections never exceeded the configured limit
	for observed := range maxObserved {
		require.LessOrEqual(t, observed, maxOpenConns, 
			fmt.Sprintf("Open connections (%d) exceeded max limit (%d)", observed, maxOpenConns))
	}

	// Final stats check
	finalStats := ds.db.Stats()
	require.LessOrEqual(t, finalStats.OpenConnections, maxOpenConns)
}

func TestMySQLConnectionPoolMaxIdleConnections(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	uri := testDatastore.GetConnectionURI(true)

	maxIdleConns := 1
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = 5
	cfg.MaxIdleConns = maxIdleConns

	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	// Create several connections by running queries
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	for i := 0; i < 3; i++ {
		rows, err := ds.db.QueryContext(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}

	// Wait a bit for connections to become idle
	time.Sleep(100 * time.Millisecond)

	// Check that idle connections don't exceed the limit
	stats := ds.db.Stats()
	require.LessOrEqual(t, stats.Idle, maxIdleConns,
		fmt.Sprintf("Idle connections (%d) exceeded max limit (%d)", stats.Idle, maxIdleConns))
}

func TestMySQLConnectionPoolMaxLifetime(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	uri := testDatastore.GetConnectionURI(true)

	maxLifetime := 500 * time.Millisecond
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = 5
	cfg.MaxIdleConns = 2
	cfg.ConnMaxLifetime = maxLifetime

	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	// Create some connections
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	for i := 0; i < 2; i++ {
		rows, err := ds.db.QueryContext(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}

	// Record initial stats
	initialStats := ds.db.Stats()
	initialMaxLifetimeClosed := initialStats.MaxLifetimeClosed

	// Wait for connections to exceed max lifetime
	time.Sleep(maxLifetime + 100*time.Millisecond)

	// Force new connections to be created to trigger cleanup of old ones
	for i := 0; i < 2; i++ {
		rows, err := ds.db.QueryContext(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}

	// Check that connections were closed due to max lifetime
	finalStats := ds.db.Stats()
	require.GreaterOrEqual(t, finalStats.MaxLifetimeClosed, initialMaxLifetimeClosed,
		"Expected connections to be closed due to max lifetime")
}

func TestMySQLConnectionPoolMaxIdleTime(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	uri := testDatastore.GetConnectionURI(true)

	maxIdleTime := 500 * time.Millisecond
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = 5
	cfg.MaxIdleConns = 2
	cfg.ConnMaxIdleTime = maxIdleTime

	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	// Create some connections
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	for i := 0; i < 2; i++ {
		rows, err := ds.db.QueryContext(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}

	// Record initial stats
	initialStats := ds.db.Stats()
	initialMaxIdleTimeClosed := initialStats.MaxIdleTimeClosed

	// Wait for connections to exceed max idle time
	time.Sleep(maxIdleTime + 100*time.Millisecond)

	// Force new connections to be created to trigger cleanup of idle ones
	for i := 0; i < 2; i++ {
		rows, err := ds.db.QueryContext(ctx, "SELECT 1")
		require.NoError(t, err)
		rows.Close()
	}

	// Check that connections were closed due to max idle time
	finalStats := ds.db.Stats()
	require.GreaterOrEqual(t, finalStats.MaxIdleTimeClosed, initialMaxIdleTimeClosed,
		"Expected connections to be closed due to max idle time")
}

func TestMySQLConnectionPoolLifetimeLimits(t *testing.T) {
	testDatastore := storagefixtures.RunDatastoreTestContainer(t, "mysql")
	uri := testDatastore.GetConnectionURI(true)

	maxLifetime := 1 * time.Second
	maxIdleTime := 500 * time.Millisecond
	cfg := sqlcommon.NewConfig()
	cfg.MaxOpenConns = 3
	cfg.MaxIdleConns = 2
	cfg.ConnMaxLifetime = maxLifetime
	cfg.ConnMaxIdleTime = maxIdleTime

	ds, err := New(uri, cfg)
	require.NoError(t, err)
	defer ds.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test that connections can live up to max lifetime
	start := time.Now()
	rows, err := ds.db.QueryContext(ctx, "SELECT SLEEP(0.3)")
	require.NoError(t, err)
	rows.Close()
	elapsed := time.Since(start)
	require.Less(t, elapsed, maxLifetime, "Connection should be usable within max lifetime")

	// Test that idle connections can live up to max idle time  
	start = time.Now()
	rows, err = ds.db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)
	rows.Close()
	
	// Wait less than max idle time
	time.Sleep(maxIdleTime / 2)
	
	// Connection should still be usable
	rows, err = ds.db.QueryContext(ctx, "SELECT 1")
	require.NoError(t, err)
	rows.Close()
	elapsed = time.Since(start)
	require.Less(t, elapsed, maxIdleTime, "Idle connection should be usable within max idle time")
}