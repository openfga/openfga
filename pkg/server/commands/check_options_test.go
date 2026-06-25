package commands

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/logger"
)

// TestNewCheckQuery_AppliesAllOptions exercises every CheckQueryV2Option by
// constructing a CheckQueryV2 with all of them set. The options only assign
// fields, so nil dependency values are safe at construction time.
func TestNewCheckQuery_AppliesAllOptions(t *testing.T) {
	q := NewCheckQuery(
		WithCheckQueryV2Logger(logger.NewNoopLogger()),
		WithCheckQueryV2Datastore(nil),
		WithCheckQueryV2Model(nil),
		WithCheckQueryV2Cache(nil),
		WithCheckQueryV2QueryCacheEnabled(true),
		WithCheckQueryV2QueryCacheTTL(10*time.Second),
		WithCheckQueryV2LastCacheInvalidationTime(time.Unix(0, 0)),
		WithCheckQueryV2Planner(nil),
		WithCheckQueryV2ConcurrencyLimit(8),
		WithCheckQueryV2MaxConcurrentReads(16),
		WithCheckQueryV2DatastoreThrottling(true, 100, time.Second),
		WithCheckQueryV2UpstreamTimeout(5*time.Second),
		WithCheckQueryV2SharedResources(nil),
		WithCheckQueryV2Fallback(nil),
	)

	require.NotNil(t, q)
	require.True(t, q.queryCacheEnabled)
	require.Equal(t, 10*time.Second, q.queryCacheTTL)
	require.Equal(t, 8, q.concurrencyLimit)
	require.Equal(t, uint32(16), q.datastoreOp.Concurrency)
	require.True(t, q.datastoreOp.ThrottlingEnabled)
	require.Equal(t, 5*time.Second, q.upstreamTimeout)
}

func TestNewCheckQuery_DefaultsWithoutOptions(t *testing.T) {
	q := NewCheckQuery()
	require.NotNil(t, q)
	require.NotNil(t, q.logger)
	require.Equal(t, V2CheckMethodName, string(q.datastoreOp.Method))
}

func TestNewBatchCheckCommand_AppliesOptions(t *testing.T) {
	cmd := NewBatchCheckCommand(
		nil, // checker dependency is only invoked at Execute time
		WithBatchCheckCommandLogger(logger.NewNoopLogger()),
		WithBatchCheckMaxConcurrentChecks(7),
		WithBatchCheckMaxChecksPerBatch(123),
	)
	require.NotNil(t, cmd)

	// Defaults path.
	require.NotNil(t, NewBatchCheckCommand(nil))
}
