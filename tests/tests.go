package tests

import (
	"context"
	"testing"

	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/testfixtures/storage"
	"github.com/stretchr/testify/require"
)

func StartServer(t testing.TB, cfg *run.Config) context.CancelFunc {
	container := storage.RunDatastoreTestContainer(t, cfg.Datastore.Engine)
	cfg.Datastore.URI = container.GetConnectionURI()

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := run.RunServer(ctx, cfg)
		require.NoError(t, err)
	}()

	return cancel
}
