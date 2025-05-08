// Package util provides common utilities for spf13/cobra CLI utilities
// that can be used for various commands within this project.
package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/openfga/openfga/pkg/storage/sqlite"
	storagefixtures "github.com/openfga/openfga/pkg/testfixtures/storage"
)

// MustBindPFlag attempts to bind a specific key to a pflag (as used by cobra) and panics
// if the binding fails with a non-nil error.
func MustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic("failed to bind pflag: " + err.Error())
	}
}

func MustBindEnv(input ...string) {
	if err := viper.BindEnv(input...); err != nil {
		panic("failed to bind env key: " + err.Error())
	}
}

func Contains[E comparable](s []E, v E) bool {
	return Index(s, v) >= 0
}

func Index[E comparable](s []E, v E) int {
	for i, vs := range s {
		if v == vs {
			return i
		}
	}
	return -1
}

// MustBootstrapDatastore returns the datastore's container, the datastore, and the URI to connect to it.
// It automatically cleans up the container after the test finishes.
func MustBootstrapDatastore(t testing.TB, engine string, imageVersion string) (storagefixtures.DatastoreTestContainer, storage.OpenFGADatastore, string) {
	container := storagefixtures.RunDatastoreTestContainer(t, engine, imageVersion)

	uri := container.GetConnectionURI(true)

	var ds storage.OpenFGADatastore
	var err error

	switch engine {
	case "memory":
		ds = memory.New()
	case "postgres":
		ds, err = postgres.New(uri, sqlcommon.NewConfig())
	case "mysql":
		ds, err = mysql.New(uri, sqlcommon.NewConfig())
	case "sqlite":
		ds, err = sqlite.New(uri, sqlcommon.NewConfig())
	default:
		t.Fatalf("unsupported datastore engine: %q", engine)
	}
	require.NoError(t, err)
	t.Cleanup(ds.Close)

	return container, ds, uri
}

func PrepareTempConfigDir(t *testing.T) string {
	_, err := os.Stat("/etc/openfga/config.yaml")
	require.ErrorIs(t, err, os.ErrNotExist, "Config file at /etc/openfga/config.yaml would disturb test result.")

	homedir := t.TempDir()
	t.Setenv("HOME", homedir)

	confdir := filepath.Join(homedir, ".openfga")
	require.NoError(t, os.Mkdir(confdir, 0750))

	return confdir
}

func PrepareTempConfigFile(t *testing.T, config string) {
	confdir := PrepareTempConfigDir(t)
	confFile, err := os.Create(filepath.Join(confdir, "config.yaml"))
	require.NoError(t, err)
	_, err = confFile.WriteString(config)
	require.NoError(t, err)
	require.NoError(t, confFile.Close())
}
