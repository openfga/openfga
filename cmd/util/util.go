// Package util provides common utilities for spf13/cobra CLI utilities
// that can be used for various commands within this project.
package util

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
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

func MustBootstrapDatastore(t testing.TB, engine string) (storagefixtures.DatastoreTestContainer, storage.OpenFGADatastore, func(), string, error) {
	container, stopFunc := storagefixtures.RunDatastoreTestContainer(t, engine)

	uri := container.GetConnectionURI(true)

	var ds storage.OpenFGADatastore
	var err error

	switch engine {
	case "postgres":
		ds, err = postgres.New(uri, sqlcommon.NewConfig())
	case "mysql":
		ds, err = mysql.New(uri, sqlcommon.NewConfig())
	default:
		return nil, nil, func() {}, "", fmt.Errorf("'%s' is not a supported datastore engine", engine)
	}
	require.NoError(t, err)

	return container, ds, func() {
		defer ds.Close()
		stopFunc()
	}, uri, nil
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
