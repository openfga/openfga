package util

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestContains(t *testing.T) {
	s := []string{"a", "b", "c"}
	require.True(t, Contains(s, "b"))
	require.False(t, Contains(s, "z"))
	require.False(t, Contains([]string{}, "a"))

	ints := []int{1, 2, 3}
	require.True(t, Contains(ints, 3))
	require.False(t, Contains(ints, 4))
}

func TestIndex(t *testing.T) {
	s := []string{"a", "b", "c"}
	require.Equal(t, 0, Index(s, "a"))
	require.Equal(t, 2, Index(s, "c"))
	require.Equal(t, -1, Index(s, "missing"))
}

func TestMustBindPFlag(t *testing.T) {
	t.Cleanup(viper.Reset)

	flagSet := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flagSet.String("my-flag", "default-value", "a test flag")

	require.NotPanics(t, func() {
		MustBindPFlag("my-flag", flagSet.Lookup("my-flag"))
	})
	require.Equal(t, "default-value", viper.GetString("my-flag"))
}

func TestMustBindEnv(t *testing.T) {
	t.Cleanup(viper.Reset)

	t.Setenv("OPENFGA_TEST_KEY", "from-env")
	require.NotPanics(t, func() {
		MustBindEnv("test-key", "OPENFGA_TEST_KEY")
	})
	require.Equal(t, "from-env", viper.GetString("test-key"))
}

func TestPrepareTempConfigDir(t *testing.T) {
	// Skip if a real system config would interfere with the assertion.
	if _, err := os.Stat("/etc/openfga/config.yaml"); err == nil {
		t.Skip("/etc/openfga/config.yaml exists in this environment")
	}

	confDir := PrepareTempConfigDir(t)
	require.DirExists(t, confDir)
	require.True(t, filepath.IsAbs(confDir))
}

func TestPrepareTempConfigFile(t *testing.T) {
	if _, err := os.Stat("/etc/openfga/config.yaml"); err == nil {
		t.Skip("/etc/openfga/config.yaml exists in this environment")
	}

	PrepareTempConfigFile(t, "datastore:\n  engine: memory\n")

	confFile := filepath.Join(os.Getenv("HOME"), ".openfga", "config.yaml")
	require.FileExists(t, confFile)

	contents, err := os.ReadFile(confFile)
	require.NoError(t, err)
	require.Contains(t, string(contents), "engine: memory")
}
