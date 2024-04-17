// Package main contains the root of all commands.
package main

import (
	"os"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/migrate"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/cmd/validatemodels"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	runCmd := run.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	migrateCmd := migrate.NewMigrateCommand()
	rootCmd.AddCommand(migrateCmd)

	validateModelsCmd := validatemodels.NewValidateCommand()
	rootCmd.AddCommand(validateModelsCmd)

	versionCmd := cmd.NewVersionCommand()
	rootCmd.AddCommand(versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
