package main

import (
	"os"

	"github.com/openfga/openfga/cmd"
	"github.com/openfga/openfga/cmd/run"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	runCmd := run.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	migrateCmd := cmd.NewMigrateCommand()
	rootCmd.AddCommand(migrateCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
