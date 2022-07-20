package main

import (
	"os"

	"github.com/openfga/openfga/pkg/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	runCmd := cmd.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	migrateCmd := cmd.NewMigrateCommand()
	rootCmd.AddCommand(migrateCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
