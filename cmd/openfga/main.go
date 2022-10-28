package main

import (
	"os"
)

func main() {
	rootCmd := NewRootCommand()

	runCmd := NewRunCommand()
	rootCmd.AddCommand(runCmd)

	migrateCmd := NewMigrateCommand()
	rootCmd.AddCommand(migrateCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
