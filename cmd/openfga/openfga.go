package main

import (
	"os"

	"github.com/openfga/openfga/pkg/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	runCmd := cmd.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
