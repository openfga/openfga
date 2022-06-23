package main

import (
	"log"

	"github.com/openfga/openfga/pkg/cmd"
)

func main() {
	rootCmd := cmd.NewRootCommand()

	runCmd := cmd.NewRunCommand()
	rootCmd.AddCommand(runCmd)

	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}
