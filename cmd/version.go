package cmd

import (
	"log"

	"github.com/openfga/openfga/internal/build"
	"github.com/spf13/cobra"
)

// NewVersionCommand returns the command to get openfga version
func NewVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Return the OpenFGA version",
		Long:  "Return the OpenFGA version.",
		RunE:  version,
		Args:  cobra.NoArgs,
	}

	return cmd
}

// print out the built version
func version(_ *cobra.Command, _ []string) error {
	log.Printf("OpenFGA Version %s Date %s commit id %s ", build.Version, build.Date, build.Commit)
	return nil
}
