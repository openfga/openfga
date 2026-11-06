package cmd

import (
	"log"

	"github.com/spf13/cobra"

	"github.com/openfga/openfga/internal/build"
)

// NewVersionCommand returns the command to get openfga version.
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

// print out the built version.
func version(_ *cobra.Command, _ []string) error {
	if build.FIPSEnabled {
		log.Printf("OpenFGA version `%s` build from `%s` on `%s` (FIPS 140-3)", build.Version, build.Commit, build.Date)
	} else {
		log.Printf("OpenFGA version `%s` build from `%s` on `%s` ", build.Version, build.Commit, build.Date)
	}
	return nil
}
