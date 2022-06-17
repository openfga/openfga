package cmd

import (
	"github.com/spf13/cobra"
)

func NewRootCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "openfga",
		Short: "A high performance and flexible authorization/permission engine built for developers and inspired by Google Zanzibar",
		Long: `A high performance and flexible authorization/permission engine built for developers and inspired by Google Zanzibar.
OpenFGA is designed to make it easy for developers to model their application permissions, and to add and integrate fine-grained authorization into their applications.`,
	}
}
