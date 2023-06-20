package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/storage/sqlcommon"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func NewValidateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate-models",
		Short: "Validate authorization models. NOTE: this command is in beta and may be removed in future releases.",
		Long:  "List all authorization models across all stores and run validations against them.\nNOTE: this command is in beta and may be removed in future releases.",
		RunE:  runValidate,
		Args:  cobra.NoArgs,
		PreRun: func(cmd *cobra.Command, args []string) {
			flags := cmd.Flags()

			util.MustBindPFlag(datastoreEngineFlag, flags.Lookup(datastoreEngineFlag))
			util.MustBindPFlag(datastoreURIFlag, flags.Lookup(datastoreURIFlag))
		},
	}

	flags := cmd.Flags()
	flags.String(datastoreEngineFlag, "", "the datastore engine")
	flags.String(datastoreURIFlag, "", "the connection uri to the datastore")

	// NOTE: if you add a new flag here, add the binding in PreRunE

	return cmd
}

func runValidate(_ *cobra.Command, _ []string) error {
	engine := viper.GetString(datastoreEngineFlag)
	uri := viper.GetString(datastoreURIFlag)

	ctx := context.Background()

	var (
		db  storage.OpenFGADatastore
		err error
	)
	switch engine {
	case "mysql":
		db, err = mysql.New(uri, sqlcommon.NewConfig())
	case "postgres":
		db, err = postgres.New(uri, sqlcommon.NewConfig())
	case "":
		return fmt.Errorf("missing datastore engine type")
	case "memory":
		fallthrough
	default:
		return fmt.Errorf("invalid datastore engine type: %s", engine)
	}

	if err != nil {
		return fmt.Errorf("could not connect to database")
	}

	validationResults, err := util.ValidateAllAuthorizationModels(ctx, db)
	if err != nil {
		return err
	}

	// print validation results in json format to allow piping to other commands, e.g. jq
	marshalled, err := json.Marshal(validationResults)
	if err != nil {
		return fmt.Errorf("error gathering validation results: %w", err)
	}
	fmt.Println(string(marshalled))

	return nil
}
