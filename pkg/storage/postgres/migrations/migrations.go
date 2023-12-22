package migrations

import "github.com/openfga/openfga/pkg/storage/migrate"

var (
	Migrations = migrate.NewRegistry("postgres")
)
