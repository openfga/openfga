package migrations

import (
	"github.com/pressly/goose/v3"
)

type migration struct {
	up   goose.GoMigrationContext
	down goose.GoMigrationContext
}

var (
	migrations = map[string]migration{}
)

func Register() {
	for name, migration := range migrations {
		goose.AddNamedMigrationContext(name+".go", migration.up, migration.down)
	}
}
