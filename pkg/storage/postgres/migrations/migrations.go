package migrations

import (
	"github.com/pressly/goose/v3"
)

var Migrations []*goose.Migration

func register(migration *goose.Migration) {
	Migrations = append(Migrations, migration)
}
