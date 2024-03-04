// Package assets contains database migration scripts and test files
package assets

import "embed"

const (
	MySQLMigrationDir    = "migrations/mysql"
	PostgresMigrationDir = "migrations/postgres"
	SQLiteMigrationDir   = "migrations/sqlite"
)

//go:embed migrations/*
var EmbedMigrations embed.FS

//go:embed playground/*
var EmbedPlayground embed.FS

//go:embed tests/*
var EmbedTests embed.FS
