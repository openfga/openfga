package assets

import "embed"

const (
	PostgresMigrationDir = "migrations/postgres"
	MySQLMigrationDir    = "migrations/mysql"
)

//go:embed migrations/*
var EmbedMigrations embed.FS

//go:embed playground/*
var EmbedPlayground embed.FS
