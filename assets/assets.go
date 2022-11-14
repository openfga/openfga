package assets

import "embed"

const (
	MySQLMigrationDir    = "migrations/mysql"
	PostgresMigrationDir = "migrations/postgres"
)

//go:embed migrations/*
var EmbedMigrations embed.FS

//go:embed playground/*
var EmbedPlayground embed.FS
