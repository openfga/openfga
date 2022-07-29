package assets

import "embed"

const (
	PostgresMigrationDir = "migrations/postgres"
)

//go:embed migrations/*
var EmbedMigrations embed.FS

//go:embed playground/*
var EmbedPlayground embed.FS
