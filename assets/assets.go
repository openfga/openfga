// Package assets contains database migration scripts and test files
package assets

import "embed"

const (
	MySQLMigrationDir    = "migrations/mysql"
	PostgresMigrationDir = "migrations/postgres"
	SqliteMigrationDir   = "migrations/sqlite"
)

// EmbedMigrations within the openfga binary.
//
//go:embed migrations/*
var EmbedMigrations embed.FS

// EmbedPlayground within the openfga binary.
//
//go:embed playground/*
var EmbedPlayground embed.FS

// EmbedTests within the openfga binary.
//
//go:embed tests/*
var EmbedTests embed.FS

// EmbedRelationshipVisualizer embeds the relationship visualizer assets.
//
//go:embed relationship-visualizer/src/*
var EmbedRelationshipVisualizer embed.FS

// EmbedModelEditor embeds the model editor assets.
//
//go:embed model-editor/src/*
var EmbedModelEditor embed.FS
