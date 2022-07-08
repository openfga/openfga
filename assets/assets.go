package assets

import "embed"

//go:embed migrations/*
var EmbedMigrations embed.FS
