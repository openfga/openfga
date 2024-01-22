// Package assets contains playground assets and test files
package assets

import "embed"

//go:embed playground/*
var EmbedPlayground embed.FS

//go:embed tests/*
var EmbedTests embed.FS
