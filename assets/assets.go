package assets

import "embed"

const (
	PlaygroundAssets = "playground/"
)

//go:embed playground/*
var EmbedPlayground embed.FS
