package migrate

import "github.com/pressly/goose/v3"

func init() {
	goose.SetLogger(goose.NopLogger())
}
