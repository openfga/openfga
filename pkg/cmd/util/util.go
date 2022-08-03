// Package util provides common utilities for spf13/cobra CLI utilities
// that can be used for various commands within this project.
package util

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// MustBindPFlag attempts to bind a specific key to a pflag (as used by cobra) and panics
// if the binding fails with a non-nil error.
func MustBindPFlag(key string, flag *pflag.Flag) {
	if err := viper.BindPFlag(key, flag); err != nil {
		panic("failed to bind pflag: " + err.Error())
	}
}
