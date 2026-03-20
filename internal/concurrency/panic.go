package concurrency

import (
	"fmt"
	"runtime"
)

func RecoverFromPanic(err *error) {
	if r := recover(); r != nil {
		const size = 64 << 10
		stacktrace := make([]byte, size)
		stacktrace = stacktrace[:runtime.Stack(stacktrace, false)]

		*err = fmt.Errorf(
			"recovered from panic %q. (err=%w) Call stack:\n%s",
			r,
			*err,
			stacktrace,
		)
	}
}
