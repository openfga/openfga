package condition

import "fmt"

type CompilationError struct {
	Expression string
	Cause      error
}

func (e *CompilationError) Error() string {
	return fmt.Sprintf("failed to compile condition expression '%s'", e.Expression)
}

func (e *CompilationError) Unwrap() error {
	return e.Cause
}
