// Package build provides build information that is linked into the application. Other
// packages within this project can use this information in logs etc..
package build

var (

	// Version is the build version of the app (e.g. v0.1.0)
	Version = "dev"

	// Commit is the sha of the git commit the app was built against.
	Commit = "none"

	// Date is the date when the app was built.
	Date = "unknown"
)
