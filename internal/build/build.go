// Package build provides build information that is linked into the application. Other
// packages within this project can use this information in logs etc..
package build

var (

	// Version is the build version of the app (e.g. v0.1.0).
	Version = "dev"

	// Commit is the sha of the git commit the app was built against.
	Commit = "none"

	// Date is the date when the app was built.
	Date = "unknown"

	// fipsEnabled is "true" when the binary was compiled with GOFIPS140=v1.0.0.
	// Set via -ldflags at build time; do not edit directly. Use FIPSEnabled instead.
	fipsEnabled = "false"

	// FIPSEnabled reports whether the binary was compiled with GOFIPS140=v1.0.0.
	FIPSEnabled = fipsEnabled == "true"

	// MinimumSupportedDatastoreSchemaRevision refers to the minimum schema version that is required to run
	// this specific build of OpenFGA. Refer to the `assets/migrations` artifacts for more information.
	MinimumSupportedDatastoreSchemaRevision int64 = 4

	ProjectName = "openfga"
)
