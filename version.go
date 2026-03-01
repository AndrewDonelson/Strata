// Copyright (c) 2026 Nlaak Studios (https://nlaak.com)
// Author: Andrew Donelson (https://www.linkedin.com/in/andrew-donelson/)
//
// version.go — build-time version, date, and environment metadata injected
// via -ldflags by the Makefile and exposed through the Version() function.

package strata

// Build-time variables injected via -ldflags by the Makefile.
// Defaults represent an unversioned local development build.
//
//	BuildDate format : YYYY.MM.DD-HHMM  (24-hour clock)
//	BuildEnv  values : dev | qa | prod
//
// Full version example: "2026.02.28-1750-dev"
var (
	// BuildDate is the date and time the binary was built.
	// Set by: -ldflags "-X 'github.com/AndrewDonelson/strata.BuildDate=2026.02.28-1750'"
	BuildDate = "0000.00.00-0000"

	// BuildEnv is the target environment for this build.
	// Set by: -ldflags "-X 'github.com/AndrewDonelson/strata.BuildEnv=dev'"
	BuildEnv = "dev"
)

// Version returns the full version string in the form "YYYY.MM.DD-HHMM-env",
// e.g. "2026.02.28-1750-dev".
func Version() string {
	return BuildDate + "-" + BuildEnv
}
