package api

import (
	"os"
	"testing"
)

// TestMain sets DEV_MODE=1 so the authtoken middleware bypasses auth when no
// SERVICE_TOKEN_SECRET is configured. Individual tests that need auth set
// SERVICE_TOKEN_SECRET explicitly.
func TestMain(m *testing.M) {
	if os.Getenv("DEV_MODE") == "" {
		os.Setenv("DEV_MODE", "1")
	}
	os.Exit(m.Run())
}
