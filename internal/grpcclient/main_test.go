package grpcclient

import (
	"os"
	"testing"
)

// TestMain sets DEV_MODE=1 so the partner dial constructors fall back to
// insecure transport when no TLS material is configured.
func TestMain(m *testing.M) {
	if os.Getenv("DEV_MODE") == "" {
		os.Setenv("DEV_MODE", "1")
	}
	os.Exit(m.Run())
}
