package grpcclient

import (
	"os"
	"testing"
)

func clearTLS() {
	os.Unsetenv("TLS_CERT_FILE")
	os.Unsetenv("TLS_KEY_FILE")
	os.Unsetenv("TLS_CA_FILE")
}

func TestLoadTLSConfigDevModeFallsBackToNil(t *testing.T) {
	clearTLS()
	if v := os.Getenv("DEV_MODE"); v != "1" {
		t.Setenv("DEV_MODE", "1")
	}
	cfg, err := loadTLSConfig()
	if err != nil {
		t.Fatalf("expected nil err in dev mode, got %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil cfg in dev mode, got %+v", cfg)
	}
}

func TestLoadTLSConfigProdMissingEnvIsError(t *testing.T) {
	clearTLS()
	t.Setenv("DEV_MODE", "0")
	if _, err := loadTLSConfig(); err == nil {
		t.Fatal("expected error when TLS env vars are missing in prod")
	}
}

func TestLoadTLSConfigPartialSetIsError(t *testing.T) {
	t.Setenv("TLS_CERT_FILE", "/x/cert.pem")
	t.Setenv("TLS_KEY_FILE", "")
	t.Setenv("TLS_CA_FILE", "")
	t.Setenv("DEV_MODE", "1")
	if _, err := loadTLSConfig(); err == nil {
		t.Fatal("expected error when only some TLS env vars are set")
	}
}

func TestLoadTLSConfigBadCertFilesIsError(t *testing.T) {
	t.Setenv("TLS_CERT_FILE", "/no/cert.pem")
	t.Setenv("TLS_KEY_FILE", "/no/key.pem")
	t.Setenv("TLS_CA_FILE", "/no/ca.pem")
	t.Setenv("DEV_MODE", "0")
	if _, err := loadTLSConfig(); err == nil {
		t.Fatal("expected error when cert files do not exist")
	}
}
