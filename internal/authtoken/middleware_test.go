package authtoken

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
}

func issue(t *testing.T, secret string) string {
	t.Helper()
	tok, err := Issue("transaction-orchestrator", secret)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return tok
}

func TestMiddlewareValidTokenPasses(t *testing.T) {
	const secret = "s3cret"
	tok := issue(t, secret)
	h := Middleware(secret, false)(okHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestMiddlewareMissingTokenReturns401(t *testing.T) {
	h := Middleware("s3cret", false)(okHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	var body map[string]map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"]["code"] != "unauthorized" {
		t.Fatalf("unexpected error body: %v", body)
	}
}

func TestMiddlewareInvalidTokenReturns401(t *testing.T) {
	h := Middleware("s3cret", false)(okHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddlewareWrongSecretReturns401(t *testing.T) {
	tok := issue(t, "real-secret")
	h := Middleware("different-secret", false)(okHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestMiddlewareHealthzBypasses(t *testing.T) {
	h := Middleware("s3cret", false)(okHandler())
	for _, p := range []string{"/healthz", "/readyz", "/metrics"} {
		req := httptest.NewRequest(http.MethodGet, p, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: expected 200, got %d", p, rec.Code)
		}
	}
}

func TestMiddlewareDevModeBypasses(t *testing.T) {
	h := Middleware("", true)(okHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 in dev bypass, got %d", rec.Code)
	}
}

func TestMiddlewareExpiredTokenReturns401(t *testing.T) {
	const secret = "s3cret"
	now := time.Now().UTC()
	claims := Claims{Sub: "transaction-orchestrator", Iat: now.Unix(), Exp: now.Add(-time.Hour).Unix()}
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	tok, err := sign(header, claims, secret)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	h := Middleware(secret, false)(okHandler())
	req := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for expired token, got %d", rec.Code)
	}
}

func TestIssueRejectsEmptySecret(t *testing.T) {
	if _, err := Issue("svc", ""); err == nil {
		t.Fatal("expected error on empty secret")
	}
}
