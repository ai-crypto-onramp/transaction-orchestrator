// Package quotelocker defines the quote-lock abstraction used at tx creation.
//
// The real implementation (Redis-backed) lands in Stage 8; the default NoopLocker
// satisfies the interface for Stages 2–7.
package quotelocker

import "context"

// Locker locks a quote for the duration of tx creation so two concurrent
// purchases against the same quote cannot both succeed.
type Locker interface {
	// Acquire returns a release function on success, or false if the quote
	// is already locked.  The release function is safe to call multiple times.
	Acquire(ctx context.Context, quoteID string) (release func(), ok bool, err error)
}

// NoopLocker is a Locker that always succeeds and does nothing on release.
type NoopLocker struct{}

// NewNoop returns a no-op Locker.
func NewNoop() *NoopLocker { return &NoopLocker{} }

// Acquire always returns ok=true.
func (NoopLocker) Acquire(ctx context.Context, quoteID string) (func(), bool, error) {
	return func() {}, true, nil
}