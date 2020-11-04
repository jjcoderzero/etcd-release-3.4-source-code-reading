package auth

import (
	"context"
)

type tokenNop struct{}

func (t *tokenNop) enable()                         {}
func (t *tokenNop) disable()                        {}
func (t *tokenNop) invalidateUser(string)           {}
func (t *tokenNop) genTokenPrefix() (string, error) { return "", nil }
func (t *tokenNop) info(ctx context.Context, token string, rev uint64) (*AuthInfo, bool) {
	return nil, false
}
func (t *tokenNop) assign(ctx context.Context, username string, revision uint64) (string, error) {
	return "", ErrAuthFailed
}
func newTokenProviderNop() (*tokenNop, error) {
	return &tokenNop{}, nil
}
