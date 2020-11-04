package auth

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

// TestSimpleTokenDisabled ensures that TokenProviderSimple behaves correctly when
// disabled.
func TestSimpleTokenDisabled(t *testing.T) {
	initialState := newTokenProviderSimple(zap.NewExample(), dummyIndexWaiter, simpleTokenTTLDefault)

	explicitlyDisabled := newTokenProviderSimple(zap.NewExample(), dummyIndexWaiter, simpleTokenTTLDefault)
	explicitlyDisabled.enable()
	explicitlyDisabled.disable()

	for _, tp := range []*tokenSimple{initialState, explicitlyDisabled} {
		ctx := context.WithValue(context.WithValue(context.TODO(), AuthenticateParamIndex{}, uint64(1)), AuthenticateParamSimpleTokenPrefix{}, "dummy")
		token, err := tp.assign(ctx, "user1", 0)
		if err != nil {
			t.Fatal(err)
		}
		authInfo, ok := tp.info(ctx, token, 0)
		if ok {
			t.Errorf("expected (true, \"user1\") got (%t, %s)", ok, authInfo.Username)
		}

		tp.invalidateUser("user1") // should be no-op
	}
}

// TestSimpleTokenAssign ensures that TokenProviderSimple can correctly assign a
// token, look it up with info, and invalidate it by user.
func TestSimpleTokenAssign(t *testing.T) {
	tp := newTokenProviderSimple(zap.NewExample(), dummyIndexWaiter, simpleTokenTTLDefault)
	tp.enable()
	ctx := context.WithValue(context.WithValue(context.TODO(), AuthenticateParamIndex{}, uint64(1)), AuthenticateParamSimpleTokenPrefix{}, "dummy")
	token, err := tp.assign(ctx, "user1", 0)
	if err != nil {
		t.Fatal(err)
	}
	authInfo, ok := tp.info(ctx, token, 0)
	if !ok || authInfo.Username != "user1" {
		t.Errorf("expected (true, \"token2\") got (%t, %s)", ok, authInfo.Username)
	}

	tp.invalidateUser("user1")

	_, ok = tp.info(context.TODO(), token, 0)
	if ok {
		t.Errorf("expected ok == false after user is invalidated")
	}
}
