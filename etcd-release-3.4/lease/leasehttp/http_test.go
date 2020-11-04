package leasehttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.uber.org/zap"
)

func TestRenewHTTP(t *testing.T) {
	lg := zap.NewNop()
	be, tmpPath := backend.NewTmpBackend(time.Hour, 10000)
	defer os.Remove(tmpPath)
	defer be.Close()

	le := lease.NewLessor(lg, be, lease.LessorConfig{MinLeaseTTL: int64(5)})
	le.Promote(time.Second)
	l, err := le.Grant(1, int64(5))
	if err != nil {
		t.Fatalf("failed to create lease: %v", err)
	}

	ts := httptest.NewServer(NewHandler(le, waitReady))
	defer ts.Close()

	ttl, err := RenewHTTP(context.TODO(), l.ID, ts.URL+LeasePrefix, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	if ttl != 5 {
		t.Fatalf("ttl expected 5, got %d", ttl)
	}
}

func TestTimeToLiveHTTP(t *testing.T) {
	lg := zap.NewNop()
	be, tmpPath := backend.NewTmpBackend(time.Hour, 10000)
	defer os.Remove(tmpPath)
	defer be.Close()

	le := lease.NewLessor(lg, be, lease.LessorConfig{MinLeaseTTL: int64(5)})
	le.Promote(time.Second)
	l, err := le.Grant(1, int64(5))
	if err != nil {
		t.Fatalf("failed to create lease: %v", err)
	}

	ts := httptest.NewServer(NewHandler(le, waitReady))
	defer ts.Close()

	resp, err := TimeToLiveHTTP(context.TODO(), l.ID, true, ts.URL+LeaseInternalPrefix, http.DefaultTransport)
	if err != nil {
		t.Fatal(err)
	}
	if resp.LeaseTimeToLiveResponse.ID != 1 {
		t.Fatalf("lease id expected 1, got %d", resp.LeaseTimeToLiveResponse.ID)
	}
	if resp.LeaseTimeToLiveResponse.GrantedTTL != 5 {
		t.Fatalf("granted TTL expected 5, got %d", resp.LeaseTimeToLiveResponse.GrantedTTL)
	}
}

func TestRenewHTTPTimeout(t *testing.T) {
	testApplyTimeout(t, func(l *lease.Lease, serverURL string) error {
		_, err := RenewHTTP(context.TODO(), l.ID, serverURL+LeasePrefix, http.DefaultTransport)
		return err
	})
}

func TestTimeToLiveHTTPTimeout(t *testing.T) {
	testApplyTimeout(t, func(l *lease.Lease, serverURL string) error {
		_, err := TimeToLiveHTTP(context.TODO(), l.ID, true, serverURL+LeaseInternalPrefix, http.DefaultTransport)
		return err
	})
}

func testApplyTimeout(t *testing.T, f func(*lease.Lease, string) error) {
	lg := zap.NewNop()
	be, tmpPath := backend.NewTmpBackend(time.Hour, 10000)
	defer os.Remove(tmpPath)
	defer be.Close()

	le := lease.NewLessor(lg, be, lease.LessorConfig{MinLeaseTTL: int64(5)})
	le.Promote(time.Second)
	l, err := le.Grant(1, int64(5))
	if err != nil {
		t.Fatalf("failed to create lease: %v", err)
	}

	ts := httptest.NewServer(NewHandler(le, waitNotReady))
	defer ts.Close()
	err = f(l, ts.URL)
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if err.Error() != ErrLeaseHTTPTimeout.Error() {
		t.Fatalf("expected (%v), got (%v)", ErrLeaseHTTPTimeout.Error(), err.Error())
	}
}

func waitReady() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func waitNotReady() <-chan struct{} {
	return nil
}
