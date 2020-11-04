package auth

import (
	"testing"

	"go.etcd.io/etcd/auth/authpb"
	"go.etcd.io/etcd/pkg/adt"

	"go.uber.org/zap"
)

func TestRangePermission(t *testing.T) {
	tests := []struct {
		perms []adt.Interval
		begin []byte
		end   []byte
		want  bool
	}{
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("c")), adt.NewBytesAffineInterval([]byte("x"), []byte("z"))},
			[]byte("a"), []byte("z"),
			false,
		},
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("f")), adt.NewBytesAffineInterval([]byte("c"), []byte("d")), adt.NewBytesAffineInterval([]byte("f"), []byte("z"))},
			[]byte("a"), []byte("z"),
			true,
		},
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("d")), adt.NewBytesAffineInterval([]byte("a"), []byte("b")), adt.NewBytesAffineInterval([]byte("c"), []byte("f"))},
			[]byte("a"), []byte("f"),
			true,
		},
	}

	for i, tt := range tests {
		readPerms := adt.NewIntervalTree()
		for _, p := range tt.perms {
			readPerms.Insert(p, struct{}{})
		}

		result := checkKeyInterval(zap.NewExample(), &unifiedRangePermissions{readPerms: readPerms}, tt.begin, tt.end, authpb.READ)
		if result != tt.want {
			t.Errorf("#%d: result=%t, want=%t", i, result, tt.want)
		}
	}
}

func TestKeyPermission(t *testing.T) {
	tests := []struct {
		perms []adt.Interval
		key   []byte
		want  bool
	}{
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("c")), adt.NewBytesAffineInterval([]byte("x"), []byte("z"))},
			[]byte("f"),
			false,
		},
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("f")), adt.NewBytesAffineInterval([]byte("c"), []byte("d")), adt.NewBytesAffineInterval([]byte("f"), []byte("z"))},
			[]byte("b"),
			true,
		},
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("d")), adt.NewBytesAffineInterval([]byte("a"), []byte("b")), adt.NewBytesAffineInterval([]byte("c"), []byte("f"))},
			[]byte("d"),
			true,
		},
		{
			[]adt.Interval{adt.NewBytesAffineInterval([]byte("a"), []byte("d")), adt.NewBytesAffineInterval([]byte("a"), []byte("b")), adt.NewBytesAffineInterval([]byte("c"), []byte("f"))},
			[]byte("f"),
			false,
		},
	}

	for i, tt := range tests {
		readPerms := adt.NewIntervalTree()
		for _, p := range tt.perms {
			readPerms.Insert(p, struct{}{})
		}

		result := checkKeyPoint(zap.NewExample(), &unifiedRangePermissions{readPerms: readPerms}, tt.key, authpb.READ)
		if result != tt.want {
			t.Errorf("#%d: result=%t, want=%t", i, result, tt.want)
		}
	}
}
