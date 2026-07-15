package fsm

import (
	"encoding/json"
	"testing"
)

func apply(t *testing.T, f *KVFSM, cmd Command) Result {
	t.Helper()
	raw, err := cmd.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out := f.Apply(raw)
	var res Result
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	return res
}

func TestKVFSM_ApplySetGetDelete(t *testing.T) {
	f := NewKVFSM()

	if res := apply(t, f, Command{Op: OpSet, Key: "foo", Value: "bar"}); !res.OK {
		t.Fatalf("set failed: %+v", res)
	}
	val, ok := f.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("Get(foo) = (%q, %v), want (bar, true)", val, ok)
	}

	if res := apply(t, f, Command{Op: OpSet, Key: "foo", Value: "baz"}); !res.OK {
		t.Fatalf("overwrite failed: %+v", res)
	}
	val, ok = f.Get("foo")
	if !ok || val != "baz" {
		t.Fatalf("Get(foo) after overwrite = (%q, %v), want (baz, true)", val, ok)
	}

	if res := apply(t, f, Command{Op: OpDelete, Key: "foo"}); !res.OK || !res.Existed {
		t.Fatalf("delete result = %+v, want OK=true Existed=true", res)
	}
	if _, ok := f.Get("foo"); ok {
		t.Fatalf("key foo still present after delete")
	}

	if res := apply(t, f, Command{Op: OpDelete, Key: "missing"}); !res.OK || res.Existed {
		t.Fatalf("delete of missing key = %+v, want OK=true Existed=false", res)
	}
}

func TestKVFSM_ApplyUnknownOp(t *testing.T) {
	f := NewKVFSM()
	res := apply(t, f, Command{Op: "bogus", Key: "x"})
	if res.OK {
		t.Fatalf("expected failure for unknown op, got %+v", res)
	}
}

func TestKVFSM_ApplyInvalidJSON(t *testing.T) {
	f := NewKVFSM()
	out := f.Apply([]byte("not json"))
	var res Result
	if err := json.Unmarshal(out, &res); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if res.OK {
		t.Fatalf("expected failure for invalid JSON command")
	}
}

func TestKVFSM_SnapshotRestore(t *testing.T) {
	f := NewKVFSM()
	apply(t, f, Command{Op: OpSet, Key: "a", Value: "1"})
	apply(t, f, Command{Op: OpSet, Key: "b", Value: "2"})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	f2 := NewKVFSM()
	if err := f2.Restore(snap); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, ok := f2.Get(kv.k)
		if !ok || got != kv.v {
			t.Errorf("restored Get(%s) = (%q, %v), want (%q, true)", kv.k, got, ok, kv.v)
		}
	}
}

func TestKVFSM_ConcurrentAccess(t *testing.T) {
	f := NewKVFSM()
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(i int) {
			apply(t, f, Command{Op: OpSet, Key: "k", Value: "v"})
			f.Get("k")
			done <- struct{}{}
		}(i)
	}
	for i := 0; i < 10; i++ {
		<-done
	}
}
