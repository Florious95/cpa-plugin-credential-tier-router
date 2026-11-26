package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"
)

type falseAckRestHost struct{ *reviewDiskHost }

func (h falseAckRestHost) saveAuth(ctx context.Context, name string, raw json.RawMessage) error {
	if err := h.reviewDiskHost.saveAuth(ctx, name, raw); err != nil {
		return err
	}
	return errors.New("host persisted the pause but its acknowledgement was lost")
}

func TestQuota429AdoptsExactPauseAfterFalseHostAcknowledgement(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	r.state.Settings.ActivePoolSize = 0
	r.host = falseAckRestHost{h}
	if err := r.handleUsage(context.Background(), quota429Event(t, "a")); err != nil {
		t.Fatal(err)
	}
	loaded, err := r.store.load()
	if err != nil || len(loaded.PendingRestPauses) != 0 || !loaded.Quota["a"].NeedsUnpause || loaded.Quota["a"].PauseID == "" {
		t.Fatalf("false acknowledgement did not recover the active lease: %+v / %v", loaded.Quota["a"], err)
	}
	assertManagedProjection(t, h, "a", true, -1)
}

func TestQuota429RecoversPauseAcrossKilledProcess(t *testing.T) {
	const childEnv = "CPA_TEST_REST_CRASH_AUTH_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		h := &reviewDiskHost{dir: dir}
		r := newRuntime(h)
		r.host = &quota429PostWriteFaultHost{reviewDiskHost: h, afterFirstSave: func() {
			p, err := os.FindProcess(os.Getpid())
			if err != nil || p.Kill() != nil {
				os.Exit(72)
			}
			os.Exit(73)
		}}
		_ = r.handleUsage(context.Background(), quota429Event(t, "a"))
		t.Fatal("crash fixture never wrote the credential")
	}
	r, h := reviewRuntime(t, 1)
	cmd := exec.Command(os.Args[0], "-test.run=^TestQuota429RecoversPauseAcrossKilledProcess$")
	cmd.Env = append(os.Environ(), childEnv+"="+h.dir)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Fatalf("crash fixture did not exit: %s", out)
	}
	interrupted, err := r.store.load()
	if err != nil || interrupted.Quota["a"].ManagedRest || interrupted.PendingRestPauses["a"].PauseID == "" {
		t.Fatalf("did not interrupt between host write and ledger commit: %v", err)
	}
	assertManagedProjection(t, h, "a", true, -1)
	x := newRuntime(h)
	if err := x.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	q := x.state.Quota["a"]
	if !q.ManagedRest || q.NeedsUnpause || len(x.state.PendingRestPauses) != 0 || q.RestUntil == nil || q.RestUntil.Sub(interrupted.PendingRestPauses["a"].At) != 16*time.Hour {
		t.Fatalf("recovered pause did not reach phase two: %+v", q)
	}
	assertManagedProjection(t, h, "a", false, -1)
}

func TestQuota429DoesNotAdoptAnUnjournaledHistoricalMarker(t *testing.T) {
	_, h := reviewRuntime(t, 1)
	stickySetAuth(t, h, "a", map[string]any{"disabled": true, "priority": -1, "cpa_pause_id": fixturePauseID})
	r := newRuntime(h)
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if r.state.Quota["a"].ManagedRest {
		t.Fatal("an old UUID without a pending intent must not grant enablement")
	}
	assertManagedProjection(t, h, "a", true, -1)
}
