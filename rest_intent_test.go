package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
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

type restoreRestFaultHost struct {
	*reviewDiskHost
	afterEnable func() error
}

func (h restoreRestFaultHost) saveAuth(ctx context.Context, name string, raw json.RawMessage) error {
	var doc struct {
		Disabled bool   `json:"disabled"`
		PauseID  string `json:"cpa_pause_id"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return err
	}
	if name == "b.json" && !doc.Disabled {
		return errors.New("inject b restore failure")
	}
	if err := h.reviewDiskHost.saveAuth(ctx, name, raw); err != nil {
		return err
	}
	if name == "a.json" && !doc.Disabled && doc.PauseID == "" {
		return h.afterEnable()
	}
	return nil
}

func TestRestoreResumesWithoutRevertingSuccessfulAccounts(t *testing.T) {
	const childEnv = "CPA_TEST_RESTORE_RESUME_CRASH_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		r := newRuntime(&reviewDiskHost{dir: dir})
		r.host = restoreRestFaultHost{reviewDiskHost: &reviewDiskHost{dir: dir}, afterEnable: func() error {
			p, err := os.FindProcess(os.Getpid())
			if err != nil || p.Kill() != nil {
				os.Exit(72)
			}
			os.Exit(73)
			return nil
		}}
		_, _ = r.restorePool429(context.Background())
		t.Fatal("crash fixture never wrote the enabled projection")
	}
	for _, crash := range []bool{false, true} {
		t.Run(map[bool]string{false: "false_ack", true: "process_kill"}[crash], func(t *testing.T) {
			r, h := reviewRuntime(t, 2)
			r.state.Settings.ActivePoolSize = 0
			ctx := context.Background()
			for _, id := range []string{"a", "b"} {
				if err := r.handleUsage(ctx, quota429Event(t, id)); err != nil {
					t.Fatal(err)
				}
			}
			pool429Send(t, r)
			before := r.state.Quota["a"]
			if crash {
				cmd := exec.Command(os.Args[0], "-test.run=^TestRestoreResumesWithoutRevertingSuccessfulAccounts$")
				cmd.Env = append(os.Environ(), childEnv+"="+h.dir)
				if out, err := cmd.CombinedOutput(); err == nil {
					t.Fatalf("restore crash fixture did not exit: %s", out)
				}
			} else {
				r.host = restoreRestFaultHost{reviewDiskHost: h, afterEnable: func() error { return errors.New("lost enable acknowledgement") }}
				if _, err := r.restorePool429(ctx); err == nil {
					t.Fatal("b failure must leave the pool locked")
				}
			}
			state, err := r.store.load()
			if err != nil || !state.PoolLocked429 || !state.Pool429Restoring {
				t.Fatalf("partial restore must preserve the durable pool lock: %v", err)
			}
			if len(state.PendingRestPauses) != 0 || !state.Quota["a"].NeedsUnpause {
				t.Fatal("partial restore must not replace the original rest lease")
			}
			assertManagedProjection(t, h, "a", false, -1)
			restart := newRuntime(h)
			// A is already correct; either periodic re-disable or a redundant
			// restore write would hit this failure and fail the test.
			restart.host = &stickyFaultHost{reviewDiskHost: h, failSave: "a.json"}
			if err := restart.scheduledInspection(ctx, false); err != nil {
				t.Fatal(err)
			}
			q := restart.state.Quota["a"]
			if !q.NeedsUnpause || q.PauseID != before.PauseID || q.RestUntil == nil || !q.RestUntil.Equal(*before.RestUntil) {
				t.Fatalf("background enforcement modified a partial restore: %+v", q)
			}
			assertManagedProjection(t, h, "a", false, -1)
			if _, err := restart.restorePool429(ctx); err != nil {
				t.Fatal(err)
			}
			if restart.poolLocked429() || restart.state.Pool429Restoring || restart.state.Quota["a"].NeedsUnpause {
				t.Fatal("successful retry must clear the lock and the consumed permission")
			}
			assertManagedProjection(t, h, "a", false, -1)
			assertManagedProjection(t, h, "b", false, -1)
		})
	}
}

func TestPartialRestoreKeepsLockIfSnapshotFileDisappears(t *testing.T) {
	r, h := reviewRuntime(t, 2)
	ctx := context.Background()
	pool429Send(t, r)
	r.host = &stickyFaultHost{reviewDiskHost: h, failSave: "b.json"}
	if _, err := r.restorePool429(ctx); err == nil {
		t.Fatal("expected b write failure to keep lock")
	}
	assertManagedProjection(t, h, "a", false, 400)
	b, err := h.getAuth(ctx, "b")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(h.dir, b.Name)); err != nil {
		t.Fatal(err)
	}
	r = newRuntime(h)
	if _, err := r.restorePool429(ctx); err == nil || !r.poolLocked429() || !r.state.Pool429Restoring {
		t.Fatalf("missing snapshot credential must keep durable gate: %v", err)
	}
	assertManagedProjection(t, h, "a", false, 400)
	if err := h.saveAuth(ctx, b.Name, b.JSON); err != nil {
		t.Fatal(err)
	}
	if _, err := r.restorePool429(ctx); err != nil || r.poolLocked429() {
		t.Fatalf("full snapshot restored but gate did not clear: %v", err)
	}
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
