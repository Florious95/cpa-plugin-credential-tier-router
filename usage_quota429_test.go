package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

const fixtureQuota429 = `{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"9792s"}]}}`

func quota429Event(t *testing.T, indexes ...string) []byte {
	t.Helper()
	index := "fixture"
	if len(indexes) > 0 {
		index = indexes[0]
	}
	event, err := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: index, Failed: true, Failure: usageFailure{StatusCode: 429, Body: fixtureQuota429}})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestQuota429ManagedRest(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	shortRest := now.Add(time.Hour)
	r.state.Quota["fixture"] = quotaSnapshot{ManagedRest: true, RestUntil: &shortRest}
	ctx := context.Background()
	if err := r.handleUsage(ctx, quota429Event(t)); err != nil {
		t.Fatal(err)
	}
	state, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	q := state.Quota["fixture"]
	if r.poolLocked429() || state.PoolLocked429 || state.Pool429Generation != 0 || !q.ManagedRest || q.RestUntil == nil || !q.RestUntil.Equal(now.Add(16*time.Hour)) || !h.file.Disabled || h.file.Priority != -1 {
		t.Fatalf("expected persisted single-credential rest without a pool lock; quota=%+v disabled=%v priority=%d", q, h.file.Disabled, h.file.Priority)
	}
	if err := r.scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	if h.file.Disabled || h.file.Priority != -1 || len(h.requests) != 0 {
		t.Fatal("next inspection should retain -1 while releasing hard disable without quota probes")
	}
}

func TestQuota429PhaseTwoPreservesManualDisableAcrossRestartAndExpiry(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	ctx := context.Background()
	if err := r.handleUsage(ctx, quota429Event(t)); err != nil {
		t.Fatal(err)
	}
	if !r.state.Quota["fixture"].NeedsUnpause {
		t.Fatal("phase one did not persist a pending unpause")
	}
	if err := r.scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	state, err := r.store.load()
	q := state.Quota["fixture"]
	if err != nil || h.file.Disabled || h.file.Priority != -1 || !q.ManagedRest || q.NeedsUnpause {
		t.Fatalf("phase two did not consume enable permission: %+v / %v", q, err)
	}
	var auth map[string]any
	if err := json.Unmarshal(h.document, &auth); err != nil {
		t.Fatal(err)
	}
	auth["disabled"] = true
	manual, err := json.Marshal(auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.saveAuth(ctx, h.file.Name, manual); err != nil {
		t.Fatal(err)
	}
	r.state = state // Reload the durable phase, not a transient process flag.
	if err := r.handleUsage(ctx, quota429Event(t)); err != nil {
		t.Fatal(err)
	}
	for _, expired := range []bool{false, true} {
		if expired {
			past := time.Now().Add(-time.Minute)
			q.RestUntil = &past
			r.state.Quota["fixture"] = q
		}
		if err := r.scheduledInspection(ctx, false); err != nil {
			t.Fatal(err)
		}
		if !h.file.Disabled || string(h.document) != string(manual) || r.state.Quota["fixture"].NeedsUnpause {
			t.Fatal("external disable was reversed or its one-shot permission rearmed")
		}
	}
}

func TestQuota429FailedHardDisableCannotUndoLaterManualDisable(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	ctx := context.Background()
	original := reviewAuth(t, h, "a")
	if original.Disabled {
		t.Fatal("fixture must start enabled")
	}
	r.host = &stickyFaultHost{reviewDiskHost: h, failSave: "a.json"}
	if err := r.handleUsage(ctx, quota429Event(t, "a")); err == nil {
		t.Fatal("expected a host write failure")
	}
	state, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if r.state.Quota["a"].NeedsUnpause || state.Quota["a"].NeedsUnpause || reviewAuth(t, h, "a").Disabled {
		t.Fatal("failed hard-disable granted permission or changed the credential")
	}
	stickySetAuth(t, h, "a", map[string]any{"disabled": true, "priority": -1})
	restarted := newRuntime(h)
	if err := restarted.scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	assertManagedProjection(t, h, "a", true, -1)
}

type quota429StateFaultHost struct {
	*reviewDiskHost
	failState func()
}

func (h *quota429StateFaultHost) getAuth(ctx context.Context, index string) (authDocument, error) {
	doc, err := h.reviewDiskHost.getAuth(ctx, index)
	h.failState()
	return doc, err
}

func TestQuota429FailedPermissionConsumptionDoesNotEnable(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	ctx := context.Background()
	if err := r.handleUsage(ctx, quota429Event(t, "a")); err != nil {
		t.Fatal(err)
	}
	before, err := h.getAuth(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	store := r.store
	badPath := t.TempDir() // rename onto an existing directory must fail.
	r.host = &quota429StateFaultHost{reviewDiskHost: h, failState: func() { r.store.path = badPath }}
	if err := r.scheduledInspection(ctx, false); err == nil {
		t.Fatal("expected state persistence failure after planning and before host write")
	}
	after, err := h.getAuth(ctx, "a")
	if err != nil || string(after.JSON) != string(before.JSON) {
		t.Fatalf("failed consumption changed the auth document: %v", err)
	}
	state, err := store.load()
	if err != nil || !state.Quota["a"].NeedsUnpause {
		t.Fatalf("failed consumption unexpectedly changed the durable lease: %v", err)
	}
	assertManagedProjection(t, h, "a", true, -1)
}

func TestQuota429BatchClearsEachSuccessfulUnpauseBeforeNextSave(t *testing.T) {
	r, h := reviewRuntime(t, 2)
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		if err := r.handleUsage(ctx, quota429Event(t, id)); err != nil {
			t.Fatal(err)
		}
		assertManagedProjection(t, h, id, true, -1)
	}
	r.host = &stickyFaultHost{reviewDiskHost: h, failSave: "b.json"}
	if err := r.scheduledInspection(ctx, false); err == nil {
		t.Fatal("expected b to fail")
	}
	state, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if state.Quota["a"].NeedsUnpause || state.Quota["b"].NeedsUnpause {
		t.Fatalf("wrong per-account phase after partial batch: a=%+v b=%+v", state.Quota["a"], state.Quota["b"])
	}
	assertManagedProjection(t, h, "a", false, -1)
	assertManagedProjection(t, h, "b", true, -1)
	stickySetAuth(t, h, "a", map[string]any{"disabled": true})
	restarted := newRuntime(h)
	if err := restarted.scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	assertManagedProjection(t, h, "a", true, -1)
	assertManagedProjection(t, h, "b", true, -1)
}

func TestLateQuota429OnPoolOwnedDisableSurvivesRestore(t *testing.T) {
	r, h := newPool429Fixture(t)
	ctx := context.Background()
	const id = "fixture-ag-00"
	pool429Send(t, r)
	if !r.poolLocked429() || r.state.Pool429Before[id].Disabled || !h.files[id].Disabled {
		t.Fatal("fixture must be enabled before the lock and disabled by its projection")
	}
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	gen := r.state.Pool429Generation
	if err := r.handleUsage(ctx, quota429Event(t, id)); err != nil {
		t.Fatal(err)
	}
	q := r.state.Quota[id]
	if !r.poolLocked429() || r.state.Pool429Generation != gen || !q.NeedsUnpause || q.RestUntil == nil || !q.RestUntil.Equal(now.Add(16*time.Hour)) {
		t.Fatalf("late quota event failed to persist rest while preserving the existing lock: %+v", q)
	}
	if _, err := r.restorePool429(ctx); err != nil {
		t.Fatal(err)
	}
	q = r.state.Quota[id]
	if r.poolLocked429() || h.files[id].Disabled || h.files[id].Priority != -1 || !q.ManagedRest || q.NeedsUnpause {
		t.Fatalf("restore failed to project phase two: %+v, file=%+v", q, h.files[id])
	}
}

func TestQuota429DoesNotEnableReplacementWithoutOwnershipMarker(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	ctx := context.Background()
	if err := r.handleUsage(ctx, quota429Event(t, "a")); err != nil {
		t.Fatal(err)
	}
	q := r.state.Quota["a"]
	oldDeadline := q.RestUntil.Add(-time.Nanosecond)
	stickySetAuth(t, h, "a", map[string]any{"disabled": true, "cpa_pause_marker": restPauseMarker(&oldDeadline)})
	if err := r.scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	assertManagedProjection(t, h, "a", true, -1)
	state, err := r.store.load()
	if err != nil || state.Quota["a"].NeedsUnpause {
		t.Fatalf("stale permission not revoked: %+v / %v", state.Quota["a"], err)
	}
	if err := newRuntime(h).scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	assertManagedProjection(t, h, "a", true, -1)
}

func TestRestoreNeverEnablesStaleManagedRestMarkerEvenWithPreviouslyEnabledSnapshot(t *testing.T) {
	r, h := newPool429Fixture(t)
	ctx := context.Background()
	const id = "fixture-ag-00"
	pool429Send(t, r)
	if err := r.handleUsage(ctx, quota429Event(t, id)); err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := json.Unmarshal(h.docs[id], &doc); err != nil {
		t.Fatal(err)
	}
	delete(doc, "cpa_pause_marker")
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.saveAuth(ctx, id+".json", raw); err != nil {
		t.Fatal(err)
	}
	before := *r.state.Quota[id].RestUntil
	if err := r.handleUsage(ctx, quota429Event(t, id)); err != nil {
		t.Fatal(err)
	}
	if !r.state.Quota[id].RestUntil.Equal(before) || string(h.docs[id]) != string(raw) {
		t.Fatal("late quota event must not renew a stale or missing marker under the pool lock")
	}
	if _, err := r.restorePool429(ctx); err != nil {
		t.Fatal(err)
	}
	if r.poolLocked429() || !h.files[id].Disabled || r.state.Quota[id].NeedsUnpause {
		t.Fatal("restore must exclude the replaced file rather than using its pre-lock enabled status")
	}
}

func TestRestoreRegrantsRestOnlyAfterRollingBackSuccessfulUnpause(t *testing.T) {
	r, h := newPool429Fixture(t)
	ctx := context.Background()
	const a, b = "fixture-ag-00", "fixture-ag-01"
	pool429Send(t, r)
	for _, id := range []string{a, b} {
		if err := r.handleUsage(ctx, quota429Event(t, id)); err != nil {
			t.Fatal(err)
		}
	}
	h.fail[b+".json"] = true
	if _, err := r.restorePool429(ctx); err == nil {
		t.Fatal("expected partial restore failure")
	}
	state, err := r.store.load()
	if err != nil || !state.PoolLocked429 || !state.Quota[a].NeedsUnpause || state.Quota[b].NeedsUnpause {
		t.Fatalf("rollback did not restore precisely the successful enable: a=%+v b=%+v err=%v", state.Quota[a], state.Quota[b], err)
	}
	if !h.files[a].Disabled || !h.files[b].Disabled {
		t.Fatal("rollback did not leave credentials disabled")
	}
	var rollbackDoc map[string]any
	if err := json.Unmarshal(h.docs[a], &rollbackDoc); err != nil || !ownsRestDisable(rollbackDoc, restPauseMarker(state.Quota[a].RestUntil)) {
		t.Fatal("successfully rolled-back credential lost its pause ownership marker")
	}
	delete(h.fail, b+".json")
	if _, err := r.restorePool429(ctx); err != nil {
		t.Fatal(err)
	}
	if r.poolLocked429() || h.files[a].Disabled || h.files[a].Priority != -1 || r.state.Quota[a].NeedsUnpause {
		t.Fatal("retry did not release the rolled-back credential into phase two")
	}
}

func TestLateQuota429DoesNotAdoptExternallyDisabled(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	r.state.PoolLocked429 = true
	r.state.Pool429Before = map[string]pool429Policy{"fixture": {Disabled: true, Priority: -1}}
	h.file.Disabled, h.file.Priority = true, -1
	original := string(h.document)
	if err := r.handleUsage(context.Background(), quota429Event(t)); err != nil {
		t.Fatal(err)
	}
	if !r.state.Pool429Before["fixture"].Disabled || r.state.Quota["fixture"].ManagedRest || string(h.document) != original {
		t.Fatal("late quota event gained auto-enable ownership of an externally disabled credential")
	}
}

func TestQuota429NullAuthDocumentDoesNotPanic(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	h.document = json.RawMessage(`null`)
	if err := r.handleUsage(context.Background(), quota429Event(t)); err == nil {
		t.Fatal("expected a controlled projection error")
	}
	assertNoQuotaRestIntent(t, r)
	if string(h.document) != "null" || r.poolLocked429() {
		t.Fatal("malformed document was rewritten or quota event tripped the lock")
	}
}

func TestQuota429ValidatesLiveCredentialBeforeRest(t *testing.T) {
	for _, tc := range []struct {
		name, document, filename, identity string
		wantError                          bool
	}{
		{"manual disable", `{"type":"antigravity","disabled":true}`, "fixture.json", "fixture", false},
		{"manual disable with tier marker", `{"type":"antigravity","disabled":true,"credential_tier_router":{"managed":true,"tier":"paused","reason":"quota_exhausted"}}`, "fixture.json", "fixture", false},
		{"failed get", `{}`, "fixture.json", "not-fixture", true},
		{"unsafe filename", `{}`, "nested/fixture.json", "fixture", true},
		{"wrong provider", `{"type":"codex"}`, "fixture.json", "fixture", true},
		{"ambiguous disabled", `{"disabled":"true"}`, "fixture.json", "fixture", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, h := newHTTP403HubRuntime(t)
			h.document, h.file.Name, h.file.AuthIndex = json.RawMessage(tc.document), tc.filename, tc.identity
			if err := r.handleUsage(context.Background(), quota429Event(t)); (err != nil) != tc.wantError {
				t.Fatalf("unexpected error: %v", err)
			}
			assertNoQuotaRestIntent(t, r)
			if string(h.document) != tc.document {
				t.Fatal("credential document was overwritten")
			}
		})
	}
}

func assertNoQuotaRestIntent(t *testing.T, r *runtime) {
	t.Helper()
	s, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if r.state.Quota["fixture"].ManagedRest || s.Quota["fixture"].ManagedRest || r.poolLocked429() {
		t.Fatal("invalid credential must not create rest ownership or lock the pool")
	}
}

func TestQuota429ClassifierUsesTypedDetails(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"native status", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}`, true},
		{"message only", `{"message":"QUOTA_EXHAUSTED"}`, false},
		{"missing type", `{"details":[{"reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"other detail type", `{"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"other reason", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED"}]}`, false},
		{"incomplete", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}`, false},
		{"duplicate reason", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"case variant collision", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","Reason":"RATE_LIMIT_EXCEEDED","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"competing details", `{"details":[],"error":` + fixtureQuota429 + `}`, false},
		{"duplicate detail objects", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"duplicate error field", `{"error":null,` + fixtureQuota429[1:], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if isGoogleQuotaExhausted(tc.body) != tc.want {
				t.Fatal("wrong classification")
			}
		})
	}
}
