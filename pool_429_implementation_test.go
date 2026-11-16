package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// Own implementation regressions. No private acceptance source is used.
type pool429FixtureHost struct {
	mu       sync.Mutex
	files    map[string]authFile
	docs     map[string]json.RawMessage
	fail     map[string]bool
	onSave   func(bool)
	httpSeen chan struct{}
	httpWait chan struct{}
	httpN    int
}

func newPool429Fixture(t *testing.T) (*runtime, *pool429FixtureHost) {
	t.Helper()
	h := &pool429FixtureHost{files: map[string]authFile{}, docs: map[string]json.RawMessage{}, fail: map[string]bool{}}
	cfg := defaultSettings()
	cfg.Providers = []string{"antigravity"}
	cfg.AutoApply = false
	r := &runtime{host: h, store: stateStore{path: filepath.Join(t.TempDir(), "state.json")}, state: persistedState{Settings: cfg, Quota: map[string]quotaSnapshot{}}, geoNow: time.Now}
	for i := 0; i <= 20; i++ {
		id, provider, priority, disabled := fmt.Sprintf("fixture-ag-%02d", i), "antigravity", 200, false
		if i < 4 {
			priority = 400
		}
		if i == 19 {
			disabled, priority = true, -1 // External disable must survive manual restore.
		}
		if i == 20 {
			id, provider, priority = "fixture-codex", "codex", 400
		}
		h.files[id] = authFile{Name: id + ".json", AuthIndex: id, Provider: provider, Priority: priority, Disabled: disabled}
		h.docs[id], _ = json.Marshal(map[string]any{"type": provider, "priority": priority, "disabled": disabled, "access_token": "fixture-current-token", "proxy_url": "fixture-proxy", "future_field": map[string]any{"keep": true}})
		remaining := 80
		r.state.Quota[id] = quotaSnapshot{Remaining: &remaining, Status: quotaReady, ObservedAt: time.Now().UTC()}
	}
	return r, h
}
func (h *pool429FixtureHost) listAuth(context.Context) ([]authFile, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	var files []authFile
	for _, f := range h.files {
		files = append(files, f)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].AuthIndex < files[j].AuthIndex })
	return files, nil
}
func (h *pool429FixtureHost) getAuth(_ context.Context, id string) (authDocument, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return authDocument{Name: h.files[id].Name, AuthIndex: id, JSON: append(json.RawMessage(nil), h.docs[id]...)}, nil
}
func (h *pool429FixtureHost) saveAuth(_ context.Context, name string, raw json.RawMessage) error {
	var p authFile
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	h.mu.Lock()
	if h.fail[name] {
		h.mu.Unlock()
		return errors.New("fixture save failure")
	}
	hook := h.onSave
	h.mu.Unlock()
	if hook != nil {
		hook(p.Disabled)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for id, f := range h.files {
		if f.Name == name {
			f.Priority, f.Disabled = p.Priority, p.Disabled
			h.files[id], h.docs[id] = f, append(json.RawMessage(nil), raw...)
			return nil
		}
	}
	return errors.New("fixture missing file")
}
func (h *pool429FixtureHost) httpDo(context.Context, hostHTTPRequest) (hostHTTPResponse, error) {
	h.mu.Lock()
	h.httpN++
	seen, wait := h.httpSeen, h.httpWait
	h.mu.Unlock()
	if seen != nil {
		select {
		case seen <- struct{}{}:
		default:
		}
	}
	if wait != nil {
		<-wait
	}
	return hostHTTPResponse{StatusCode: 200, Body: []byte(`{"groups":[{"displayName":"Gemini Models","buckets":[{"bucketId":"gemini-weekly","remainingFraction":0.8,"resetTime":"2099-01-01T00:00:00Z"}]}]}`)}, nil
}
func pool429AssertAllPaused(t *testing.T, r *runtime, h *pool429FixtureHost) {
	t.Helper()
	if !r.poolLocked429() {
		t.Fatal("global gate unexpectedly opened")
	}
	files, _ := h.listAuth(context.Background())
	for _, f := range files {
		if f.Provider == "antigravity" && (!f.Disabled || f.Priority != -1) {
			t.Fatalf("not hard-paused: %s", f.AuthIndex)
		}
	}
}
func pool429Send(t *testing.T, r *runtime) {
	t.Helper()
	if err := r.handleUsage(context.Background(), []byte(`{"Provider":" Antigravity ","Failed":true,"Failure":{"StatusCode":429}}`)); err != nil {
		t.Fatal(err)
	}
}
func pool429Request(t *testing.T, r *runtime, method, path string) managementResponse {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"Method": method, "Path": "/v0/management/plugins/credential-tier-router" + path, "Body": base64.StdEncoding.EncodeToString([]byte(`{}`))})
	response, err := r.handleManagement(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func TestPool429ImplementationLifecycle(t *testing.T) {
	r, h := newPool429Fixture(t)
	codex, _ := h.getAuth(context.Background(), "fixture-codex")
	original, _ := h.getAuth(context.Background(), "fixture-ag-00")
	pool429Send(t, r)
	pool429AssertAllPaused(t, r, h)
	state, err := r.store.load()
	if err != nil || !state.PoolLocked429 || state.Pool429Before["fixture-ag-00"].Priority != 400 || state.Quota["fixture-ag-00"].ManagedRest {
		t.Fatal("lock intent missing or incorrectly borrowed timed-rest ownership")
	}
	raw, _ := json.Marshal(state)
	if bytes.Contains(raw, []byte("fixture-current-token")) {
		t.Fatal("policy snapshot leaked credential material")
	}
	paused, _ := h.getAuth(context.Background(), "fixture-ag-00")
	var before, after map[string]any
	json.Unmarshal(original.JSON, &before)
	json.Unmarshal(paused.JSON, &after)
	delete(before, "priority")
	delete(before, "disabled")
	delete(after, "priority")
	delete(after, "disabled")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("non-policy auth fields changed")
	}
	// Simulate arbitrarily old lock and an expired ordinary403 rest: neither
	// periodic inspection, reconfigure, preview nor apply may unlock the pool.
	old := time.Now().UTC().Add(-72 * time.Hour)
	r.state.Pool429Since = &old
	q := r.state.Quota["fixture-ag-00"]
	q.RestUntil, q.ManagedRest = &old, true
	r.state.Quota["fixture-ag-00"] = q
	for _, auto := range []bool{false, true} {
		if err := r.scheduledInspection(context.Background(), auto); err != nil {
			t.Fatal(err)
		}
		pool429AssertAllPaused(t, r, h)
	}
	if _, err := r.run(context.Background(), false, "fixture preview"); err != nil {
		t.Fatal(err)
	}
	if err := r.configure(r.state.Settings); err != nil {
		t.Fatal(err)
	}
	defer r.shutdown()
	pool429Send(t, r)
	pool429AssertAllPaused(t, r, h)
	if h.httpN != 0 || r.state.Pool429Before["fixture-ag-00"].Disabled || r.state.Pool429Before["fixture-ag-00"].Priority != 400 {
		t.Fatal("locked pool was probed or repeated429 replaced original eligibility")
	}
	loaded, err := r.store.load()
	if err != nil || !loaded.PoolLocked429 {
		t.Fatal("reconfigure did not retain durable lock")
	}
	dashboard, err := r.dashboard(context.Background())
	if err != nil || !dashboard.PoolLocked429 {
		t.Fatal("dashboard omitted global alert state")
	}
	if response := pool429Request(t, r, "GET", "/pool/restore"); response.StatusCode != 404 {
		t.Fatal("GET must not restore")
	}
	// A normal owner refresh after locking must survive the later restore.
	h.mu.Lock()
	var fresh map[string]any
	json.Unmarshal(h.docs["fixture-ag-01"], &fresh)
	fresh["access_token"] = "fixture-refreshed-token"
	h.docs["fixture-ag-01"], _ = json.Marshal(fresh)
	h.mu.Unlock()
	response := pool429Request(t, r, "POST", "/pool/restore")
	if response.StatusCode != 200 || r.poolLocked429() {
		t.Fatalf("manual restore failed: %d %s", response.StatusCode, response.Body)
	}
	files, _ := h.listAuth(context.Background())
	for _, f := range files {
		if f.Provider == "antigravity" && f.AuthIndex != "fixture-ag-19" && (f.Disabled || (f.Priority != 400 && f.Priority != 200)) {
			t.Fatalf("healthy account not recovered: %+v", f)
		}
		if f.AuthIndex == "fixture-ag-19" && !f.Disabled {
			t.Fatal("external disable was lost")
		}
	}
	currentCodex, _ := h.getAuth(context.Background(), "fixture-codex")
	currentAG, _ := h.getAuth(context.Background(), "fixture-ag-01")
	if !bytes.Equal(currentCodex.JSON, codex.JSON) || !bytes.Contains(currentAG.JSON, []byte("fixture-refreshed-token")) {
		t.Fatal("Codex or refreshed auth material changed")
	}
	loaded, err = r.store.load()
	if err != nil || loaded.PoolLocked429 || loaded.Pool429Before != nil {
		t.Fatal("manual release was not persisted")
	}
}

func TestPool429ImplementationScopeAndScheduler(t *testing.T) {
	for _, tc := range []struct {
		provider string
		status   int
		failed   bool
		want     bool
	}{{"antigravity", 429, true, true}, {"antigravity", 429, false, false}, {"codex", 429, true, false}, {"gemini", 429, true, false}, {"antigravity", 503, true, false}} {
		t.Run(fmt.Sprintf("%s-%d-%v", tc.provider, tc.status, tc.failed), func(t *testing.T) {
			r, _ := newPool429Fixture(t)
			r.state.Settings.Providers, r.state.Settings.ActivePoolSize = []string{"codex"}, 0
			raw, _ := json.Marshal(usageEvent{Provider: tc.provider, Failed: tc.failed, Failure: usageFailure{StatusCode: tc.status}})
			if err := r.handleUsage(context.Background(), raw); err != nil || r.poolLocked429() != tc.want {
				t.Fatalf("unexpected trigger: locked=%v err=%v", r.poolLocked429(), err)
			}
			for _, provider := range []string{"antigravity", "codex"} {
				wire, _ := json.Marshal(map[string]any{"Provider": provider})
				var result struct {
					Result struct{ Handled, Reject bool }
				}
				if err := json.Unmarshal(r.handle(context.Background(), "scheduler.pick", wire), &result); err != nil || result.Result.Reject != (tc.want && provider == "antigravity") || result.Result.Handled != result.Result.Reject {
					t.Fatal("scheduler did not explicitly reject only the locked provider")
				}
			}
		})
	}
}

func TestPool429ImplementationFailureContinuesAndRestores(t *testing.T) {
	r, h := newPool429Fixture(t)
	h.fail["fixture-ag-03.json"] = true
	if err := r.tripPool429(context.Background()); err == nil || !r.poolLocked429() {
		t.Fatal("partial save failure was hidden or latch opened")
	}
	files, _ := h.listAuth(context.Background())
	for _, f := range files {
		if f.Provider == "antigravity" && f.AuthIndex != "fixture-ag-03" && (!f.Disabled || f.Priority != -1) {
			t.Fatal("one failure stopped remaining pool disables")
		}
	}
	if response := pool429Request(t, r, "POST", "/pool/restore"); response.StatusCode != 500 || !r.poolLocked429() {
		t.Fatal("partial failure falsely reported restored")
	}
	delete(h.fail, "fixture-ag-03.json")
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	pool429AssertAllPaused(t, r, h)
}

func TestPool429ImplementationSlowProbeCannotRecover(t *testing.T) {
	r, h := newPool429Fixture(t)
	h.httpSeen, h.httpWait = make(chan struct{}, 1), make(chan struct{})
	done := make(chan error, 1)
	go func() { _, err := r.run(context.Background(), true, "slow fixture"); done <- err }()
	select {
	case <-h.httpSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("probe never started")
	}
	trip := make(chan error, 1)
	go func() { trip <- r.tripPool429(context.Background()) }()
	select {
	case err := <-trip:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(h.httpWait)
		t.Fatal("429 trip waited for slow probe/runMu")
	}
	close(h.httpWait)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	pool429AssertAllPaused(t, r, h)
}

func TestPool429ImplementationNewArrivalsStayDisabled(t *testing.T) {
	r, h := newPool429Fixture(t)
	pool429Send(t, r)
	h.files["fixture-import"] = authFile{Name: "fixture-import.json", AuthIndex: "fixture-import", Provider: "antigravity", Priority: 400}
	h.docs["fixture-import"] = json.RawMessage(`{"type":"antigravity","priority":400,"disabled":false}`)
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	pool429AssertAllPaused(t, r, h)
	if _, err := r.restorePool429(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !h.files["fixture-import"].Disabled {
		t.Fatal("a new arrival was implicitly enabled by an older restore snapshot")
	}
}

func TestPool429ImplementationStateFailureKeepsGate(t *testing.T) {
	r, h := newPool429Fixture(t)
	r.store.path = t.TempDir() // Rename onto a directory fails even as root.
	if err := r.tripPool429(context.Background()); err == nil {
		t.Fatal("state persistence failure was hidden")
	}
	pool429AssertAllPaused(t, r, h)
}

func TestPool429ImplementationPendingTripKeepsOriginalEligibility(t *testing.T) {
	r, h := newPool429Fixture(t)
	for id, f := range h.files {
		if f.Provider == "antigravity" && !f.Disabled {
			f.Priority = 300 // Force the existing ordinary plan to write a batch.
			h.files[id] = f
			var root map[string]any
			json.Unmarshal(h.docs[id], &root)
			root["priority"] = 300
			h.docs[id], _ = json.Marshal(root)
		}
	}
	var once sync.Once
	trip := make(chan error, 1)
	h.onSave = func(disabled bool) {
		if disabled {
			return
		}
		once.Do(func() {
			go func() { trip <- r.tripPool429(context.Background()) }()
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				r.mu.Lock()
				pending := r.pool429Pending
				r.mu.Unlock()
				if pending {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Error("trip never latched behind the existing commit")
		})
	}
	if _, err := r.run(context.Background(), true, "ordinary batch"); err != nil {
		t.Fatal(err)
	}
	if err := <-trip; err != nil {
		t.Fatal(err)
	}
	pool429AssertAllPaused(t, r, h)
	if _, err := r.restorePool429(context.Background()); err != nil {
		t.Fatal(err)
	}
	for id, f := range h.files {
		if f.Provider == "antigravity" && id != "fixture-ag-19" && f.Disabled {
			t.Fatal("the pending guard polluted original eligibility with its own disable")
		}
	}
}

func TestPool429ImplementationNewTripDefeatsConcurrentRestore(t *testing.T) {
	r, h := newPool429Fixture(t)
	pool429Send(t, r)
	var once sync.Once
	trip := make(chan error, 1)
	h.onSave = func(disabled bool) {
		if disabled {
			return
		}
		once.Do(func() {
			go func() { trip <- r.tripPool429(context.Background()) }()
			deadline := time.Now().Add(3 * time.Second)
			for time.Now().Before(deadline) {
				r.mu.Lock()
				pending := r.pool429Pending
				r.mu.Unlock()
				if pending {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Error("concurrent trip did not latch")
		})
	}
	response := pool429Request(t, r, "POST", "/pool/restore")
	if response.StatusCode != 409 {
		t.Fatalf("concurrent429 lost: %d %s", response.StatusCode, response.Body)
	}
	if err := <-trip; err != nil {
		t.Fatal(err)
	}
	pool429AssertAllPaused(t, r, h)
}
