package main

// Independent reviewer regression tests. No real credentials, network or egress commands.
import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

type reviewDiskHost struct {
	mu        sync.Mutex
	dir       string
	responses map[string]hostHTTPResponse
	hook      func(hostHTTPRequest)
}

func (h *reviewDiskHost) listAuth(context.Context) ([]authFile, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	entries, err := os.ReadDir(h.dir)
	if err != nil {
		return nil, err
	}
	var files []authFile
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(h.dir, entry.Name()))
		if err != nil {
			return nil, err
		}
		var f authFile
		if err = json.Unmarshal(raw, &f); err != nil {
			return nil, err
		}
		f.Name = entry.Name()
		files = append(files, f)
	}
	return files, nil
}
func (h *reviewDiskHost) getAuth(_ context.Context, index string) (authDocument, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	raw, err := os.ReadFile(filepath.Join(h.dir, index+".json"))
	return authDocument{AuthIndex: index, Name: index + ".json", JSON: raw}, err
}
func (h *reviewDiskHost) saveAuth(_ context.Context, name string, raw json.RawMessage) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return os.WriteFile(filepath.Join(h.dir, name), raw, 0600)
}
func (h *reviewDiskHost) httpDo(_ context.Context, req hostHTTPRequest) (hostHTTPResponse, error) {
	if h.hook != nil {
		h.hook(req)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	response, ok := h.responses[req.AuthIndex]
	if !ok {
		return hostHTTPResponse{}, errors.New("review injected quota probe failure")
	}
	return response, nil
}
func (h *reviewDiskHost) quota(index string, remaining int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.responses[index] = hostHTTPResponse{StatusCode: 200, Body: []byte(fmt.Sprintf(`{"buckets":[{"modelId":"gemini-test","remainingFraction":%.2f,"resetTime":%q}]}`, float64(remaining)/100, time.Now().Add(time.Hour).UTC().Format(time.RFC3339)))}
}
func reviewRuntime(t *testing.T, n int) (*runtime, *reviewDiskHost) {
	t.Helper()
	dir := t.TempDir()
	t.Setenv("CREDENTIAL_TIER_ROUTER_STATE_PATH", filepath.Join(dir, "state.json"))
	h := &reviewDiskHost{dir: filepath.Join(dir, "auth"), responses: map[string]hostHTTPResponse{}}
	if err := os.Mkdir(h.dir, 0700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		id := string(rune('a' + i))
		priority := 400
		if i >= 4 {
			priority = 200
		}
		raw, _ := json.Marshal(map[string]any{"auth_index": id, "provider": "antigravity", "priority": priority, "disabled": false, "access_token": "TEST_ONLY"})
		if err := h.saveAuth(context.Background(), id+".json", raw); err != nil {
			t.Fatal(err)
		}
		h.quota(id, 90-i*10)
	}
	r := newRuntime(h)
	r.state.Settings.AutoApply = true
	return r, h
}
func reviewRun(t *testing.T, r *runtime) {
	t.Helper()
	if _, err := r.run(context.Background(), true, "review"); err != nil {
		t.Fatal(err)
	}
}
func reviewAuth(t *testing.T, h *reviewDiskHost, index string) authFile {
	t.Helper()
	d, err := h.getAuth(context.Background(), index)
	if err != nil {
		t.Fatal(err)
	}
	var f authFile
	if err = json.Unmarshal(d.JSON, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func reviewGeo(index string) []byte {
	raw, _ := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: index, Failed: true, Failure: usageFailure{StatusCode: 400, Body: "FAILED_PRECONDITION User location is not supported for the API use."}})
	return raw
}
func reviewExpire(r *runtime, index string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.state.Quota[index]
	past := time.Now().Add(-time.Minute)
	q.RestUntil = &past
	r.state.Quota[index] = q
}

func TestReviewRestOwnershipSurvivesProbeAndRecovers(t *testing.T) {
	for _, source := range []string{"quota_zero", "geo400"} {
		t.Run(source, func(t *testing.T) {
			r, h := reviewRuntime(t, 1)
			if source == "quota_zero" {
				h.quota("a", 0)
				reviewRun(t, r)
			} else {
				if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
					t.Fatal(err)
				}
			}
			original := r.state.Quota["a"]
			if !original.ManagedRest || original.RestUntil == nil || !reviewAuth(t, h, "a").Disabled {
				t.Fatalf("setup did not pause account: %+v", original)
			}
			h.quota("a", 80)
			reviewRun(t, r)
			during := r.state.Quota["a"]
			if during.RestUntil == nil || !during.RestUntil.Equal(*original.RestUntil) {
				t.Fatal("deadline moved")
			}
			t.Logf("after repeated successful probe: managed_rest=%v; original deadline preserved=%v", during.ManagedRest, during.RestUntil.Equal(*original.RestUntil))
			reviewExpire(r, "a")
			reviewRun(t, r)
			after := reviewAuth(t, h, "a")
			if after.Disabled {
				t.Fatalf("rest expired with latest quota=80 but disk disabled=%v priority=%d; managed_rest was lost on intermediate probe", after.Disabled, after.Priority)
			}
		})
	}
}

func TestReviewPoolCapAfterProbeFailures(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	h.mu.Lock()
	delete(h.responses, "a")
	h.mu.Unlock()
	for i := 0; i < 3; i++ {
		reviewRun(t, r)
	}
	files, err := h.listAuth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var primary []string
	for _, f := range files {
		if !f.Disabled && f.Priority == 400 {
			primary = append(primary, f.AuthIndex)
		}
	}
	sort.Strings(primary)
	if len(primary) > 4 {
		t.Fatalf("cap=4 but enabled priority400 auths=%v; a quota=%+v", primary, r.state.Quota["a"])
	}
}

func TestReviewExistingSoftPauseGetsHardDisabled(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	doc, _ := h.getAuth(context.Background(), "a")
	var root map[string]any
	_ = json.Unmarshal(doc.JSON, &root)
	root["priority"] = -1
	raw, _ := json.Marshal(root)
	if err := h.saveAuth(context.Background(), "a.json", raw); err != nil {
		t.Fatal(err)
	}
	h.quota("a", 0)
	reviewRun(t, r)
	f := reviewAuth(t, h, "a")
	if !f.Disabled {
		t.Fatalf("legacy paused account remains runtime eligible: priority=%d disabled=%v RestUntil=%v; plan.Changes=%d", f.Priority, f.Disabled, r.state.Quota["a"].RestUntil, r.latest.Changes)
	}
}

func TestReviewNewManualPauseRecordsRest(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	r.state.Settings.Strategy = strategyManual
	r.state.Settings.ManualTiers["a"] = tierPaused
	reviewRun(t, r)
	q := r.state.Quota["a"]
	f := reviewAuth(t, h, "a")
	if q.RestUntil == nil {
		t.Fatalf("new manual pause wrote priority=%d disabled=%v but no RestUntil; expected configured 16h hold", f.Priority, f.Disabled)
	}
}

func TestReviewExpiredRestRestoresRegularWithoutCap(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	r.state.Settings.ActivePoolSize = 0
	h.quota("a", 0)
	reviewRun(t, r)
	reviewExpire(r, "a")
	h.quota("a", 35)
	reviewRun(t, r)
	f := reviewAuth(t, h, "a")
	if f.Disabled {
		t.Fatalf("expired managed rest latest quota=35; plan current=%s proposed=%s changes=%d but disk priority=%d disabled=%v", r.latest.Credentials[0].CurrentTier, r.latest.Credentials[0].ProposedTier, r.latest.Changes, f.Priority, f.Disabled)
	}
}

func TestReviewFailedReturnRetainsPendingRecovery(t *testing.T) {
	r, _ := reviewRuntime(t, 0)
	previous := runEgressCommand
	defer func() { runEgressCommand = previous }()
	calls := 0
	runEgressCommand = func(context.Context, egressInvocation) error {
		calls++
		if calls == 1 {
			return errors.New("temporary switch command failure")
		}
		return nil
	}
	r.state.EgressReturnAt = ptrTime(time.Now().Add(-time.Minute))
	if err := r.maybeReturnEgress(context.Background(), time.Now()); err == nil {
		t.Fatal("expected injected failure")
	}
	if err := r.maybeReturnEgress(context.Background(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("return command failed once; pending deadline=%v calls after next due check=%d; no automatic recovery remains", r.state.EgressReturnAt, calls)
	}
}

func TestReviewGeoWindowCountsLatestSameBodyFailures(t *testing.T) {
	r, _ := reviewRuntime(t, 2)
	r.state.Settings.Geo400EgressEnabled = true
	previous := runEgressCommand
	defer func() { runEgressCommand = previous }()
	calls := 0
	runEgressCommand = func(context.Context, egressInvocation) error { calls++; return nil }
	start := time.Now().UTC()
	now := start
	r.geoNow = func() time.Time { return now }
	for _, event := range []struct {
		after time.Duration
		index string
	}{{0, "a"}, {4 * time.Minute, "a"}, {6 * time.Minute, "b"}} {
		now = start.Add(event.after)
		if err := r.handleUsage(context.Background(), reviewGeo(event.index)); err != nil {
			t.Fatal(err)
		}
		r.egressWG.Wait()
	}
	if calls != 1 {
		t.Fatalf("a@0m,a@4m,b@6m same region body; last 5m contains a and b but switch calls=%d; geoAccounts=%v", calls, r.geoAccounts)
	}
}

func TestReviewQuotaRunDoesNotOverwriteConcurrentGeoPause(t *testing.T) {
	for _, cap := range []int{4, 0} {
		t.Run(fmt.Sprintf("cap_%d", cap), func(t *testing.T) { reviewConcurrentGeoPause(t, cap) })
	}
}

func reviewConcurrentGeoPause(t *testing.T, cap int) {
	r, h := reviewRuntime(t, 1)
	r.state.Settings.ActivePoolSize = cap
	// Start from Backup so a quota run plans a write back to Primary.
	doc, _ := h.getAuth(context.Background(), "a")
	var root map[string]any
	_ = json.Unmarshal(doc.JSON, &root)
	root["priority"] = 200
	raw, _ := json.Marshal(root)
	if err := h.saveAuth(context.Background(), "a.json", raw); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	h.hook = func(hostHTTPRequest) { once.Do(func() { close(entered); <-release }) }
	runDone := make(chan error, 1)
	go func() { _, err := r.run(context.Background(), true, "concurrent review"); runDone <- err }()
	<-entered
	geoDone := make(chan error, 1)
	go func() { geoDone <- r.handleUsage(context.Background(), reviewGeo("a")) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.mu.Lock()
		q := r.state.Quota["a"]
		r.mu.Unlock()
		if q.ManagedRest && reviewAuth(t, h, "a").Disabled {
			break
		}
		if time.Now().After(deadline) {
			close(release)
			t.Fatal("geo pause was not recorded")
		}
		time.Sleep(time.Millisecond)
	}
	close(release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if err := <-geoDone; err != nil {
		t.Fatal(err)
	}
	q := r.state.Quota["a"]
	f := reviewAuth(t, h, "a")
	if q.RestUntil == nil {
		t.Errorf("in-flight quota run overwrote newly created 2h geo rest: quota=%+v", q)
	}
	if !f.Disabled {
		t.Errorf("in-flight quota run undid geo hard disable: priority=%d disabled=%v", f.Priority, f.Disabled)
	}
}
