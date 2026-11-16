package main

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// Implementation-side regressions; independent of the private acceptance suite.
type http403HubHost struct {
	file     authFile
	document json.RawMessage
	requests []hostHTTPRequest
}

func newHTTP403HubRuntime(t *testing.T) (*runtime, *http403HubHost) {
	t.Helper()
	h := &http403HubHost{
		file:     authFile{Name: "fixture.json", AuthIndex: "fixture", Provider: "antigravity", Priority: 400},
		document: json.RawMessage(`{"type":"antigravity","priority":400,"disabled":false,"preserved":"keep"}`),
	}
	cfg := defaultSettings()
	cfg.AutoApply, cfg.ActivePoolSize = false, 0
	return &runtime{host: h, store: stateStore{path: filepath.Join(t.TempDir(), "state.json")}, state: persistedState{Settings: cfg, Quota: map[string]quotaSnapshot{}}}, h
}

func (h *http403HubHost) listAuth(context.Context) ([]authFile, error) {
	return []authFile{h.file}, nil
}
func (h *http403HubHost) getAuth(_ context.Context, index string) (authDocument, error) {
	if index != h.file.AuthIndex {
		return authDocument{}, fmt.Errorf("unexpected fixture identity")
	}
	return authDocument{Name: h.file.Name, AuthIndex: index, JSON: h.document}, nil
}
func (h *http403HubHost) saveAuth(_ context.Context, name string, raw json.RawMessage) error {
	if name != h.file.Name {
		return fmt.Errorf("unexpected fixture filename")
	}
	var projection authFile
	if err := json.Unmarshal(raw, &projection); err != nil {
		return err
	}
	h.file.Priority, h.file.Disabled = projection.Priority, projection.Disabled
	h.document = append(json.RawMessage(nil), raw...)
	return nil
}
func (h *http403HubHost) httpDo(_ context.Context, request hostHTTPRequest) (hostHTTPResponse, error) {
	h.requests = append(h.requests, request)
	if request.Headers["User-Agent"][0] != "antigravity/hub/2.23.0 darwin/arm64" {
		return hostHTTPResponse{}, fmt.Errorf("old quota user agent")
	}
	return hostHTTPResponse{StatusCode: 200, Body: []byte(`{"groups":[{"displayName":"Gemini Models","buckets":[{"bucketId":"gemini-weekly","remainingFraction":0.8,"resetTime":"2099-01-01T00:00:00Z"}]}]}`)}, nil
}

func TestFixHTTP403RestAndRecovery(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	event := []byte(`{"Provider":"antigravity","AuthIndex":"fixture","Failed":true,"Failure":{"StatusCode":403,"Body":"PERMISSION_DENIED"}}`)
	if err := r.handleUsage(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	q := r.state.Quota["fixture"]
	if !h.file.Disabled || h.file.Priority != -1 || !q.ManagedRest || q.RestUntil == nil || !q.RestUntil.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("missing immediate managed rest: disabled=%v priority=%d quota=%+v", h.file.Disabled, h.file.Priority, q)
	}
	var document map[string]any
	if err := json.Unmarshal(h.document, &document); err != nil {
		t.Fatal(err)
	}
	if document["preserved"] != "keep" || document["credential_tier_router"].(map[string]any)["reason"] != "http_403" {
		t.Fatal("credential metadata was lost or mislabeled")
	}
	deadline := *q.RestUntil
	r.geoNow = func() time.Time { return now.Add(time.Minute) }
	if err := r.handleUsage(context.Background(), event); err != nil || !r.state.Quota["fixture"].RestUntil.Equal(deadline) {
		t.Fatalf("duplicate403 extended rest: %v", err)
	}
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if h.file.Disabled || h.file.Priority != -1 || !r.state.Quota["fixture"].RestUntil.Equal(deadline) || len(h.requests) != 0 {
		t.Fatal("next inspection must release only disabled, without probing or ending rest")
	}
	expired := time.Now().UTC().Add(-time.Second)
	q = r.state.Quota["fixture"]
	q.RestUntil = &expired
	r.state.Quota["fixture"] = q
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	q = r.state.Quota["fixture"]
	if h.file.Disabled || h.file.Priority != tierRegular.priority() || q.ManagedRest || q.RestUntil != nil {
		t.Fatal("expired rest did not follow existing recovery")
	}
}

func TestFixHTTP403BreakerScope(t *testing.T) {
	for _, tc := range []struct {
		name, provider, body string
		status               int
		failed, want         bool
	}{
		{"empty403", "antigravity", "", 403, true, true},
		{"normalized403", " Antigravity ", "VALIDATION_REQUIRED", 403, true, true},
		{"tos403", "antigravity", "TOS_VIOLATION", 403, true, true},
		{"geo400", "antigravity", "FAILED_PRECONDITION User location is not supported for the API use.", 400, true, true},
		{"other400", "antigravity", "bad request", 400, true, false},
		{"otherProvider", "codex", "", 403, true, false},
		{"notFailed", "antigravity", "", 403, false, false},
		{"unauthorized", "antigravity", "", 401, true, false},
		{"quotaPoolLock", "antigravity", "", 429, true, true},
		{"server", "antigravity", "", 503, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, h := newHTTP403HubRuntime(t)
			r.state.Settings.Geo400RestHours = 7
			now := time.Now().UTC()
			r.geoNow = func() time.Time { return now }
			event := usageEvent{Provider: tc.provider, AuthIndex: "fixture", Failed: tc.failed, Failure: usageFailure{StatusCode: tc.status, Body: tc.body}}
			raw, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if err := r.handleUsage(context.Background(), raw); err != nil {
				t.Fatal(err)
			}
			if h.file.Disabled != tc.want {
				t.Fatalf("disabled=%v want=%v", h.file.Disabled, tc.want)
			}
			if tc.status == 429 && (!r.state.PoolLocked429 || r.state.Quota["fixture"].ManagedRest) {
				t.Fatal("429 must use the indefinite pool lock, not timed403 rest")
			}
			if tc.want && tc.status != 429 && !r.state.Quota["fixture"].RestUntil.Equal(now.Add(7*time.Hour)) {
				t.Fatal("breaker did not reuse geo400_rest_hours")
			}
		})
	}
}

func TestFixHubQuotaRequest(t *testing.T) {
	_, h := newHTTP403HubRuntime(t)
	q, err := probeAntigravity(context.Background(), h, "fixture", authMaterial{AccessToken: "fixture-not-a-credential", ProjectID: "fixture-project"}, "gemini", time.Now())
	if err != nil || q.Status != quotaReady || q.Remaining == nil || *q.Remaining != 80 || len(h.requests) != 1 {
		t.Fatalf("Hub quota request failed: quota=%+v error=%v requests=%d", q, err, len(h.requests))
	}
	request := h.requests[0]
	if request.AuthIndex != "fixture" || request.Method != "POST" || request.URL != antigravityQuotaURLs[0] || request.Headers["Authorization"][0] != "Bearer fixture-not-a-credential" || string(request.Body) != `{"project":"fixture-project"}` {
		t.Fatal("unrelated quota request semantics changed")
	}
}
