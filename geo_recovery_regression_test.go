package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

// Regression red test for the reported trentboultjfjf75@gmail.com incident:
// after a two-hour managed Geo-400 rest expires, reconcile must persist the
// account's runtime eligibility even when the quota probe is still unknown.
func TestGeo400ManagedRestRecoveryAfterThreeHours(t *testing.T) {
	startedAt := time.Now().UTC().Add(-3 * time.Hour)
	restUntil := startedAt.Add(2 * time.Hour)
	simulatedNow := startedAt.Add(3 * time.Hour)
	if !simulatedNow.After(restUntil) {
		t.Fatalf("test clock did not pass rest deadline: now=%v rest_until=%v", simulatedNow, restUntil)
	}

	const (
		authIndex = "trentboultjfjf75@gmail.com"
		filename  = "trentboultjfjf75.json"
	)
	host := &fakeHost{
		files: []authFile{{
			AuthIndex: authIndex,
			Name:      filename,
			Provider:  "antigravity",
			Email:     authIndex,
			Priority:  -1,
			Disabled:  true,
		}},
		documents: map[string]authDocument{
			authIndex: {
				AuthIndex: authIndex,
				Name:      filename,
				JSON:      json.RawMessage(`{"email":"trentboultjfjf75@gmail.com","priority":-1,"disabled":true}`),
			},
		},
	}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.ActivePoolSize = 1
	r.state.Quota = map[string]quotaSnapshot{
		authIndex: {
			Status:      quotaUnknown,
			RestUntil:   &restUntil,
			ManagedRest: true,
			ObservedAt:  startedAt,
		},
	}

	if err := r.reconcileActivePool(context.Background(), "Geo-400 到期巡检"); err != nil {
		t.Fatal(err)
	}

	saved, ok := host.saved[filename]
	if !ok {
		t.Fatalf("expired managed rest was not written back: latest=%+v", r.latest)
	}
	var restored map[string]any
	if err := json.Unmarshal(saved, &restored); err != nil {
		t.Fatal(err)
	}
	if restored["disabled"] != false {
		t.Fatalf("expired managed rest left account disabled: %v", restored)
	}
	if priority := int(restored["priority"].(float64)); priority < 200 {
		t.Fatalf("expired managed rest did not restore healthy priority: %d", priority)
	}
	if len(r.latest.Credentials) != 1 || r.latest.Credentials[0].ProposedTier == tierPaused {
		t.Fatalf("expired managed rest remained paused in plan: %+v", r.latest)
	}
}
