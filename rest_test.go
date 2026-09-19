package main

import (
	"errors"
	"testing"
	"time"
)

func TestConfiguredRestAndEgressSettings(t *testing.T) {
	cfg, err := parsePluginConfig([]byte("rest_duration_hours: 20\negress_command: /opt/cycle\negress_target: to-2.5x\ngeo400_debounce_minutes: 7\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RestDurationHours != 20 || cfg.EgressCommand != "/opt/cycle" || cfg.EgressTarget != "to-2.5x" || cfg.Geo400DebounceMinutes != 7 {
		t.Fatalf("unexpected configured settings: %+v", cfg)
	}
}

func TestAntigravityRestDefaultDurationAndPausedPriority(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	quota := readyQuota(0, ptrTime(now.Add(time.Hour)), now)
	file := authFile{Provider: "antigravity"}

	if !applyAntigravityRest(&cfg, file, tierRegular, &quota, now) {
		t.Fatal("expected quota exhaustion to start a rest period")
	}
	wantUntil := now.Add(16 * time.Hour)
	if quota.RestUntil == nil || !quota.RestUntil.Equal(wantUntil) {
		t.Fatalf("RestUntil=%v, want %v", quota.RestUntil, wantUntil)
	}
	if got, reason := chooseTier(cfg, file, tierRegular, quota, now.Add(time.Hour)); got != tierPaused || reason == "" {
		t.Fatalf("active rest tier=(%s,%q), want paused with a reason", got, reason)
	}
	file.Unavailable = true
	if got, _ := chooseTier(cfg, file, tierRegular, quota, now.Add(time.Hour)); got != tierPaused {
		t.Fatalf("active rest with unavailable auth tier=%s, want paused", got)
	}
}

func TestAntigravityRestHoldsThroughEarlyQuotaResetThenReleases(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	cfg.RestDurationHours = 20
	quota := readyQuota(0, ptrTime(now.Add(time.Hour)), now)
	file := authFile{Provider: "antigravity"}
	if !applyAntigravityRest(&cfg, file, tierRegular, &quota, now) {
		t.Fatal("expected rest to start")
	}

	quota.Remaining = ptrInt(80)
	quota.Status = quotaReady
	early := now.Add(2 * time.Hour)
	if applyAntigravityRest(&cfg, file, tierPaused, &quota, early) {
		t.Fatal("early quota reset must not restart or shorten the active rest")
	}
	if got, _ := chooseTier(cfg, file, tierPaused, quota, early); got != tierPaused {
		t.Fatalf("early reset tier=%s, want paused", got)
	}

	late := now.Add(20 * time.Hour)
	if got, _ := chooseTier(cfg, file, tierPaused, quota, late); got != tierPrimary {
		t.Fatalf("expired rest tier=%s, want primary from current quota", got)
	}
	if applyAntigravityRest(&cfg, file, tierPaused, &quota, late) {
		t.Fatal("expired rest with recovered quota must not restart")
	}
	if quota.RestUntil != nil {
		t.Fatalf("expired RestUntil was not cleared: %v", quota.RestUntil)
	}
}

func TestFailedQuotaPreservesActiveAntigravityRest(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	until := now.Add(16 * time.Hour)
	previous := readyQuota(80, ptrTime(now.Add(time.Hour)), now)
	previous.RestUntil = &until
	failed := failedQuota(previous, errors.New("temporary"), 3, now.Add(time.Minute))
	if failed.RestUntil == nil || !failed.RestUntil.Equal(until) {
		t.Fatalf("failed quota lost active RestUntil: %v", failed.RestUntil)
	}
}

func TestAntigravityPausedCredentialStartsRestAndOtherProvidersDoNot(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	positive := readyQuota(80, ptrTime(now.Add(time.Hour)), now)
	if !applyAntigravityRest(&cfg, authFile{Provider: "antigravity"}, tierPaused, &positive, now) {
		t.Fatal("a newly paused Antigravity credential should start rest")
	}
	if got := positive.RestUntil.Sub(now); got != 16*time.Hour {
		t.Fatalf("paused rest duration=%v, want 16h", got)
	}

	codex := readyQuota(0, ptrTime(now.Add(time.Hour)), now)
	if applyAntigravityRest(&cfg, authFile{Provider: "codex"}, tierRegular, &codex, now) {
		t.Fatal("non-Antigravity credentials must not enter the Antigravity rest policy")
	}
}

func ptrInt(value int) *int { return &value }
