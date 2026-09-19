package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const egressCommandTimeout = 15 * time.Second

var runEgressCommand = func(ctx context.Context, invocation egressInvocation) error {
	return exec.CommandContext(ctx, invocation.Command, invocation.Args...).Run()
}

func isAntigravityGeo400(event usageEvent) bool {
	if !event.Failed || event.Failure.StatusCode != 400 || !strings.EqualFold(strings.TrimSpace(event.Provider), "antigravity") {
		return false
	}
	body := strings.ToLower(event.Failure.Body)
	return strings.Contains(body, "failed_precondition") && strings.Contains(body, "user location is not supported for the api use.")
}

func (r *runtime) handleUsage(ctx context.Context, raw []byte) error {
	var event usageEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("decode usage event: %w", err)
	}
	if !isAntigravityGeo400(event) {
		return nil
	}

	now := time.Now().UTC()
	if r.geoNow != nil {
		now = r.geoNow().UTC()
	}
	r.mu.Lock()
	cfg := normalizeSettings(r.state.Settings)
	r.mu.Unlock()
	window := time.Duration(cfg.Geo400DebounceMinutes) * time.Minute
	key := geoEventKey(event)
	if !r.acceptGeoEvent(key, now, window) {
		return nil
	}

	r.recordHistory("地区400", 0, 0, fmt.Sprintf("凭证 %s 命中地区限制，触发出口 %s", firstText(event.AuthIndex, event.AuthID), cfg.EgressTarget))
	r.persistState()
	r.egressWG.Add(1)
	go func(invocation egressInvocation) {
		defer r.egressWG.Done()
		commandCtx, cancel := context.WithTimeout(context.Background(), egressCommandTimeout)
		defer cancel()
		if err := runEgressCommand(commandCtx, invocation); err != nil {
			r.recordHistory("地区400", 0, 1, "出口切换命令失败："+safeError(err))
			r.persistState()
		}
	}(egressInvocation{Command: cfg.EgressCommand, Args: []string{cfg.EgressTarget}})
	return nil
}

func (r *runtime) persistState() {
	r.mu.Lock()
	state := r.state
	r.mu.Unlock()
	_ = r.store.save(state)
}

func (r *runtime) acceptGeoEvent(key string, now time.Time, window time.Duration) bool {
	r.geoMu.Lock()
	defer r.geoMu.Unlock()
	if r.geoEvents == nil {
		r.geoEvents = make(map[string]time.Time)
	}
	for knownKey, at := range r.geoEvents {
		if !at.Add(window).After(now) {
			delete(r.geoEvents, knownKey)
		}
	}
	if previous, ok := r.geoEvents[key]; ok && previous.Add(window).After(now) {
		return false
	}
	r.geoEvents[key] = now
	return true
}

func geoEventKey(event usageEvent) string {
	bodyHash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(event.Failure.Body))))
	identity := firstText(event.AuthIndex, event.AuthID, "unknown")
	return identity + ":" + hex.EncodeToString(bodyHash[:])
}
