package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	egressCommandTimeout = 15 * time.Second
	geoAlertFilename     = "geo-400-alert.json"
)

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

func (r *runtime) pauseGeoCredential(ctx context.Context, event usageEvent) error {
	index := firstText(event.AuthIndex, event.AuthID)
	if index == "" {
		return fmt.Errorf("usage event has no credential identity")
	}
	document, err := r.host.getAuth(ctx, index)
	if err != nil {
		return fmt.Errorf("get geo-400 credential %s: %w", index, err)
	}
	var root map[string]any
	if err := json.Unmarshal(document.JSON, &root); err != nil {
		return fmt.Errorf("decode geo-400 credential %s: %w", index, err)
	}
	root["priority"] = -1
	root["disabled"] = true
	metadata := map[string]any{}
	if existing, ok := root["credential_tier_router"].(map[string]any); ok {
		metadata = existing
	}
	metadata["managed"] = true
	metadata["tier"] = string(tierPaused)
	metadata["reason"] = "geo_400"
	metadata["updated_at"] = time.Now().UTC().Format(time.RFC3339)
	root["credential_tier_router"] = metadata
	updated, err := json.Marshal(root)
	if err != nil {
		return fmt.Errorf("encode geo-400 credential %s: %w", index, err)
	}
	name := strings.TrimSpace(document.Name)
	if name == "" {
		return fmt.Errorf("geo-400 credential %s has no auth filename", index)
	}
	if err := r.host.saveAuth(ctx, name, updated); err != nil {
		return fmt.Errorf("save geo-400 credential %s: %w", index, err)
	}
	return nil
}

func (r *runtime) markGeoRest(event usageEvent, now time.Time, duration time.Duration) time.Time {
	index := firstText(event.AuthIndex, event.AuthID)
	if index == "" {
		return now
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Quota == nil {
		r.state.Quota = map[string]quotaSnapshot{}
	}
	quota := r.state.Quota[index]
	until := now.Add(duration)
	if quota.RestUntil != nil && quota.RestUntil.After(now) {
		until = *quota.RestUntil
	}
	quota.RestUntil = &until
	quota.ManagedRest = true
	quota.ObservedAt = now
	r.state.Quota[index] = quota
	return until
}

func (r *runtime) markLatestGeoPaused(event usageEvent, until time.Time) {
	index := firstText(event.AuthIndex, event.AuthID)
	if index == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.latest.Credentials {
		credential := &r.latest.Credentials[i]
		if credential.AuthIndex != index {
			continue
		}
		if credential.ProposedTier != tierPaused {
			r.latest.Changes++
		}
		credential.CurrentTier = tierPaused
		credential.ProposedTier = tierPaused
		credential.Reason = fmt.Sprintf("地区400，休眠至 %s", until.UTC().Format(time.RFC3339))
		credential.Changed = true
	}
}

func geoAlertPath(r *runtime) string {
	if path := strings.TrimSpace(os.Getenv("CREDENTIAL_TIER_ROUTER_GEO_ALERT_PATH")); path != "" {
		return path
	}
	return filepath.Join(filepath.Dir(r.store.path), geoAlertFilename)
}

func writeGeoAlert(r *runtime, event usageEvent, cfg settings, commandErr error) error {
	path := geoAlertPath(r)
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	record := map[string]any{
		"at":             time.Now().UTC(),
		"provider":       event.Provider,
		"auth_id":        event.AuthID,
		"auth_index":     event.AuthIndex,
		"body":           event.Failure.Body,
		"egress_command": cfg.EgressCommand,
		"egress_target":  cfg.EgressTarget,
		"command_error":  safeError(commandErr),
	}
	raw, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(directory, ".geo-400-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
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
	account := firstText(event.AuthIndex, event.AuthID)
	accountCount := r.recordGeoAccount(account, now, window)

	pauseErr := r.pauseGeoCredential(ctx, event)
	restUntil := r.markGeoRest(event, now, time.Duration(cfg.Geo400RestHours)*time.Hour)
	if pauseErr != nil {
		r.recordHistory("地区400", 0, 1, fmt.Sprintf("凭证 %s 命中地区限制，但即时降权失败：%s", firstText(event.AuthIndex, event.AuthID), safeError(pauseErr)))
	} else {
		r.markLatestGeoPaused(event, restUntil)
		r.recordHistory("地区400", 1, 0, fmt.Sprintf("凭证 %s 命中地区限制，已降权并休眠 %d 小时", firstText(event.AuthIndex, event.AuthID), cfg.Geo400RestHours))
	}
	if cfg.ActivePoolSize > 0 && pauseErr == nil {
		if err := r.reconcileActivePool(ctx, "地区400补位"); err != nil {
			r.recordHistory("地区400", 0, 1, "活跃池补位失败："+safeError(err))
		}
	}
	r.persistState()
	if !cfg.Geo400EgressEnabled || accountCount < cfg.Geo400AccountThreshold || !r.acceptGeoEgress(now, window) {
		return nil
	}
	if cfg.Geo400ReturnHours > 0 {
		r.scheduleEgressReturn(now.Add(time.Duration(cfg.Geo400ReturnHours) * time.Hour))
	}
	r.egressWG.Add(1)
	go func(invocation egressInvocation) {
		defer r.egressWG.Done()
		if err := invokeEgressCommand(context.Background(), cfg, invocation.Args[0]); err != nil {
			alertErr := writeGeoAlert(r, event, cfg, err)
			summary := "出口切换命令失败：" + safeError(err)
			if alertErr != nil {
				summary += "；告警文件写入失败：" + safeError(alertErr)
			}
			r.recordHistory("地区400", 0, 1, summary)
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

func (r *runtime) recordGeoAccount(account string, now time.Time, window time.Duration) int {
	account = strings.TrimSpace(account)
	if account == "" {
		return 0
	}
	r.geoMu.Lock()
	defer r.geoMu.Unlock()
	if r.geoAccounts == nil {
		r.geoAccounts = make(map[string]time.Time)
	}
	for knownAccount, at := range r.geoAccounts {
		if !at.Add(window).After(now) {
			delete(r.geoAccounts, knownAccount)
		}
	}
	r.geoAccounts[account] = now
	return len(r.geoAccounts)
}

func (r *runtime) acceptGeoEgress(now time.Time, window time.Duration) bool {
	r.geoMu.Lock()
	defer r.geoMu.Unlock()
	if !r.geoEgressAt.IsZero() && r.geoEgressAt.Add(window).After(now) {
		return false
	}
	r.geoEgressAt = now
	return true
}

func geoEventKey(event usageEvent) string {
	bodyHash := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(event.Failure.Body))))
	identity := firstText(event.AuthIndex, event.AuthID, "unknown")
	return identity + ":" + hex.EncodeToString(bodyHash[:])
}
