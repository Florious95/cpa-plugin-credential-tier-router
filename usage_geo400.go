package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

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
	root["credential_tier_router"] = map[string]any{
		"managed": true, "tier": string(tierPaused), "reason": "geo_400",
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	}
	updated, err := json.Marshal(root)
	if err != nil {
		return err
	}
	name := strings.TrimSpace(document.Name)
	if name == "" {
		return fmt.Errorf("geo-400 credential %s has no auth filename", index)
	}
	return r.host.saveAuth(ctx, name, updated)
}

func (r *runtime) markGeoRest(event usageEvent, now time.Time, duration time.Duration) time.Time {
	index := firstText(event.AuthIndex, event.AuthID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state.Quota == nil {
		r.state.Quota = map[string]quotaSnapshot{}
	}
	quota := r.state.Quota[index]
	until := now.Add(duration)
	// Repeated errors do not extend an already established rest deadline.
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
		credential.Disabled = true
		credential.ProposedDisabled = true
		credential.Quota = r.state.Quota[index]
		credential.Reason = fmt.Sprintf("强制休眠至 %s", until.UTC().Format(time.RFC3339))
		credential.Changed = true
	}
}

func (r *runtime) handleUsage(ctx context.Context, raw []byte) error {
	var event usageEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return fmt.Errorf("decode usage event: %w", err)
	}
	if !isAntigravityGeo400(event) {
		return nil
	}
	index := firstText(event.AuthIndex, event.AuthID)
	if index == "" {
		return fmt.Errorf("usage event has no credential identity")
	}
	now := time.Now().UTC()
	if r.geoNow != nil {
		now = r.geoNow().UTC()
	}
	// Probes never hold this fence: a slow upstream cannot delay the breaker.
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	r.mu.Lock()
	cfg := normalizeSettings(r.state.Settings)
	r.mu.Unlock()
	until := r.markGeoRest(event, now, time.Duration(cfg.Geo400RestHours)*time.Hour)
	persistErr := r.persistState()
	// Attempt the safety disable even if state storage fails, but never admit
	// a replacement without durable recovery intent.
	pauseErr := r.pauseGeoCredential(ctx, event)
	if pauseErr != nil {
		r.recordHistory("地区400", 0, 1, "即时熔断失败："+safeError(pauseErr))
	} else {
		r.markLatestGeoPaused(event, until)
		r.recordHistory("地区400", 1, 0, fmt.Sprintf("已熔断并强制休眠至 %s；下次巡检解除禁用但保持暂停", until.Format(time.RFC3339)))
	}
	if cfg.ActivePoolSize > 0 && pauseErr == nil && persistErr == nil {
		// This event must leave its own credential hard-disabled. Only a later
		// reconciliation may release disabled while keeping the rest priority.
		if _, err := r.commitPoolWithPause(ctx, nil, true, "地区400补位", index); err != nil {
			r.recordHistory("地区400", 0, 1, "活跃池补位失败："+safeError(err))
		}
	}
	saveErr := r.persistState()
	if persistErr != nil {
		return persistErr
	}
	if pauseErr != nil {
		return pauseErr
	}
	return saveErr
}

// Serialize snapshot capture, JSON encoding and rename with all mutations.
func (r *runtime) persistState() error {
	if r.loadErr != nil {
		return fmt.Errorf("load state: %w", r.loadErr)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.store.save(r.state)
}
