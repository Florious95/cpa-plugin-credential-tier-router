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

// prepareRestProjection runs under commitMu and must succeed before a
// ManagedRest lease is written. A nil payload means an external disable.
func (r *runtime) prepareRestProjection(ctx context.Context, index string, status int, needsUnpause, poolOwnsDisable bool) (string, []byte, error) {
	document, err := r.host.getAuth(ctx, index)
	if err != nil {
		return "", nil, fmt.Errorf("get credential %s for rest: %w", index, err)
	}
	name := strings.TrimSpace(document.Name)
	if !managedAuthFile(authFile{Name: name, Provider: "antigravity"}) || (document.AuthIndex != "" && document.AuthIndex != index) {
		return "", nil, fmt.Errorf("invalid rest credential identity or filename for %s", index)
	}
	var root map[string]any
	if err := json.Unmarshal(document.JSON, &root); err != nil {
		return "", nil, fmt.Errorf("decode credential %s for rest: %w", index, err)
	}
	if root == nil {
		return "", nil, fmt.Errorf("credential %s document is not a JSON object", index)
	}
	if kind, ok := root["type"]; ok && kind != "antigravity" {
		return "", nil, fmt.Errorf("credential %s is not Antigravity", index)
	}
	if flag, present := root["disabled"]; present {
		disabled, ok := flag.(bool)
		if !ok {
			return "", nil, fmt.Errorf("credential %s disabled field is not boolean", index)
		}
		if disabled {
			marker, _ := root["credential_tier_router"].(map[string]any)
			why, _ := marker["reason"].(string)
			restOwnsDisable := needsUnpause && marker["managed"] == true && marker["tier"] == string(tierPaused) &&
				(why == "geo_400" || why == "http_403" || why == "quota_exhausted")
			if !poolOwnsDisable && !restOwnsDisable {
				return "", nil, nil
			}
		}
	}
	root["priority"] = -1
	root["disabled"] = true
	reason := "geo_400"
	switch status {
	case 403:
		reason = "http_403"
	case 429:
		reason = "quota_exhausted"
	}
	root["credential_tier_router"] = map[string]any{
		"managed": true, "tier": string(tierPaused), "reason": reason,
		"updated_at": time.Now().UTC().Format(time.RFC3339),
	}
	updated, err := json.Marshal(root)
	return name, updated, err
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
	// Ordinary errors preserve an active rest; quota 429 guarantees at least 16 hours.
	if quota.RestUntil != nil && quota.RestUntil.After(now) &&
		(event.Failure.StatusCode != 429 || quota.RestUntil.After(until)) {
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
	if !event.Failed || !strings.EqualFold(strings.TrimSpace(event.Provider), "antigravity") {
		return nil
	}
	trigger := "地区400"
	var restDuration time.Duration
	switch event.Failure.StatusCode {
	case 429:
		if !isGoogleQuotaExhausted(event.Failure.Body) {
			return r.tripPool429(ctx)
		}
		trigger, restDuration = "额度429", 16*time.Hour
	case 403:
		trigger = "HTTP403"
	default:
		if !isAntigravityGeo400(event) {
			return nil
		}
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
	// A late event must not grant auto-enable ownership to a credential that
	// was externally disabled or absent from the full-pool lock snapshot.
	previous, known := r.state.Pool429Before[index]
	q := r.state.Quota[index]
	needsUnpause := q.ManagedRest && q.NeedsUnpause && q.RestUntil != nil
	poolOwnsDisable := r.state.PoolLocked429 && known && !previous.Disabled
	external := r.state.PoolLocked429 && (!known || (previous.Disabled && !needsUnpause))
	r.mu.Unlock()
	if external {
		return nil
	}
	name, projection, err := r.prepareRestProjection(ctx, index, event.Failure.StatusCode, needsUnpause, poolOwnsDisable)
	if err != nil || projection == nil {
		return err
	}
	if restDuration == 0 {
		restDuration = time.Duration(cfg.Geo400RestHours) * time.Hour
	}
	until := r.markGeoRest(event, now, restDuration)
	persistErr := r.persistState()
	// Attempt the safety disable even if state storage fails, but never admit
	// a replacement without durable recovery intent.
	pauseErr := r.host.saveAuth(ctx, name, projection)
	if pauseErr != nil {
		r.recordHistory(trigger, 0, 1, "即时熔断失败："+safeError(pauseErr))
	} else {
		// A durable rest deadline is not permission to undo a manual disable.
		// Grant that permission only after our hard-disable has succeeded.
		r.mu.Lock()
		q := r.state.Quota[index]
		q.NeedsUnpause = true
		r.state.Quota[index] = q
		r.mu.Unlock()
		if err := r.persistState(); persistErr == nil {
			persistErr = err
		}
		r.markLatestGeoPaused(event, until)
		r.recordHistory(trigger, 1, 0, fmt.Sprintf("已熔断并强制休眠至 %s；下次巡检解除禁用但保持暂停", until.Format(time.RFC3339)))
	}
	if cfg.ActivePoolSize > 0 && pauseErr == nil && persistErr == nil {
		// This event must leave its own credential hard-disabled. Only a later
		// reconciliation may release disabled while keeping the rest priority.
		if _, err := r.commitPoolWithPause(ctx, nil, true, trigger+"补位", index); err != nil {
			r.recordHistory(trigger, 0, 1, "活跃池补位失败："+safeError(err))
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
