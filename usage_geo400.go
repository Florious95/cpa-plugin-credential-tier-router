package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"maps"
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

type restProjection struct {
	name             string
	original, paused json.RawMessage
}

// prepareRestProjection runs under commitMu and must succeed before a
// ManagedRest lease is written. A nil result means an external disable.
func (r *runtime) prepareRestProjection(ctx context.Context, index string, status int, priorID string, poolOwnsDisable bool, pauseID string) (*restProjection, error) {
	document, err := r.host.getAuth(ctx, index)
	if err != nil {
		return nil, fmt.Errorf("get credential %s for rest: %w", index, err)
	}
	name := strings.TrimSpace(document.Name)
	if !managedAuthFile(authFile{Name: name, Provider: "antigravity"}) || (document.AuthIndex != "" && document.AuthIndex != index) {
		return nil, fmt.Errorf("invalid rest credential identity or filename for %s", index)
	}
	var root map[string]any
	if err := json.Unmarshal(document.JSON, &root); err != nil {
		return nil, fmt.Errorf("decode credential %s for rest: %w", index, err)
	}
	if root == nil {
		return nil, fmt.Errorf("credential %s document is not a JSON object", index)
	}
	if kind, ok := root["type"]; ok && kind != "antigravity" {
		return nil, fmt.Errorf("credential %s is not Antigravity", index)
	}
	if flag, present := root["disabled"]; present {
		disabled, ok := flag.(bool)
		if !ok {
			return nil, fmt.Errorf("credential %s disabled field is not boolean", index)
		}
		if disabled && !ownsRestDisable(root, priorID) && (priorID != "" || !poolOwnsDisable) {
			return nil, nil
		}
	}
	root["priority"] = -1
	root["disabled"] = true
	root["cpa_pause_id"] = pauseID
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
	return &restProjection{name: name, original: bytes.Clone(document.JSON), paused: updated}, err
}

func newRestPauseID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

func ownsRestDisable(root map[string]any, expected string) bool {
	return expected != "" && root["cpa_pause_id"] == expected
}

func (r *runtime) readRestPauseMarkers(ctx context.Context, files []authFile, cache map[string]quotaSnapshot) {
	for i := range files {
		f := &files[i]
		f.RestPauseMarker, f.RestPauseReadFailed = "", false
		q := cache[f.AuthIndex]
		if !f.Disabled || providerOf(*f) != "antigravity" || !q.NeedsUnpause || !q.ManagedRest || q.RestUntil == nil {
			continue
		}
		doc, err := r.host.getAuth(ctx, f.AuthIndex)
		var root map[string]json.RawMessage
		if err != nil || (doc.AuthIndex != "" && doc.AuthIndex != f.AuthIndex) || json.Unmarshal(doc.JSON, &root) != nil || root == nil {
			f.RestPauseReadFailed = true
			continue // Leave this credential and its rest untouched; service the other accounts.
		}
		var marker string
		if json.Unmarshal(root["cpa_pause_id"], &marker) == nil && marker != "" && marker == q.PauseID {
			f.RestPauseMarker = marker
		}
	}
}

func eventRestUntil(quota quotaSnapshot, now time.Time, duration time.Duration, status int) time.Time {
	until := now.Add(duration)
	// Ordinary errors preserve an active rest; quota 429 guarantees at least 16 hours.
	if quota.RestUntil != nil && quota.RestUntil.After(now) &&
		(status != 429 || quota.RestUntil.After(until)) {
		until = *quota.RestUntil
	}
	return until
}

func (r *runtime) commitGeoRest(index string, intent restPauseIntent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	oldQuota, oldPending := r.state.Quota, r.state.PendingRestPauses
	if oldPending[index] != intent {
		return fmt.Errorf("pending rest intent changed for %s", index)
	}
	r.state.Quota, r.state.PendingRestPauses = cloneQuota(oldQuota), maps.Clone(oldPending)
	quota := oldQuota[index]
	quota.RestUntil, quota.ManagedRest, quota.NeedsUnpause, quota.PauseID = &intent.Until, true, true, intent.PauseID
	quota.ObservedAt = intent.At
	r.state.Quota[index] = quota
	delete(r.state.PendingRestPauses, index)
	if err := r.store.save(r.state); err != nil {
		r.state.Quota, r.state.PendingRestPauses = oldQuota, oldPending
		return fmt.Errorf("persist managed rest for %s: %w", index, err)
	}
	return nil
}

func (r *runtime) rollbackRestProjection(ctx context.Context, index string, projection *restProjection) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	current, err := r.host.getAuth(ctx, index)
	if err != nil {
		return fmt.Errorf("read credential %s for rest rollback: %w", index, err)
	}
	// Do not knowingly overwrite a concurrent manual edit or OAuth renewal.
	// The host exposes no compare-and-swap spanning this read and save.
	if strings.TrimSpace(current.Name) != projection.name || (current.AuthIndex != "" && current.AuthIndex != index) ||
		!sameRestDocument(current.JSON, projection.paused) {
		return fmt.Errorf("credential %s changed; rest rollback refused", index)
	}
	if err := r.host.saveAuth(ctx, projection.name, projection.original); err != nil {
		return fmt.Errorf("restore credential %s after rest ledger failure: %w", index, err)
	}
	return nil
}

func sameRestDocument(a, b []byte) bool {
	canonical := func(raw []byte) []byte {
		if !json.Valid(raw) {
			return nil
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var obj map[string]any
		if err := dec.Decode(&obj); err != nil || obj == nil {
			return nil
		}
		canonical, _ := json.Marshal(obj)
		return canonical
	}
	x := canonical(a)
	return x != nil && bytes.Equal(x, canonical(b))
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
	if r.loadErr != nil {
		return fmt.Errorf("load state: %w", r.loadErr)
	}
	if _, err := r.recoverRestPauseIntent(ctx, index); err != nil {
		return err
	}
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
	if restDuration == 0 {
		restDuration = time.Duration(cfg.Geo400RestHours) * time.Hour
	}
	until := eventRestUntil(q, now, restDuration, event.Failure.StatusCode)
	var priorID string
	if needsUnpause {
		priorID = q.PauseID
	}
	pauseID, err := newRestPauseID()
	if err != nil {
		return err
	}
	projection, err := r.prepareRestProjection(ctx, index, event.Failure.StatusCode, priorID, poolOwnsDisable, pauseID)
	if err != nil || projection == nil {
		return err
	}
	intent := restPauseIntent{PauseID: pauseID, Until: until, At: now}
	if err := r.applyRestPause(ctx, index, projection, intent); err != nil {
		return err
	}
	r.markLatestGeoPaused(event, until)
	r.recordHistory(trigger, 1, 0, fmt.Sprintf("已熔断并强制休眠至 %s；下次巡检解除禁用但保持暂停", until.Format(time.RFC3339)))
	if cfg.ActivePoolSize > 0 {
		// Leave this event's own credential hard-disabled until reconciliation.
		if _, err := r.commitPoolWithPause(ctx, nil, true, trigger+"补位", index); err != nil {
			r.recordHistory(trigger, 0, 1, "活跃池补位失败："+safeError(err))
		}
	}
	return r.persistState()
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
