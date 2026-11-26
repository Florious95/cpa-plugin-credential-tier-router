package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

// A pause and its renewals share the same write-ahead path.
func (r *runtime) applyRestPause(ctx context.Context, index string, projection *restProjection, intent restPauseIntent) error {
	if err := r.writeRestPauseIntent(index, intent); err != nil {
		return err
	}
	if err := r.host.saveAuth(ctx, projection.name, projection.paused); err != nil {
		verifyCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		committed, recoverErr := r.recoverRestPauseIntent(verifyCtx, index)
		if committed {
			return nil
		}
		return errors.Join(err, recoverErr)
	}
	if err := r.commitGeoRest(index, intent); err != nil {
		return errors.Join(err, r.rollbackRestProjection(ctx, index, projection))
	}
	return nil
}

// commitMu serializes the prepare/recover sequence with auth projections.
// An intent is durable before the host write, but never grants enablement.
func (r *runtime) writeRestPauseIntent(index string, intent restPauseIntent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.state.PendingRestPauses
	next := maps.Clone(old)
	if intent.PauseID == "" {
		delete(next, index)
	} else {
		if next == nil {
			next = make(map[string]restPauseIntent)
		}
		next[index] = intent
	}
	r.state.PendingRestPauses = next
	if err := r.store.save(r.state); err != nil {
		r.state.PendingRestPauses = old
		return fmt.Errorf("persist rest pause intent for %s: %w", index, err)
	}
	return nil
}

// recoverRestPauseIntent accepts only the exact still-pending UUID. A marker
// from an old backup or a previous completed pause cannot create a lease.
func (r *runtime) recoverRestPauseIntent(ctx context.Context, index string) (bool, error) {
	r.mu.Lock()
	intent, exists := r.state.PendingRestPauses[index]
	r.mu.Unlock()
	if !exists {
		return false, nil
	}
	if intent.PauseID == "" || intent.Until.IsZero() || intent.At.IsZero() {
		return false, fmt.Errorf("invalid pending rest intent for %s", index)
	}
	document, err := r.host.getAuth(ctx, index)
	if err != nil {
		return false, fmt.Errorf("read pending rest credential %s: %w", index, err)
	}
	var root map[string]any
	if (document.AuthIndex != "" && document.AuthIndex != index) ||
		!managedAuthFile(authFile{Name: document.Name, Provider: "antigravity"}) ||
		json.Unmarshal(document.JSON, &root) != nil || root == nil {
		return false, fmt.Errorf("invalid pending rest document for %s", index)
	}
	credentialType, _ := root["type"].(string)
	provider, _ := root["provider"].(string)
	if kind := firstText(credentialType, provider); kind != "" && !strings.EqualFold(kind, "antigravity") {
		return false, r.writeRestPauseIntent(index, restPauseIntent{})
	}
	if root["disabled"] != true || root["priority"] != float64(-1) || !ownsRestDisable(root, intent.PauseID) {
		return false, r.writeRestPauseIntent(index, restPauseIntent{})
	}
	if err := r.commitGeoRest(index, intent); err != nil {
		return false, err
	}
	return true, nil
}

func (r *runtime) recoverRestPauseIntents(ctx context.Context) {
	r.mu.Lock()
	pending := maps.Clone(r.state.PendingRestPauses)
	r.mu.Unlock()
	for index := range pending {
		if ctx.Err() != nil {
			return
		}
		_, _ = r.recoverRestPauseIntent(ctx, index) // Leave failed reads pending; don't block healthy accounts.
	}
}
