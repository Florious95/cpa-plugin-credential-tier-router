package main

import (
	"context"
	"fmt"
	"time"
)

func invokeEgressCommand(ctx context.Context, cfg settings, target string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	commandCtx, cancel := context.WithTimeout(ctx, egressCommandTimeout)
	defer cancel()
	return runEgressCommand(commandCtx, egressInvocation{Command: cfg.EgressCommand, Args: []string{target}})
}

func (r *runtime) scheduleEgressReturn(at time.Time) {
	if at.IsZero() {
		return
	}
	r.mu.Lock()
	if r.state.EgressReturnAt == nil || at.Before(*r.state.EgressReturnAt) {
		r.state.EgressReturnAt = &at
	}
	r.egressRetryAt = nil
	r.mu.Unlock()
	_ = r.persistState()
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *runtime) returnEgress(ctx context.Context, trigger string) error {
	r.mu.Lock()
	cfg := normalizeSettings(r.state.Settings)
	r.mu.Unlock()
	err := invokeEgressCommand(ctx, cfg, cfg.EgressReturnTarget)
	r.mu.Lock()
	pending := r.state.EgressReturnAt != nil
	if err == nil {
		r.state.EgressReturnAt = nil
		r.egressRetryAt = nil
	} else if pending {
		retry := time.Now().UTC().Add(cfg.interval())
		r.egressRetryAt = &retry
	}
	r.mu.Unlock()
	if err != nil {
		r.recordHistory(trigger, 0, 1, "切回主路失败："+safeError(err))
	} else {
		r.recordHistory(trigger, 0, 0, "已切回主路出口 "+cfg.EgressReturnTarget)
	}
	if saveErr := r.persistState(); saveErr != nil && err == nil {
		return fmt.Errorf("save return state: %w", saveErr)
	}
	return err
}

func (r *runtime) maybeReturnEgress(ctx context.Context, now time.Time) error {
	r.mu.Lock()
	returnAt := r.state.EgressReturnAt
	r.mu.Unlock()
	if returnAt == nil || returnAt.After(now) {
		return nil
	}
	return r.returnEgress(ctx, "自动切回")
}

func (r *runtime) managementReturnEgress(ctx context.Context) (map[string]any, error) {
	if err := r.returnEgress(ctx, "手动切回"); err != nil {
		return nil, err
	}
	return map[string]any{"returned": true}, nil
}
