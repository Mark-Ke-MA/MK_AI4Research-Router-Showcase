// Package activity manages best-effort "accepted and not yet terminal" UI
// indicators. Platform failure never changes the durable job or outbox.
package activity

import (
	"context"
	"time"

	"codex-persona-router/internal/model"
)

type Store interface {
	Activities() ([]model.Activity, error)
	ActivityAttempt(jobID, state string, next time.Time) error
	ActivityActive(jobID string, reactionID []byte, next time.Time) error
	ActivityRetry(jobID string, next time.Time) error
	ActivityPause(jobID string) error
	ActivityDone(jobID string) error
}

type Telegram interface {
	Typing(context.Context, []byte) error
}

type Feishu interface {
	CreateReaction(context.Context, []byte) ([]byte, error)
	DeleteReaction(context.Context, []byte, []byte) error
	FindOwnReaction(context.Context, []byte) ([]byte, bool, error)
}

type Manager struct {
	Store              Store
	Telegram           Telegram
	Feishu             Feishu
	Tick               time.Duration
	TelegramKeepalive  time.Duration
	CallTimeout        time.Duration
	CleanupRetention   time.Duration
	GracefulStopBudget time.Duration
	Now                func() time.Time
}

func (m *Manager) defaults() {
	if m.Tick <= 0 {
		m.Tick = 100 * time.Millisecond
	}
	if m.TelegramKeepalive <= 0 {
		m.TelegramKeepalive = 4 * time.Second
	}
	if m.CallTimeout <= 0 {
		m.CallTimeout = 3 * time.Second
	}
	if m.CleanupRetention <= 0 {
		m.CleanupRetention = 24 * time.Hour
	}
	if m.GracefulStopBudget <= 0 {
		m.GracefulStopBudget = 5 * time.Second
	}
	if m.Now == nil {
		m.Now = time.Now
	}
}

func (m *Manager) Run(ctx context.Context) {
	m.defaults()
	m.reconcile(ctx, false)
	ticker := time.NewTicker(m.Tick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.FinalizeDrain()
			return
		case <-ticker.C:
			m.reconcile(ctx, false)
		}
	}
}

// FinalizeDrain performs one bounded, idempotent drain reconciliation. The
// router calls it once when intake stops (through Run cancellation) and again
// after workers/outbox have persisted their final states. The second pass
// deletes locally retained rows that became terminal during the drain, while
// recoverable uncertainty remains paused for explicit reconciliation.
func (m *Manager) FinalizeDrain() {
	m.defaults()
	stopCtx, cancel := context.WithTimeout(context.Background(), m.GracefulStopBudget)
	defer cancel()
	m.reconcile(stopCtx, true)
}

func (m *Manager) reconcile(ctx context.Context, draining bool) {
	if m.Store == nil {
		return
	}
	records, err := m.Store.Activities()
	if err != nil {
		return
	}
	now := m.Now()
	telegramGroups := map[string][]model.Activity{}
	for _, a := range records {
		if now.After(time.UnixMilli(a.ExpiresMS).Add(m.CleanupRetention)) {
			// Raw cleanup locators have a hard local retention bound. A platform
			// outage can still leave an external orphan; live enablement requires
			// create/list/delete capability to be proven.
			_ = m.Store.ActivityDone(a.JobID)
			continue
		}
		switch a.Platform {
		case "telegram":
			if m.Telegram == nil || a.Terminal || now.UnixMilli() >= a.ExpiresMS {
				_ = m.Store.ActivityDone(a.JobID)
				continue
			}
			if draining || a.RecoverableTerminal {
				_ = m.Store.ActivityPause(a.JobID)
				continue
			}
			telegramGroups[a.EndpointDigest] = append(telegramGroups[a.EndpointDigest], a)
		case "feishu":
			if m.Feishu == nil {
				_ = m.Store.ActivityDone(a.JobID)
				continue
			}
			m.reconcileFeishu(ctx, a, draining, now)
		default:
			_ = m.Store.ActivityDone(a.JobID)
		}
	}
	if !draining {
		for _, group := range telegramGroups {
			m.pulseTelegram(ctx, group, now)
		}
	}
}

func (m *Manager) pulseTelegram(ctx context.Context, group []model.Activity, now time.Time) {
	if len(group) == 0 {
		return
	}
	due := false
	for _, a := range group {
		if a.NextAttemptMS <= now.UnixMilli() {
			due = true
			break
		}
	}
	if !due {
		return
	}
	next := now.Add(m.TelegramKeepalive)
	attempted := make([]model.Activity, 0, len(group))
	for _, a := range group {
		if a.NextAttemptMS > now.UnixMilli() {
			continue
		}
		if m.Store.ActivityAttempt(a.JobID, "pulsing", next) == nil {
			attempted = append(attempted, a)
		}
	}
	if len(attempted) == 0 {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, m.CallTimeout)
	_ = m.Telegram.Typing(callCtx, attempted[0].Target)
	cancel()
	for _, a := range attempted {
		_ = m.Store.ActivityActive(a.JobID, nil, next)
	}
}

func (m *Manager) reconcileFeishu(ctx context.Context, a model.Activity, draining bool, now time.Time) {
	cleanup := draining || a.Terminal || a.RecoverableTerminal || now.UnixMilli() >= a.ExpiresMS
	rearm := (draining || a.RecoverableTerminal) && !a.Terminal && now.UnixMilli() < a.ExpiresMS
	if a.State == "deleting" {
		if a.NextAttemptMS > now.UnixMilli() && !draining {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, m.CallTimeout)
		reactionID, found, err := m.Feishu.FindOwnReaction(callCtx, a.Target)
		cancel()
		if err != nil {
			_ = m.Store.ActivityRetry(a.JobID, now.Add(retryDelay(a.Attempts)))
			return
		}
		if !found {
			if !a.Terminal && now.UnixMilli() < a.ExpiresMS {
				_ = m.Store.ActivityPause(a.JobID)
			} else {
				_ = m.Store.ActivityDone(a.JobID)
			}
			return
		}
		a.ReactionID = reactionID
		_ = m.Store.ActivityActive(a.JobID, reactionID, now)
		m.deleteFeishu(ctx, a, now, !a.Terminal && now.UnixMilli() < a.ExpiresMS)
		return
	}
	if len(a.ReactionID) > 0 {
		immediateCleanup := draining || a.Terminal || a.RecoverableTerminal
		if cleanup && (immediateCleanup || a.NextAttemptMS <= now.UnixMilli()) {
			m.deleteFeishu(ctx, a, now, rearm)
		}
		return
	}
	if a.State == "creating" {
		if a.NextAttemptMS > now.UnixMilli() && !draining {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, m.CallTimeout)
		reactionID, found, err := m.Feishu.FindOwnReaction(callCtx, a.Target)
		cancel()
		if err != nil {
			_ = m.Store.ActivityRetry(a.JobID, now.Add(retryDelay(a.Attempts)))
			return
		}
		if found {
			_ = m.Store.ActivityActive(a.JobID, reactionID, now)
			a.ReactionID = reactionID
			if cleanup {
				m.deleteFeishu(ctx, a, now, rearm)
			}
			return
		}
		if cleanup {
			if rearm {
				_ = m.Store.ActivityPause(a.JobID)
			} else {
				_ = m.Store.ActivityDone(a.JobID)
			}
			return
		}
		_ = m.Store.ActivityRetry(a.JobID, now)
		a.State = "pending"
	}
	if cleanup {
		if rearm {
			_ = m.Store.ActivityPause(a.JobID)
		} else {
			_ = m.Store.ActivityDone(a.JobID)
		}
		return
	}
	if a.NextAttemptMS > now.UnixMilli() {
		return
	}
	next := now.Add(retryDelay(a.Attempts + 1))
	if m.Store.ActivityAttempt(a.JobID, "creating", next) != nil {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, m.CallTimeout)
	reactionID, err := m.Feishu.CreateReaction(callCtx, a.Target)
	cancel()
	if err != nil || len(reactionID) == 0 {
		// Leave "creating": a timeout may have succeeded remotely. The next
		// attempt lists before creating again.
		return
	}
	_ = m.Store.ActivityActive(a.JobID, reactionID, time.UnixMilli(a.ExpiresMS))
}

func (m *Manager) deleteFeishu(ctx context.Context, a model.Activity, now time.Time, rearm bool) {
	next := now.Add(retryDelay(a.Attempts + 1))
	if m.Store.ActivityAttempt(a.JobID, "deleting", next) != nil {
		return
	}
	callCtx, cancel := context.WithTimeout(ctx, m.CallTimeout)
	err := m.Feishu.DeleteReaction(callCtx, a.Target, a.ReactionID)
	cancel()
	if err == nil {
		if rearm {
			_ = m.Store.ActivityPause(a.JobID)
		} else {
			_ = m.Store.ActivityDone(a.JobID)
		}
	}
}

func retryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 6 {
		attempts = 6
	}
	return time.Second * time.Duration(1<<attempts)
}
