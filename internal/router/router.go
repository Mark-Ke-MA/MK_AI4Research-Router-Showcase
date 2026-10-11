package router

import (
	"codex-persona-router/internal/activity"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"codex-persona-router/internal/model"
	"codex-persona-router/internal/store"
)

type Codex interface {
	Turn(context.Context, string, string, string, []byte, []string, func(string) error) (turnID string, result []byte, err error)
}

type Delivery interface {
	Send(context.Context, model.Job) error
}
type Media interface {
	Lease(string) error
	Renew(string) error
	Release(string)
}

type Router struct {
	store           *store.Store
	codex           Codex
	delivery        Delivery
	workerID        string
	concurrency     int
	lease           time.Duration
	wg              sync.WaitGroup
	draining        atomic.Bool
	drainOnce       sync.Once
	drainSignal     chan struct{}
	intakeMu        sync.RWMutex
	media           Media
	mediaRenewEvery time.Duration
	activityManager *activity.Manager
	activityEnabled map[string]bool
	activityTTL     time.Duration
}

func New(st *store.Store, codex Codex, delivery Delivery, workerID string, concurrency int) *Router {
	if concurrency < 1 {
		concurrency = 1
	}
	return &Router{
		store: st, codex: codex, delivery: delivery, workerID: workerID,
		concurrency: concurrency, lease: 2 * time.Minute, mediaRenewEvery: 2 * time.Minute,
		activityTTL: 5 * time.Minute, drainSignal: make(chan struct{}),
	}
}
func (r *Router) SetMedia(media Media) { r.media = media }
func (r *Router) SetActivityTTL(ttl time.Duration) {
	if ttl > 0 {
		r.activityTTL = ttl
	}
}
func (r *Router) SetActivity(manager *activity.Manager, platforms ...string) {
	r.activityManager = manager
	r.activityEnabled = make(map[string]bool, len(platforms))
	for _, platform := range platforms {
		if platform == "telegram" || platform == "feishu" {
			r.activityEnabled[platform] = true
		}
	}
}

func (r *Router) Accept(in model.Inbound) (model.Job, error) {
	r.intakeMu.RLock()
	defer r.intakeMu.RUnlock()
	if r.draining.Load() {
		return model.Job{}, fmt.Errorf("router draining")
	}
	authorized, err := r.store.OwnerAuthorized(in.Platform, in.Account, in.Owner)
	if err != nil || !authorized {
		return model.Job{}, model.ErrEndpoint
	}
	alias, err := r.store.ResolveEndpoint(in.Platform, in.Account, in.Conversation, in.Topic)
	if err != nil {
		return model.Job{}, err
	}
	return r.store.EnqueueInbound(in, alias, r.activityEnabled[in.Platform], r.activityTTL)
}

func (r *Router) Run(ctx context.Context) error {
	if err := r.store.Recover(); err != nil {
		return err
	}
	if r.activityManager != nil {
		activityCtx, stopActivity := context.WithCancel(ctx)
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			defer stopActivity()
			r.activityManager.Run(activityCtx)
		}()
		go func() {
			select {
			case <-r.drainSignal:
				stopActivity()
			case <-activityCtx.Done():
			}
		}()
	}
	for i := 0; i < r.concurrency; i++ {
		r.wg.Add(1)
		go r.worker(ctx)
	}
	r.wg.Add(1)
	go r.outbox(ctx)
	<-ctx.Done()
	r.Drain()
	r.wg.Wait()
	if r.activityManager != nil {
		r.activityManager.FinalizeDrain()
	}
	return r.store.Checkpoint()
}

func (r *Router) Drain() {
	r.intakeMu.Lock()
	r.draining.Store(true)
	r.intakeMu.Unlock()
	r.drainOnce.Do(func() { close(r.drainSignal) })
}

func (r *Router) DrainComplete() (bool, error) {
	pending, err := r.store.DrainPendingCount()
	return pending == 0, err
}

func (r *Router) worker(ctx context.Context) {
	defer r.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := r.store.Acquire(r.workerID, r.lease)
		if errors.Is(err, model.ErrNoWork) {
			if !wait(ctx, 20*time.Millisecond) {
				return
			}
			continue
		}
		if err != nil {
			if !wait(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		r.process(ctx, job)
	}
}

func (r *Router) process(ctx context.Context, job model.Job) {
	leasedMedia := make([]string, 0, len(job.MediaPaths))
	for _, path := range job.MediaPaths {
		if r.media == nil || r.media.Lease(path) != nil {
			for _, leased := range leasedMedia {
				r.media.Release(leased)
			}
			_ = r.store.Transition(job.ID, []model.State{model.Leased}, model.DeadLetter, "", nil)
			return
		}
		leasedMedia = append(leasedMedia, path)
	}
	stopRenew := make(chan struct{})
	renewDone := make(chan struct{})
	if len(leasedMedia) > 0 {
		go func() {
			defer close(renewDone)
			ticker := time.NewTicker(r.mediaRenewEvery)
			defer ticker.Stop()
			for {
				select {
				case <-stopRenew:
					return
				case <-ticker.C:
					for _, path := range leasedMedia {
						_ = r.media.Renew(path)
					}
				}
			}
		}()
	} else {
		close(renewDone)
	}
	stopRenewal := func() {
		select {
		case <-stopRenew:
		default:
			close(stopRenew)
		}
		<-renewDone
	}
	releaseMedia := func() {
		for _, path := range leasedMedia {
			r.media.Release(path)
		}
	}
	// "submitted" is a durable intent written immediately before any app-server
	// write. A crash after this point is conservatively uncertain, never retried.
	if err := r.store.Transition(job.ID, []model.State{model.Leased}, model.Submitted, "", nil); err != nil {
		stopRenewal()
		releaseMedia()
		return
	}
	started := func(turnID string) error {
		return r.store.Transition(job.ID, []model.State{model.Submitted}, model.Running, turnID, nil)
	}
	turnID, result, err := r.codex.Turn(ctx, job.ThreadID, job.ID, job.Platform, job.Payload, job.MediaPaths, started)
	if err != nil {
		stopRenewal()
		var pre *model.PreSubmitError
		if errors.As(err, &pre) {
			// Only the transport may certify that zero bytes were submitted.
			_ = r.store.Transition(job.ID, []model.State{model.Submitted}, model.RetryablePreSubmit, "", nil)
			releaseMedia()
		} else {
			_ = r.store.Transition(job.ID, []model.State{model.Submitted, model.Running}, model.UncertainPostSubmit, turnID, nil)
		}
		return
	}
	stopRenewal()
	releaseMedia()
	if turnID == "" {
		_ = r.store.Transition(job.ID, []model.State{model.Submitted}, model.UncertainPostSubmit, "", nil)
		return
	}
	if err := r.store.CompleteToOutbox(job.ID, job.Platform, job.DeliveryTarget, result); err != nil {
		_ = r.store.Transition(job.ID, []model.State{model.Submitted, model.Running}, model.UncertainPostSubmit, turnID, nil)
	}
}

func (r *Router) outbox(ctx context.Context) {
	defer r.wg.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := r.store.NextOutbox()
		if errors.Is(err, model.ErrNoWork) || err == nil && job.ID == "" {
			if !wait(ctx, 20*time.Millisecond) {
				return
			}
			continue
		}
		if err != nil {
			if !wait(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		err = r.delivery.Send(ctx, job)
		if err == nil {
			_ = r.store.MarkOutbox(job.ID, model.Delivered)
			continue
		}
		var uncertain *model.DeliveryUncertainError
		if errors.As(err, &uncertain) || ctx.Err() != nil {
			_ = r.store.MarkOutbox(job.ID, model.DeliveryUncertain)
		} else {
			_ = r.store.MarkOutbox(job.ID, model.DeliveryRetryable)
		}
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
