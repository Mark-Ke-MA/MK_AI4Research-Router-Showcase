package router

import (
	"codex-persona-router/internal/store"
	"time"
)

// ExternalObserver converts GUI/app-server turn notifications into a lease on
// the logical alias. Unknown threads fail closed and do not affect other aliases.
type ExternalObserver struct {
	Store *store.Store
	TTL   time.Duration
}

func (o ExternalObserver) Started(threadID, turnID string) {
	alias, err := o.Store.AliasForThread(threadID)
	if err != nil {
		return
	}
	ttl := o.TTL
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	_ = o.Store.SetExternalLease(alias, "external:"+turnID, ttl)
}
func (o ExternalObserver) Completed(threadID, turnID string) {
	alias, err := o.Store.AliasForThread(threadID)
	if err != nil {
		return
	}
	_ = o.Store.ClearExternalLease(alias, "external:"+turnID)
}
