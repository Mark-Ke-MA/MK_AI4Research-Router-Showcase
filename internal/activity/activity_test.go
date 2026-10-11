package activity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"codex-persona-router/internal/model"
)

type fakeStore struct {
	mu          sync.Mutex
	records     map[string]model.Activity
	attemptFail bool
	attempts    int
	paused      int
	done        int
}

func newFakeStore(items ...model.Activity) *fakeStore {
	s := &fakeStore{records: map[string]model.Activity{}}
	for _, item := range items {
		s.records[item.JobID] = item
	}
	return s
}
func (s *fakeStore) Activities() ([]model.Activity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.Activity, 0, len(s.records))
	for _, item := range s.records {
		out = append(out, item)
	}
	return out, nil
}
func (s *fakeStore) ActivityAttempt(id, state string, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attemptFail {
		return errors.New("durable attempt failed")
	}
	item := s.records[id]
	item.State, item.NextAttemptMS, item.Attempts = state, next.UnixMilli(), item.Attempts+1
	s.records[id] = item
	s.attempts++
	return nil
}
func (s *fakeStore) ActivityActive(id string, reaction []byte, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.records[id]
	item.State, item.ReactionID, item.NextAttemptMS = "active", append([]byte(nil), reaction...), next.UnixMilli()
	s.records[id] = item
	return nil
}
func (s *fakeStore) ActivityRetry(id string, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.records[id]
	item.NextAttemptMS = next.UnixMilli()
	s.records[id] = item
	return nil
}
func (s *fakeStore) ActivityPause(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	item := s.records[id]
	item.State, item.ReactionID, item.NextAttemptMS = "pending", nil, 0
	s.records[id] = item
	s.paused++
	return nil
}
func (s *fakeStore) ActivityDone(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	s.done++
	return nil
}

type fakeTelegram struct{ calls int }

func (f *fakeTelegram) Typing(context.Context, []byte) error { f.calls++; return nil }

type fakeFeishu struct {
	created, deleted, found int
	findID                  []byte
	findOK                  bool
	err                     error
	deletedID               []byte
}

func (f *fakeFeishu) CreateReaction(context.Context, []byte) ([]byte, error) {
	f.created++
	return []byte("opaque-reaction"), f.err
}
func (f *fakeFeishu) DeleteReaction(_ context.Context, _, reactionID []byte) error {
	f.deleted++
	f.deletedID = append([]byte(nil), reactionID...)
	return f.err
}
func (f *fakeFeishu) FindOwnReaction(context.Context, []byte) ([]byte, bool, error) {
	f.found++
	return append([]byte(nil), f.findID...), f.findOK, f.err
}

func record(id, endpoint string, now time.Time) model.Activity {
	return model.Activity{
		JobID: id, Platform: "telegram", EndpointDigest: endpoint, State: "pending",
		Target: []byte("opaque-target-" + id), ExpiresMS: now.Add(time.Minute).UnixMilli(),
	}
}

func TestTelegramGroupsConcurrentEndpointAndKeepsLaterJob(t *testing.T) {
	now := time.Unix(100, 0)
	first, second := record("first", "same-endpoint", now), record("second", "same-endpoint", now)
	store := newFakeStore(first, second)
	driver := &fakeTelegram{}
	manager := &Manager{Store: store, Telegram: driver, Now: func() time.Time { return now }}
	manager.defaults()
	manager.reconcile(context.Background(), false)
	if driver.calls != 1 || store.attempts != 2 {
		t.Fatalf("calls=%d attempts=%d", driver.calls, store.attempts)
	}
	item := store.records["first"]
	item.Terminal = true
	store.records["first"] = item
	item = store.records["second"]
	item.NextAttemptMS = now.UnixMilli()
	store.records["second"] = item
	manager.reconcile(context.Background(), false)
	if driver.calls != 2 {
		t.Fatal("first terminal job stopped the later endpoint activity")
	}
	if _, exists := store.records["first"]; exists {
		t.Fatal("terminal activity row retained")
	}
	if _, exists := store.records["second"]; !exists {
		t.Fatal("later activity row removed")
	}
}

func TestTelegramDurableAttemptFailureCannotStormPlatform(t *testing.T) {
	now := time.Unix(100, 0)
	store := newFakeStore(record("job", "endpoint", now))
	store.attemptFail = true
	driver := &fakeTelegram{}
	manager := &Manager{Store: store, Telegram: driver, Now: func() time.Time { return now }}
	manager.defaults()
	for range 5 {
		manager.reconcile(context.Background(), false)
	}
	if driver.calls != 0 {
		t.Fatal("platform called without durable attempt")
	}
}

func TestDrainPausesAndRestartRearmsNonterminalActivity(t *testing.T) {
	now := time.Unix(100, 0)
	store := newFakeStore(record("job", "endpoint", now))
	driver := &fakeTelegram{}
	manager := &Manager{Store: store, Telegram: driver, Now: func() time.Time { return now }}
	manager.defaults()
	manager.reconcile(context.Background(), true)
	if store.paused != 1 || driver.calls != 0 {
		t.Fatal("drain did not pause without platform traffic")
	}
	manager.reconcile(context.Background(), false)
	if driver.calls != 1 {
		t.Fatal("restart did not re-arm nonterminal activity")
	}
}

func TestRecoverableTerminalPausesAndDispositionCanRearm(t *testing.T) {
	now := time.Unix(100, 0)
	item := record("job", "endpoint", now)
	item.RecoverableTerminal = true
	store := newFakeStore(item)
	driver := &fakeTelegram{}
	manager := &Manager{Store: store, Telegram: driver, Now: func() time.Time { return now }}
	manager.defaults()
	manager.reconcile(context.Background(), false)
	if store.paused != 1 || driver.calls != 0 {
		t.Fatal("uncertain activity did not pause")
	}
	item = store.records["job"]
	item.RecoverableTerminal = false
	store.records["job"] = item
	manager.reconcile(context.Background(), false)
	if driver.calls != 1 {
		t.Fatal("retry disposition did not re-arm activity")
	}
}

func TestFeishuCreatingCrashWindowUsesListBeforeCleanup(t *testing.T) {
	now := time.Unix(100, 0)
	item := model.Activity{
		JobID: "job", Platform: "feishu", State: "creating", Target: []byte("opaque-target"),
		ExpiresMS: now.Add(time.Minute).UnixMilli(), Terminal: true,
	}
	store := newFakeStore(item)
	driver := &fakeFeishu{findID: []byte("opaque-reaction"), findOK: true}
	manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
	manager.defaults()
	manager.reconcile(context.Background(), false)
	if driver.found != 1 || driver.created != 0 || driver.deleted != 1 || len(store.records) != 0 {
		t.Fatal("ambiguous create was not reconciled and scrubbed")
	}
}

func TestFeishuDeletingCrashListsBeforeDecidingRemoteCleanup(t *testing.T) {
	now := time.Unix(100, 0)
	item := model.Activity{
		JobID: "job", Platform: "feishu", State: "deleting", Target: []byte("opaque-target"),
		ReactionID: []byte("stale-local-reaction"), ExpiresMS: now.Add(time.Minute).UnixMilli(), Terminal: true,
	}
	t.Run("delete succeeded remotely before local persistence", func(t *testing.T) {
		store := newFakeStore(item)
		driver := &fakeFeishu{findOK: false}
		manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
		manager.defaults()
		manager.reconcile(context.Background(), false)
		if driver.found != 1 || driver.deleted != 0 || len(store.records) != 0 {
			t.Fatal("missing remote reaction was not reconciled as cleaned")
		}
	})
	t.Run("reaction still exists and listed id is authoritative", func(t *testing.T) {
		store := newFakeStore(item)
		driver := &fakeFeishu{findOK: true, findID: []byte("listed-reaction")}
		manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
		manager.defaults()
		manager.reconcile(context.Background(), false)
		if driver.found != 1 || driver.deleted != 1 || string(driver.deletedID) != "listed-reaction" || len(store.records) != 0 {
			t.Fatal("deleting reconciliation did not use listed reaction")
		}
	})
}

func TestFeishuActiveCleanupIgnoresDisplayExpiryForTerminalAndDrain(t *testing.T) {
	now := time.Unix(100, 0)
	active := model.Activity{
		JobID: "job", Platform: "feishu", State: "active", Target: []byte("opaque-target"),
		ReactionID: []byte("opaque-reaction"), NextAttemptMS: now.Add(time.Minute).UnixMilli(),
		ExpiresMS: now.Add(time.Minute).UnixMilli(),
	}
	t.Run("terminal deletes immediately", func(t *testing.T) {
		item := active
		item.Terminal = true
		store := newFakeStore(item)
		driver := &fakeFeishu{}
		manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
		manager.defaults()
		manager.reconcile(context.Background(), false)
		if driver.deleted != 1 || len(store.records) != 0 {
			t.Fatal("terminal reaction waited for display expiry")
		}
	})
	t.Run("drain deletes immediately then pauses", func(t *testing.T) {
		store := newFakeStore(active)
		driver := &fakeFeishu{}
		manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
		manager.defaults()
		manager.reconcile(context.Background(), true)
		if driver.deleted != 1 || store.paused != 1 || store.records["job"].State != "pending" {
			t.Fatal("drain reaction waited for display expiry or failed to re-arm")
		}
	})
	t.Run("ordinary active job keeps reaction", func(t *testing.T) {
		store := newFakeStore(active)
		driver := &fakeFeishu{}
		manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
		manager.defaults()
		manager.reconcile(context.Background(), false)
		if driver.deleted != 0 || len(store.records) != 1 {
			t.Fatal("nonterminal reaction was deleted before expiry")
		}
	})
}

func TestFeishuDeleteFailureBacksOffWithoutStorm(t *testing.T) {
	now := time.Unix(100, 0)
	item := model.Activity{
		JobID: "job", Platform: "feishu", State: "active", Target: []byte("opaque-target"),
		ReactionID: []byte("opaque-reaction"), NextAttemptMS: now.Add(time.Minute).UnixMilli(),
		ExpiresMS: now.Add(time.Minute).UnixMilli(), Terminal: true,
	}
	store := newFakeStore(item)
	driver := &fakeFeishu{err: errors.New("offline")}
	manager := &Manager{Store: store, Feishu: driver, Now: func() time.Time { return now }}
	manager.defaults()
	manager.reconcile(context.Background(), false)
	if driver.deleted != 1 || store.records["job"].State != "deleting" ||
		store.records["job"].NextAttemptMS <= now.UnixMilli() {
		t.Fatal("failed delete did not enter durable backoff")
	}
	manager.reconcile(context.Background(), false)
	if driver.deleted != 1 || driver.found != 0 {
		t.Fatal("deleting backoff caused a cleanup request storm")
	}
}

func TestCleanupRetentionBoundsLocalLocatorLifetime(t *testing.T) {
	now := time.Unix(100, 0)
	item := model.Activity{
		JobID: "job", Platform: "feishu", State: "deleting", Target: []byte("opaque-target"),
		ReactionID: []byte("opaque-reaction"), ExpiresMS: now.Add(-2 * time.Hour).UnixMilli(),
	}
	store := newFakeStore(item)
	driver := &fakeFeishu{err: errors.New("offline")}
	manager := &Manager{
		Store: store, Feishu: driver, CleanupRetention: time.Hour, Now: func() time.Time { return now },
	}
	manager.defaults()
	manager.reconcile(context.Background(), false)
	if len(store.records) != 0 || driver.deleted != 0 {
		t.Fatal("expired cleanup locator was retained")
	}
}
