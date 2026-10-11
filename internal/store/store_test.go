package store

import (
	"bytes"
	"codex-persona-router/internal/model"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCloseClearsInternalDigestKey(t *testing.T) {
	d := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := Open(filepath.Join(d, "router.db"), "p", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	internal := s.key
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	for _, value := range internal {
		if value != 0 {
			t.Fatal("internal digest key not cleared")
		}
	}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	d := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := Open(filepath.Join(d, "router.sqlite3"), "test-beta", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	if err := s.InitMain("thread-main"); err != nil {
		t.Fatal(err)
	}
	if err := s.BindEndpoint("fake", "acct", "conv", "", "main"); err != nil {
		t.Fatal(err)
	}
	return s
}
func enqueue(t *testing.T, s *Store, key string) model.Job {
	t.Helper()
	j, err := s.Enqueue("fake", "acct", key, "main", []byte("payload"), nil)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func TestProtectedMainAndEndpoint(t *testing.T) {
	s := newStore(t)
	if err := s.InitMain("other"); !errors.Is(err, model.ErrProtectedAlias) {
		t.Fatalf("got %v", err)
	}
	if err := s.SetAlias("main", "other", true); !errors.Is(err, model.ErrProtectedAlias) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.ResolveEndpoint("fake", "wrong", "conv", ""); !errors.Is(err, model.ErrEndpoint) {
		t.Fatalf("got %v", err)
	}
}
func TestDuplicateEventAndFIFO(t *testing.T) {
	s := newStore(t)
	a := enqueue(t, s, "one")
	b := enqueue(t, s, "two")
	if _, err := s.Enqueue("fake", "acct", "one", "main", []byte("other"), nil); !errors.Is(err, model.ErrDuplicate) {
		t.Fatalf("got %v", err)
	}
	if _, err := s.Enqueue("fake", "other-account", "one", "main", []byte("other"), nil); err != nil {
		t.Fatalf("account-scoped event key collided: %v", err)
	}
	got, err := s.Acquire("w", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != a.ID {
		t.Fatal("not fifo")
	}
	if _, err = s.Acquire("w2", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatalf("same alias ran concurrently: %v", err)
	}
	if err = s.Transition(a.ID, []model.State{model.Leased}, model.DeadLetter, "", nil); err != nil {
		t.Fatal(err)
	}
	got, err = s.Acquire("w", time.Minute)
	if err != nil || got.ID != b.ID {
		t.Fatalf("second not released: %v", err)
	}
}
func TestDifferentAliasesCanAcquire(t *testing.T) {
	s := newStore(t)
	if err := s.SetAlias("side", "thread-side", false); err != nil {
		t.Fatal(err)
	}
	if err := s.BindEndpoint("fake", "acct", "other", "", "side"); err != nil {
		t.Fatal(err)
	}
	a := enqueue(t, s, "a")
	b, err := s.Enqueue("fake", "acct", "b", "side", []byte("p"), nil)
	if err != nil {
		t.Fatal(err)
	}
	x, _ := s.Acquire("w1", time.Minute)
	y, err := s.Acquire("w2", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if x.ID == y.ID || !(x.ID == a.ID && y.ID == b.ID || x.ID == b.ID && y.ID == a.ID) {
		t.Fatal("parallel aliases failed")
	}
}
func TestRecoveryBoundaries(t *testing.T) {
	s := newStore(t)
	// Leave enough wall-clock margin that a slow or shuffled CI run cannot
	// consume the entire first retry interval before the immediate assertion.
	s.retryBase = 25 * time.Millisecond
	leased := enqueue(t, s, "leased")
	got, _ := s.Acquire("w", time.Minute)
	if got.ID != leased.ID {
		t.Fatal()
	}
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Job(leased.ID)
	if j.State != model.RetryablePreSubmit {
		t.Fatalf("%s", j.State)
	}
	time.Sleep(60 * time.Millisecond)
	got, _ = s.Acquire("w", time.Minute)
	if err := s.Transition(got.ID, []model.State{model.Leased}, model.DeadLetter, "", nil); err != nil {
		t.Fatal(err)
	}
	sub := enqueue(t, s, "submitted")
	got, _ = s.Acquire("w", time.Minute)
	if got.ID != sub.ID {
		t.Fatal()
	}
	if err := s.Transition(sub.ID, []model.State{model.Leased}, model.Submitted, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	j, _ = s.Job(sub.ID)
	if j.State != model.UncertainPostSubmit {
		t.Fatalf("%s", j.State)
	}
	if _, err := s.Acquire("w", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("uncertain was retried")
	}
}
func TestOutboxLifecycleAndScrub(t *testing.T) {
	s := newStore(t)
	if err := s.RegisterMedia("image.png", 1, time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	j, err := s.Enqueue("fake", "acct", "x", "main", []byte("payload"), nil, "/private/media/image.png")
	if err != nil {
		t.Fatal(err)
	}
	a, _ := s.Acquire("w", time.Minute)
	if a.ID != j.ID {
		t.Fatal()
	}
	_ = s.Transition(j.ID, []model.State{model.Leased}, model.Submitted, "", nil)
	_ = s.Transition(j.ID, []model.State{model.Submitted}, model.Running, "turn", nil)
	if err := s.CompleteToOutbox(j.ID, "fake", []byte("endpoint"), []byte("answer")); err != nil {
		t.Fatal(err)
	}
	o, err := s.NextOutbox()
	if err != nil || string(o.Result) != "answer" {
		t.Fatalf("%v", err)
	}
	// Leave enough wall-clock margin that a slow or shuffled CI run cannot
	// consume the entire first retry interval before the immediate assertion.
	s.retryBase = 25 * time.Millisecond
	if err := s.MarkOutbox(j.ID, model.DeliveryRetryable); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NextOutbox(); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("delivery retry ignored backoff")
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := s.NextOutbox(); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkOutbox(j.ID, model.Delivered); err != nil {
		t.Fatal(err)
	}
	done, _ := s.Job(j.ID)
	if done.State != model.Delivered || done.Payload != nil || done.Result != nil ||
		done.DeliveryTarget != nil || done.MediaPaths != nil {
		t.Fatal("terminal content not scrubbed")
	}
}
func TestExternalLeaseBlocksAndReleases(t *testing.T) {
	s := newStore(t)
	enqueue(t, s, "x")
	if err := s.SetExternalLease("main", "gui", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire("w", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("external lease ignored")
	}
	if err := s.ClearExternalLease("main", "wrong"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire("w", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("wrong external completion cleared lease")
	}
	if err := s.ClearExternalLease("main", "gui"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Acquire("w", time.Minute); err != nil {
		t.Fatal(err)
	}
}
func TestCrossPersonaDatabaseRejected(t *testing.T) {
	d := filepath.Join(t.TempDir(), "state")
	os.Mkdir(d, 0700)
	p := filepath.Join(d, "r.sqlite3")
	a, err := Open(p, "a", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	a.Close()
	if _, err = Open(p, "b", make([]byte, 32)); !errors.Is(err, model.ErrCrossPersona) {
		t.Fatalf("got %v", err)
	}
}
func TestSQLiteFilesRemainPrivate(t *testing.T) {
	s := newStore(t)
	enqueue(t, s, "private")
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Lstat(s.path + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&0077 != 0 {
			t.Fatalf("%s mode %o", suffix, info.Mode().Perm())
		}
	}
}

func TestSQLiteExistingBroadModeRejectedRatherThanRepaired(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "router.db")
	s, err := Open(path, "p", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path, "p", make([]byte, 32)); err == nil {
		t.Fatal("existing broad SQLite mode was accepted or silently repaired")
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("rejected SQLite path was modified: %04o", info.Mode().Perm())
	}
}

func TestSchemaVersionGate(t *testing.T) {
	d := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(d, "router.db")
	key := make([]byte, 32)
	s, err := Open(p, "p", key)
	if err != nil {
		t.Fatal(err)
	}
	version, found, err := s.queryInt("PRAGMA user_version")
	if err != nil || !found || version != schemaVersion {
		t.Fatalf("version=%d found=%v err=%v", version, found, err)
	}
	if err := s.exec("PRAGMA user_version=999"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(p, "p", key); err == nil {
		t.Fatal("unknown schema version accepted")
	}
}
func TestSchemaVersionOneMigratesExplicitly(t *testing.T) {
	d := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(d, "router.db")
	key := make([]byte, 32)
	s, err := Open(p, "p", key)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.exec("DROP TABLE main_epochs; DROP TABLE main_history_baseline; ALTER TABLE turn_jobs DROP COLUMN media_paths; ALTER TABLE turn_jobs DROP COLUMN next_attempt_ms; ALTER TABLE media_leases DROP COLUMN lease_hard_expires_ms; ALTER TABLE media_leases DROP COLUMN reference_count; UPDATE metadata SET value='1' WHERE key='schema_version'; PRAGMA user_version=1;"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(p, "p", key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, item := range [][2]string{{"turn_jobs", "media_paths"}, {"turn_jobs", "next_attempt_ms"}, {"media_leases", "lease_hard_expires_ms"}, {"media_leases", "reference_count"}} {
		ok, err := s.hasColumn(item[0], item[1])
		if err != nil || !ok {
			t.Fatalf("migration missing %s.%s: %v", item[0], item[1], err)
		}
	}
}

func TestSchemaVersionThreeMigratesToMainHistorySchemaFive(t *testing.T) {
	d := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(d, "router.db")
	key := make([]byte, 32)
	s, err := Open(p, "p", key)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.exec("DROP TABLE main_epochs; DROP TABLE main_history_baseline; DROP TABLE activity_indicators; UPDATE metadata SET value='3' WHERE key='schema_version'; PRAGMA user_version=3;"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(p, "p", key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	count, _, err := s.queryInt("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='activity_indicators'")
	if err != nil || count != 1 {
		t.Fatalf("activity migration missing: %v", err)
	}
	rotations, _, rotationErr := s.queryInt("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='main_rotations'")
	version, _, err := s.queryInt("PRAGMA user_version")
	recorded, found, metadataErr := s.queryText("SELECT value FROM metadata WHERE key='schema_version'")
	if err != nil || metadataErr != nil || rotationErr != nil || rotations != 1 || !found || version != 5 || recorded != "5" {
		t.Fatalf("control-plane schema migration failed")
	}
}

func TestSchemaFiveRejectsMissingControlPlaneTables(t *testing.T) {
	for _, table := range []string{"activity_indicators", "main_rotations"} {
		t.Run(table, func(t *testing.T) {
			d := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(d, 0700); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(d, "router.db")
			key := make([]byte, 32)
			s, err := Open(p, "p", key)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.exec("DROP TABLE " + table); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(p, "p", key); err == nil {
				t.Fatal("incomplete schema v4 accepted")
			}
		})
	}
}

func TestActivityEndpointExcludesSourceMessageAndDoneDeletes(t *testing.T) {
	s := newStore(t)
	activityTTL := 17 * time.Minute
	for _, in := range []model.Inbound{
		{Platform: "telegram", Account: "acct", EventKey: "one", Conversation: "conv", Owner: "owner", DeliveryTarget: []byte(`{"chat_id":"same","reply_to_message_id":1}`), Payload: []byte("x")},
		{Platform: "telegram", Account: "acct", EventKey: "two", Conversation: "conv", Owner: "owner", DeliveryTarget: []byte(`{"chat_id":"same","reply_to_message_id":2}`), Payload: []byte("x")},
	} {
		if _, err := s.EnqueueInbound(in, "main", true, activityTTL); err != nil {
			t.Fatal(err)
		}
	}
	records, err := s.Activities()
	if err != nil || len(records) != 2 {
		t.Fatalf("records=%d err=%v", len(records), err)
	}
	if records[0].EndpointDigest != records[1].EndpointDigest {
		t.Fatal("source message leaked into endpoint grouping")
	}
	remaining := time.Until(time.UnixMilli(records[0].ExpiresMS))
	if remaining < activityTTL-time.Second || remaining > activityTTL+time.Second {
		t.Fatalf("configured activity ttl drifted: %s", remaining)
	}
	if err := s.ActivityDone(records[0].JobID); err != nil {
		t.Fatal(err)
	}
	count, err := s.ActiveActivityCount()
	if err != nil || count != 1 {
		t.Fatalf("terminal activity retained: count=%d err=%v", count, err)
	}
}
func TestCrashRecoveryVisibleStates(t *testing.T) {
	s := newStore(t)
	queued := enqueue(t, s, "q")
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	q, _ := s.Job(queued.ID)
	if q.State != model.Queued {
		t.Fatalf("queued -> %s", q.State)
	}
	a, _ := s.Acquire("w", time.Minute)
	if err := s.Transition(a.ID, []model.State{model.Leased}, model.Submitted, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Transition(a.ID, []model.State{model.Submitted}, model.Running, "turn", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	q, _ = s.Job(a.ID)
	if q.State != model.UncertainPostSubmit {
		t.Fatalf("running -> %s", q.State)
	}
}
func TestCrashDuringDeliveryBecomesUncertain(t *testing.T) {
	s := newStore(t)
	j := enqueue(t, s, "delivery")
	a, _ := s.Acquire("w", time.Minute)
	s.Transition(a.ID, []model.State{model.Leased}, model.Submitted, "", nil)
	s.Transition(a.ID, []model.State{model.Submitted}, model.Running, "turn", nil)
	s.CompleteToOutbox(j.ID, "fake", []byte("endpoint"), []byte("reply"))
	if _, err := s.NextOutbox(); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Job(j.ID)
	if got.State != model.DeliveryUncertain {
		t.Fatalf("%s", got.State)
	}
	if _, err := s.NextOutbox(); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("uncertain delivery retried")
	}
}

func TestPreSubmitRetryUsesDurableBackoffAndTerminalLimit(t *testing.T) {
	s := newStore(t)
	s.retryBase = 100 * time.Millisecond
	j := enqueue(t, s, "backoff")
	for attempt := 1; attempt <= 12; attempt++ {
		acquired, err := s.Acquire("worker", time.Minute)
		if err != nil {
			t.Fatalf("attempt %d acquire: %v", attempt, err)
		}
		if err := s.Transition(acquired.ID, []model.State{model.Leased}, model.RetryablePreSubmit, "", nil); err != nil {
			t.Fatal(err)
		}
		state, _ := s.Job(j.ID)
		if attempt == 12 {
			if state.State != model.DeadLetter || state.Payload != nil {
				t.Fatal("retry limit did not dead-letter and scrub")
			}
			break
		}
		if _, err := s.Acquire("worker", time.Minute); !errors.Is(err, model.ErrNoWork) {
			t.Fatal("pre-submit retry ignored durable backoff")
		}
		if err := s.execParams("UPDATE turn_jobs SET next_attempt_ms=0 WHERE id=?", j.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReconcileDeliveredScrubsAllTerminalMediaMetadata(t *testing.T) {
	s := newStore(t)
	j := enqueue(t, s, "reconcile-delivery")
	acquired, _ := s.Acquire("worker", time.Minute)
	s.Transition(acquired.ID, []model.State{model.Leased}, model.Submitted, "", nil)
	s.Transition(acquired.ID, []model.State{model.Submitted}, model.Running, "turn", nil)
	if err := s.CompleteToOutbox(j.ID, "fake", []byte("target"), []byte("reply")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NextOutbox(); err != nil {
		t.Fatal(err)
	}
	if err := s.Recover(); err != nil {
		t.Fatal(err)
	}
	paths, _ := json.Marshal([]string{"/private/media/stale.png"})
	if err := s.execParams("UPDATE turn_jobs SET media_paths=? WHERE id=?", paths, j.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Reconcile(j.ID, "evidence-token-long", "delivered"); err != nil {
		t.Fatal(err)
	}
	done, _ := s.Job(j.ID)
	if done.State != model.Delivered || done.Payload != nil || done.Result != nil ||
		done.DeliveryTarget != nil || done.MediaPaths != nil {
		t.Fatal("reconciled terminal state retained private metadata")
	}
}
