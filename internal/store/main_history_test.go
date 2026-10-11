package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codex-persona-router/internal/model"
)

func historyStore(t *testing.T) *Store {
	t.Helper()
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	st, err := Open(filepath.Join(state, "router.sqlite3"), "test-persona", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func fixedHistoryClock(st *Store, values ...int64) {
	index := 0
	st.now = func() time.Time {
		value := values[index]
		if index+1 < len(values) {
			index++
		}
		return time.Unix(0, value)
	}
}

func prepareRotation(t *testing.T, st *Store, operationID, newThread string) model.MainRef {
	t.Helper()
	old, err := st.MainRef()
	if err != nil {
		t.Fatal(err)
	}
	if _, created, err := st.BeginRotation(operationID, old); err != nil || !created {
		t.Fatalf("begin rotation: created=%v err=%v", created, err)
	}
	for _, transition := range []struct {
		from, to, thread string
	}{
		{"prepared", "creating", ""},
		{"creating", "created", newThread},
		{"created", "initializing", ""},
		{"initializing", "initialization_starting", ""},
		{"initialization_starting", "initialized", ""},
	} {
		if err := st.AdvanceRotation(operationID, transition.from, transition.to, transition.thread); err != nil {
			t.Fatal(err)
		}
	}
	return old
}

func TestFormalMainInitializationCreatesExactGenerationOne(t *testing.T) {
	st := historyStore(t)
	fixedHistoryClock(st, 100)
	if _, created, err := st.BeginMainInitialization("initialize-1"); err != nil || !created {
		t.Fatalf("begin initialization: created=%v err=%v", created, err)
	}
	for _, transition := range []struct {
		from, to, thread string
	}{
		{"init_prepared", "init_creating", ""},
		{"init_creating", "init_created", "thread-a"},
		{"init_created", "init_initializing", ""},
		{"init_initializing", "init_initialization_starting", ""},
		{"init_initialization_starting", "init_initialized", ""},
	} {
		if err := st.AdvanceMainInitialization("initialize-1", transition.from, transition.to, transition.thread); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.CommitInitialMain("initialize-1", "thread-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	history, err := st.MainEpochHistory(100, 200)
	if err != nil || len(history.Epochs) != 1 || history.Epochs[0].Generation != 1 ||
		history.Epochs[0].TransitionKind != "initialization" {
		t.Fatalf("formal initialization history missing: %+v err=%v", history, err)
	}
}

func TestMainEpochHistoryInitializationRotationRollback(t *testing.T) {
	st := historyStore(t)
	fixedHistoryClock(st, 100, 200, 300)
	if err := st.Bootstrap("thread-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	old := prepareRotation(t, st, "rotate-1", "thread-b")
	if _, err := st.CommitMainRotation("rotate-1", old, "thread-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RollbackMainRotation("rotate-1"); err != nil {
		t.Fatal(err)
	}

	history, err := st.MainEpochHistory(100, 400)
	if err != nil {
		t.Fatal(err)
	}
	if history.Origin != "exact_initialization" || !history.PriorHistoryComplete || history.Baseline != nil ||
		history.HeadGeneration != 3 || len(history.Epochs) != 3 {
		t.Fatalf("unexpected history: %+v", history)
	}
	wantKinds := []string{"initialization", "rotation", "rollback"}
	wantThreads := []string{"thread-a", "thread-b", "thread-a"}
	wantFrom := []int64{100, 200, 300}
	wantTo := []int64{200, 300, 0}
	for i, epoch := range history.Epochs {
		if epoch.Generation != int64(i+1) || epoch.TransitionKind != wantKinds[i] ||
			epoch.ThreadID != wantThreads[i] || epoch.EffectiveFromNS != wantFrom[i] ||
			epoch.EffectiveToNS != wantTo[i] {
			t.Fatalf("epoch %d: %+v", i, epoch)
		}
		if i > 0 && epoch.PreviousDigest != history.Epochs[i-1].Digest {
			t.Fatalf("digest chain broken at %d", i)
		}
	}
}

func TestMainEpochHistoryAbortedCandidateIsAbsent(t *testing.T) {
	st := historyStore(t)
	fixedHistoryClock(st, 100)
	if err := st.Bootstrap("thread-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	current, err := st.MainRef()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.BeginRotation("abort-1", current); err != nil {
		t.Fatal(err)
	}
	if err := st.MarkRotationAborted("abort-1", "prepared", true); err != nil {
		t.Fatal(err)
	}
	history, err := st.MainEpochHistory(100, 200)
	if err != nil || len(history.Epochs) != 1 || history.HeadGeneration != 1 {
		t.Fatalf("aborted candidate entered history: %+v err=%v", history, err)
	}
}

func TestMainEpochHistoryCorruptionFailsClosed(t *testing.T) {
	st := historyStore(t)
	fixedHistoryClock(st, 100)
	if err := st.Bootstrap("thread-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.exec("UPDATE main_epochs SET epoch_digest='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' WHERE generation=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.MainEpochHistory(100, 200); !errors.Is(err, model.ErrHistoryCorrupt) {
		t.Fatalf("corrupt history accepted: %v", err)
	}
}

func TestSchemaFourMigrationCreatesHonestBaseline(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "router.sqlite3")
	key := make([]byte, 32)
	st, err := Open(path, "test-persona", key)
	if err != nil {
		t.Fatal(err)
	}
	fixedHistoryClock(st, 100)
	if err := st.Bootstrap("legacy-thread", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.exec("DROP TABLE main_epochs; DROP TABLE main_history_baseline; UPDATE metadata SET value='4' WHERE key='schema_version'; PRAGMA user_version=4;"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(path, "test-persona", key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	baseline, err := st.baselineLocked()
	if err != nil || baseline == nil {
		t.Fatalf("baseline missing: %+v err=%v", baseline, err)
	}
	if baseline.Generation != 1 || baseline.ThreadID != "legacy-thread" || baseline.ObservedFromNS <= 0 {
		t.Fatalf("invalid baseline: %+v", baseline)
	}
	if _, err := st.MainEpochHistory(baseline.ObservedFromNS-1, baseline.ObservedFromNS+1); !errors.Is(err, model.ErrHistoryIncomplete) {
		t.Fatalf("pre-baseline range accepted: %v", err)
	}
	history, err := st.MainEpochHistory(baseline.ObservedFromNS, baseline.ObservedFromNS+1)
	if err != nil || history.Origin != "migration_baseline" || history.Baseline == nil ||
		len(history.Epochs) != 0 || history.PriorHistoryComplete {
		t.Fatalf("baseline coverage rejected: %+v err=%v", history, err)
	}
}

func TestLaterBaselineQueryKeepsProvenanceWithoutFalseAdjacency(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "router.sqlite3")
	key := make([]byte, 32)
	st, err := Open(path, "test-persona", key)
	if err != nil {
		t.Fatal(err)
	}
	fixedHistoryClock(st, 100)
	if err := st.Bootstrap("legacy-thread", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.exec("DROP TABLE main_epochs; DROP TABLE main_history_baseline; UPDATE metadata SET value='4' WHERE key='schema_version'; PRAGMA user_version=4;"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err = Open(path, "test-persona", key)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	baseline, err := st.baselineLocked()
	if err != nil || baseline == nil {
		t.Fatalf("baseline missing: %v", err)
	}
	fixedHistoryClock(st, baseline.ObservedFromNS+100, baseline.ObservedFromNS+200)
	old := prepareRotation(t, st, "rotate-1", "thread-b")
	if _, err := st.CommitMainRotation("rotate-1", old, "thread-b"); err != nil {
		t.Fatal(err)
	}
	old = prepareRotation(t, st, "rotate-2", "thread-c")
	if _, err := st.CommitMainRotation("rotate-2", old, "thread-c"); err != nil {
		t.Fatal(err)
	}

	history, err := st.MainEpochHistory(baseline.ObservedFromNS+250, baseline.ObservedFromNS+300)
	if err != nil {
		t.Fatal(err)
	}
	if history.Baseline == nil || len(history.Epochs) != 1 || history.Epochs[0].Generation != 3 {
		t.Fatalf("later query lost provenance or required false adjacency: %+v", history)
	}
}

func TestEpochAppendFailureRollsBackAliasCAS(t *testing.T) {
	st := historyStore(t)
	fixedHistoryClock(st, 100, 200)
	if err := st.Bootstrap("thread-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	old := prepareRotation(t, st, "rotate-1", "thread-b")
	if err := st.exec("UPDATE main_epochs SET generation=9 WHERE generation=1"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CommitMainRotation("rotate-1", old, "thread-b"); err == nil {
		t.Fatal("corrupt predecessor accepted")
	}
	current, err := st.MainRef()
	if err != nil || current != old {
		t.Fatalf("alias CAS was not rolled back: %+v err=%v", current, err)
	}
}

func TestHistoryPersistsAcrossRestartAndCorruptionBlocksOpen(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(state, "router.sqlite3")
	key := make([]byte, 32)
	st, err := Open(path, "test-persona", key)
	if err != nil {
		t.Fatal(err)
	}
	fixedHistoryClock(st, 100)
	if err := st.Bootstrap("thread-a", nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st, err = Open(path, "test-persona", key)
	if err != nil {
		t.Fatalf("durable history did not reopen: %v", err)
	}
	history, err := st.MainEpochHistory(100, 200)
	if err != nil || len(history.Epochs) != 1 || history.Epochs[0].ThreadID != "thread-a" {
		t.Fatalf("durable history changed: %+v err=%v", history, err)
	}
	if err := st.exec("UPDATE main_epochs SET epoch_digest='ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff' WHERE generation=1"); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if reopened, err := Open(path, "test-persona", key); err == nil {
		_ = reopened.Close()
		t.Fatal("corrupt durable history reopened")
	}
}
