package store

/*
#include <sqlite3.h>
*/
import "C"

import (
	"crypto/hmac"
	"errors"
	"fmt"
	"strconv"
	"time"
	"unsafe"

	"codex-persona-router/internal/model"
)

const historySchema = `
CREATE TABLE main_history_baseline(singleton INTEGER PRIMARY KEY CHECK(singleton=1),generation INTEGER NOT NULL CHECK(generation>0),thread_id TEXT NOT NULL,observed_from_ns INTEGER NOT NULL CHECK(observed_from_ns>0),baseline_digest TEXT NOT NULL);
CREATE TABLE main_epochs(generation INTEGER PRIMARY KEY CHECK(generation>0),thread_id TEXT NOT NULL,effective_from_ns INTEGER NOT NULL CHECK(effective_from_ns>0),transition_kind TEXT NOT NULL CHECK(transition_kind IN('initialization','rotation','rollback')),operation_id TEXT NOT NULL,previous_epoch_digest TEXT NOT NULL,epoch_digest TEXT NOT NULL UNIQUE,UNIQUE(operation_id,transition_kind));
`

func validTransitionKind(kind string) bool {
	switch kind {
	case "initialization", "rotation", "rollback":
		return true
	default:
		return false
	}
}

func (s *Store) baselineDigest(generation int64, threadID string, observedFromNS int64) string {
	return s.Digest(
		"main_history_baseline_v1", s.persona,
		strconv.FormatInt(generation, 10), threadID,
		strconv.FormatInt(observedFromNS, 10),
	)
}

func (s *Store) epochDigest(generation int64, threadID string, effectiveFromNS int64, kind, previous string) string {
	return s.Digest(
		"main_epoch_v1", s.persona,
		strconv.FormatInt(generation, 10), threadID,
		strconv.FormatInt(effectiveFromNS, 10), kind, previous,
	)
}

// migrateHistoryV5 creates an exact-history boundary without claiming that the
// migration observation is the original commit time of an existing main.
func (s *Store) migrateHistoryV5() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	version, _, err := s.queryInt("PRAGMA user_version")
	if err != nil || version != 4 {
		return fmt.Errorf("history migration source rejected")
	}
	for _, table := range []string{"main_history_baseline", "main_epochs"} {
		count, _, err := s.queryInt("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table)
		if err != nil || count != 0 {
			return fmt.Errorf("history migration target rejected")
		}
	}
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	if err := s.exec(historySchema); err != nil {
		return fmt.Errorf("history schema migration failed")
	}
	main, found, err := s.mainRefLocked()
	if err != nil {
		return err
	}
	if found {
		observed := s.clockNow().UnixNano()
		if observed <= 0 {
			return fmt.Errorf("history migration clock rejected")
		}
		digest := s.baselineDigest(main.Generation, main.ThreadID, observed)
		if err := s.execParams(`INSERT INTO main_history_baseline
			(singleton,generation,thread_id,observed_from_ns,baseline_digest)
			VALUES(1,?,?,?,?)`, main.Generation, main.ThreadID, observed, digest); err != nil {
			return fmt.Errorf("history baseline migration failed")
		}
	}
	if err := s.exec("UPDATE metadata SET value='5' WHERE key='schema_version'; PRAGMA user_version=5;"); err != nil {
		return fmt.Errorf("history version migration failed")
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Store) validateHistorySchema() error {
	for _, table := range []string{"main_history_baseline", "main_epochs"} {
		count, _, err := s.queryInt("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table)
		if err != nil || count != 1 {
			return fmt.Errorf("main history schema rejected")
		}
	}
	for _, query := range []string{
		"SELECT singleton,generation,thread_id,observed_from_ns,baseline_digest FROM main_history_baseline LIMIT 0",
		"SELECT generation,thread_id,effective_from_ns,transition_kind,operation_id,previous_epoch_digest,epoch_digest FROM main_epochs LIMIT 0",
	} {
		statement, err := s.prepare(query)
		if err != nil {
			return fmt.Errorf("main history schema rejected")
		}
		C.sqlite3_finalize(statement)
	}
	return nil
}

func (s *Store) validateHistoryState() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _, _, err := s.validateHistoryLocked()
	if errors.Is(err, model.ErrHistoryIncomplete) {
		// A never-initialized store has neither a protected main nor history.
		return nil
	}
	return err
}

func (s *Store) clockNow() time.Time {
	if s.now == nil {
		return time.Now()
	}
	return s.now()
}

func (s *Store) mainRefLocked() (model.MainRef, bool, error) {
	st, err := s.prepare("SELECT thread_id,generation FROM thread_aliases WHERE alias='main' AND protected=1 AND status='active'")
	if err != nil {
		return model.MainRef{}, false, err
	}
	defer C.sqlite3_finalize(st)
	rc := C.sqlite3_step(st)
	switch rc {
	case C.SQLITE_DONE:
		return model.MainRef{}, false, nil
	case C.SQLITE_ROW:
		return model.MainRef{
			ThreadID:   C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(st, 0)))),
			Generation: int64(C.sqlite3_column_int64(st, 1)),
		}, true, nil
	default:
		return model.MainRef{}, false, historyReadError(rc)
	}
}

func (s *Store) baselineLocked() (*model.MainHistoryBaseline, error) {
	st, err := s.prepare("SELECT generation,thread_id,observed_from_ns,baseline_digest FROM main_history_baseline WHERE singleton=1")
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(st)
	rc := C.sqlite3_step(st)
	switch rc {
	case C.SQLITE_DONE:
		return nil, nil
	case C.SQLITE_ROW:
		return &model.MainHistoryBaseline{
			Generation:     int64(C.sqlite3_column_int64(st, 0)),
			ThreadID:       sqliteText(st, 1),
			ObservedFromNS: int64(C.sqlite3_column_int64(st, 2)),
			Digest:         sqliteText(st, 3),
		}, nil
	default:
		return nil, historyReadError(rc)
	}
}

func sqliteText(st *C.sqlite3_stmt, column int) string {
	p := C.sqlite3_column_text(st, C.int(column))
	if p == nil {
		return ""
	}
	return C.GoString((*C.char)(unsafe.Pointer(p)))
}

func (s *Store) epochsLocked() ([]model.MainEpoch, error) {
	st, err := s.prepare(`SELECT generation,thread_id,effective_from_ns,transition_kind,
		previous_epoch_digest,epoch_digest FROM main_epochs ORDER BY generation`)
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(st)
	var result []model.MainEpoch
	for {
		rc := C.sqlite3_step(st)
		switch rc {
		case C.SQLITE_DONE:
			return result, nil
		case C.SQLITE_ROW:
			result = append(result, model.MainEpoch{
				Generation:      int64(C.sqlite3_column_int64(st, 0)),
				ThreadID:        sqliteText(st, 1),
				EffectiveFromNS: int64(C.sqlite3_column_int64(st, 2)),
				TransitionKind:  sqliteText(st, 3),
				PreviousDigest:  sqliteText(st, 4),
				Digest:          sqliteText(st, 5),
			})
		default:
			return nil, historyReadError(rc)
		}
	}
}

func historyReadError(rc C.int) error {
	if rc == C.SQLITE_BUSY || rc == C.SQLITE_LOCKED {
		return model.ErrHistoryBusy
	}
	return fmt.Errorf("main history read failed")
}

// appendMainEpochLocked must run inside the same BEGIN IMMEDIATE transaction
// that mutates the protected main alias. It rejects any discontinuity rather
// than repairing or guessing history.
func (s *Store) appendMainEpochLocked(generation int64, threadID, kind, operationID string) error {
	if generation < 1 || threadID == "" || operationID == "" || !validTransitionKind(kind) {
		return model.ErrUnsafeTransition
	}
	baseline, err := s.baselineLocked()
	if err != nil {
		return err
	}
	epochs, err := s.epochsLocked()
	if err != nil {
		return err
	}
	expectedGeneration := int64(1)
	previousDigest := ""
	previousTime := int64(0)
	if baseline != nil {
		expectedGeneration = baseline.Generation + 1
		previousDigest = baseline.Digest
		previousTime = baseline.ObservedFromNS
	}
	if len(epochs) > 0 {
		last := epochs[len(epochs)-1]
		expectedGeneration = last.Generation + 1
		previousDigest = last.Digest
		previousTime = last.EffectiveFromNS
	}
	if generation != expectedGeneration {
		return model.ErrHistoryCorrupt
	}
	effectiveFrom := s.clockNow().UnixNano()
	if effectiveFrom <= previousTime {
		return fmt.Errorf("main history clock rejected")
	}
	digest := s.epochDigest(generation, threadID, effectiveFrom, kind, previousDigest)
	if err := s.execParams(`INSERT INTO main_epochs
		(generation,thread_id,effective_from_ns,transition_kind,operation_id,
		previous_epoch_digest,epoch_digest) VALUES(?,?,?,?,?,?,?)`,
		generation, threadID, effectiveFrom, kind, operationID, previousDigest, digest); err != nil {
		return fmt.Errorf("main history append failed")
	}
	return nil
}

func (s *Store) validateHistoryLocked() (*model.MainHistoryBaseline, []model.MainEpoch, model.MainRef, error) {
	baseline, err := s.baselineLocked()
	if err != nil {
		return nil, nil, model.MainRef{}, err
	}
	epochs, err := s.epochsLocked()
	if err != nil {
		return nil, nil, model.MainRef{}, err
	}
	main, found, err := s.mainRefLocked()
	if err != nil {
		return nil, nil, model.MainRef{}, err
	}
	if !found {
		if baseline != nil || len(epochs) != 0 {
			return nil, nil, model.MainRef{}, model.ErrHistoryCorrupt
		}
		return nil, nil, model.MainRef{}, model.ErrHistoryIncomplete
	}

	expectedGeneration := int64(1)
	previousDigest := ""
	previousTime := int64(0)
	headThread := ""
	headGeneration := int64(0)
	if baseline != nil {
		if baseline.Generation < 1 || baseline.ThreadID == "" || baseline.ObservedFromNS <= 0 ||
			!hmac.Equal([]byte(baseline.Digest), []byte(s.baselineDigest(
				baseline.Generation, baseline.ThreadID, baseline.ObservedFromNS))) {
			return nil, nil, model.MainRef{}, model.ErrHistoryCorrupt
		}
		expectedGeneration = baseline.Generation + 1
		previousDigest = baseline.Digest
		previousTime = baseline.ObservedFromNS
		headThread, headGeneration = baseline.ThreadID, baseline.Generation
	}
	for i := range epochs {
		epoch := &epochs[i]
		if epoch.Generation != expectedGeneration || epoch.ThreadID == "" ||
			epoch.EffectiveFromNS <= previousTime || !validTransitionKind(epoch.TransitionKind) ||
			epoch.PreviousDigest != previousDigest {
			return nil, nil, model.MainRef{}, model.ErrHistoryCorrupt
		}
		expectedDigest := s.epochDigest(epoch.Generation, epoch.ThreadID,
			epoch.EffectiveFromNS, epoch.TransitionKind, epoch.PreviousDigest)
		if !hmac.Equal([]byte(epoch.Digest), []byte(expectedDigest)) {
			return nil, nil, model.MainRef{}, model.ErrHistoryCorrupt
		}
		if i+1 < len(epochs) {
			epoch.EffectiveToNS = epochs[i+1].EffectiveFromNS
		}
		previousDigest = epoch.Digest
		previousTime = epoch.EffectiveFromNS
		expectedGeneration++
		headThread, headGeneration = epoch.ThreadID, epoch.Generation
	}
	if headGeneration == 0 || main.Generation != headGeneration || main.ThreadID != headThread {
		return nil, nil, model.MainRef{}, model.ErrHistoryCorrupt
	}
	if baseline != nil && len(epochs) > 0 {
		baseline.ObservedToNS = epochs[0].EffectiveFromNS
	}
	return baseline, epochs, main, nil
}

// MainEpochHistory returns only a fully covered interval. The caller receives
// no partial data on legacy gaps or integrity failure.
func (s *Store) MainEpochHistory(fromNS, toNS int64) (model.MainEpochHistory, error) {
	if fromNS < 0 || toNS <= fromNS {
		return model.MainEpochHistory{}, fmt.Errorf("invalid history range")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	baseline, epochs, _, err := s.validateHistoryLocked()
	if err != nil {
		return model.MainEpochHistory{}, err
	}

	history := model.MainEpochHistory{Persona: s.persona}
	if baseline != nil {
		copyBaseline := *baseline
		history.Origin = "migration_baseline"
		history.AvailableFromNS = baseline.ObservedFromNS
		history.PriorHistoryComplete = false
		history.Baseline = &copyBaseline
		history.HeadGeneration = baseline.Generation
		history.HeadDigest = baseline.Digest
	} else {
		if len(epochs) == 0 || epochs[0].Generation != 1 || epochs[0].TransitionKind != "initialization" {
			return model.MainEpochHistory{}, model.ErrHistoryCorrupt
		}
		history.Origin = "exact_initialization"
		history.AvailableFromNS = epochs[0].EffectiveFromNS
		history.PriorHistoryComplete = true
	}
	if fromNS < history.AvailableFromNS {
		return model.MainEpochHistory{}, model.ErrHistoryIncomplete
	}
	if len(epochs) > 0 {
		last := epochs[len(epochs)-1]
		history.HeadGeneration, history.HeadDigest = last.Generation, last.Digest
	}
	for _, epoch := range epochs {
		if epoch.EffectiveFromNS < toNS && (epoch.EffectiveToNS == 0 || epoch.EffectiveToNS > fromNS) {
			history.Epochs = append(history.Epochs, epoch)
		}
	}
	if !historyCoversRange(history, fromNS, toNS) {
		return model.MainEpochHistory{}, model.ErrHistoryIncomplete
	}
	return history, nil
}

func historyCoversRange(history model.MainEpochHistory, fromNS, toNS int64) bool {
	cursor := fromNS
	if history.Baseline != nil {
		end := history.Baseline.ObservedToNS
		if history.Baseline.ObservedFromNS <= cursor && (end == 0 || end > cursor) {
			if end == 0 || end >= toNS {
				return true
			}
			cursor = end
		}
	}
	for _, epoch := range history.Epochs {
		if epoch.EffectiveFromNS > cursor {
			return false
		}
		if epoch.EffectiveToNS == 0 || epoch.EffectiveToNS >= toNS {
			return true
		}
		if epoch.EffectiveToNS > cursor {
			cursor = epoch.EffectiveToNS
		}
	}
	return cursor >= toNS
}
