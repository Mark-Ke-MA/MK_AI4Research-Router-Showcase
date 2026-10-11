package store

/*
#cgo darwin LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>
static int bind_text_copy(sqlite3_stmt *s, int i, const char *v) {
	return sqlite3_bind_text(s, i, v, -1, SQLITE_TRANSIENT);
}
static int bind_blob_copy(sqlite3_stmt *s, int i, const void *v, int n) {
	return sqlite3_bind_blob(s, i, v, n, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"codex-persona-router/internal/model"
	"codex-persona-router/internal/securefs"
)

type Store struct {
	mu        sync.Mutex
	db        *C.sqlite3
	persona   string
	key       []byte
	path      string
	retryBase time.Duration
	now       func() time.Time
}
type OwnerBinding struct{ Platform, Account, Owner string }
type EndpointBinding struct{ Platform, Account, Conversation, Topic, Alias string }

const schemaVersion = 5
const initialMainSentinel = "__initial_main__"

func Open(path, persona string, digestKey []byte) (*Store, error) {
	if persona == "" || len(digestKey) < 32 {
		return nil, fmt.Errorf("invalid persona or digest key")
	}
	if err := securefs.EnsurePrivateDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	if err := securefs.CheckPrivateFile(path, true); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		f, createErr := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if createErr != nil {
			return nil, createErr
		}
		if closeErr := f.Close(); closeErr != nil {
			return nil, closeErr
		}
	} else if err != nil {
		return nil, err
	}
	canonicalParent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("sqlite parent resolution failed")
	}
	openPath := filepath.Join(canonicalParent, filepath.Base(path))
	cp := C.CString(openPath)
	defer C.free(unsafe.Pointer(cp))
	var db *C.sqlite3
	flags := C.int(C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_CREATE | C.SQLITE_OPEN_FULLMUTEX | C.SQLITE_OPEN_NOFOLLOW)
	if C.sqlite3_open_v2(cp, &db, flags, nil) != C.SQLITE_OK {
		if db != nil {
			C.sqlite3_close(db)
		}
		return nil, fmt.Errorf("sqlite open failed")
	}
	s := &Store{db: db, persona: persona, key: append([]byte(nil), digestKey...), path: path, retryBase: time.Second, now: time.Now}
	if err := s.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;"); err != nil {
		s.Close()
		return nil, err
	}
	version, _, err := s.queryInt("PRAGMA user_version")
	if err != nil {
		s.Close()
		return nil, err
	}
	switch version {
	case 0:
		tables, _, err := s.queryInt("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
		if err != nil || tables != 0 {
			s.Close()
			return nil, fmt.Errorf("unversioned non-empty database rejected")
		}
		if err := s.exec("BEGIN IMMEDIATE;" + schema + "INSERT INTO metadata(key,value) VALUES('schema_version','5'); PRAGMA user_version=5; COMMIT;"); err != nil {
			_ = s.exec("ROLLBACK")
			s.Close()
			return nil, err
		}
	case 1, 2:
		recorded, found, err := s.queryText("SELECT value FROM metadata WHERE key='schema_version'")
		if err != nil || !found || recorded != fmt.Sprint(version) {
			s.Close()
			return nil, fmt.Errorf("schema metadata rejected")
		}
		hasMediaPaths, err := s.hasColumn("turn_jobs", "media_paths")
		if err != nil {
			s.Close()
			return nil, err
		}
		hasHardLease, err := s.hasColumn("media_leases", "lease_hard_expires_ms")
		if err != nil {
			s.Close()
			return nil, err
		}
		hasReferences, err := s.hasColumn("media_leases", "reference_count")
		if err != nil {
			s.Close()
			return nil, err
		}
		hasNextAttempt, err := s.hasColumn("turn_jobs", "next_attempt_ms")
		if err != nil {
			s.Close()
			return nil, err
		}
		migration := "BEGIN IMMEDIATE;"
		if !hasMediaPaths {
			migration += " ALTER TABLE turn_jobs ADD COLUMN media_paths BLOB;"
		}
		if !hasHardLease {
			migration += " ALTER TABLE media_leases ADD COLUMN lease_hard_expires_ms INTEGER NOT NULL DEFAULT 0;"
		}
		if !hasReferences {
			migration += " ALTER TABLE media_leases ADD COLUMN reference_count INTEGER NOT NULL DEFAULT 0;"
		}
		if !hasNextAttempt {
			migration += " ALTER TABLE turn_jobs ADD COLUMN next_attempt_ms INTEGER NOT NULL DEFAULT 0;"
		}
		migration += " CREATE TABLE IF NOT EXISTS owner_bindings(platform TEXT NOT NULL,account_digest TEXT NOT NULL,owner_digest TEXT NOT NULL,PRIMARY KEY(platform,account_digest));"
		migration += " CREATE TABLE IF NOT EXISTS reconciliations(id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL REFERENCES turn_jobs(id),evidence_digest TEXT NOT NULL,disposition TEXT NOT NULL,created_ms INTEGER NOT NULL);"
		migration += " CREATE TABLE IF NOT EXISTS activity_indicators(job_id TEXT PRIMARY KEY REFERENCES turn_jobs(id),platform TEXT NOT NULL,endpoint_digest TEXT NOT NULL,target BLOB,state TEXT NOT NULL,reaction_id BLOB,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ms INTEGER NOT NULL DEFAULT 0,created_ms INTEGER NOT NULL,expires_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);"
		migration += " CREATE INDEX IF NOT EXISTS activity_due ON activity_indicators(state,next_attempt_ms);"
		migration += " CREATE TABLE IF NOT EXISTS main_rotations(operation_id TEXT PRIMARY KEY,old_thread_id TEXT NOT NULL,new_thread_id TEXT,old_generation INTEGER NOT NULL,committed_generation INTEGER NOT NULL DEFAULT 0,phase TEXT NOT NULL,cleanup_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_incomplete IN(0,1)),created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);"
		migration += " UPDATE metadata SET value='4' WHERE key='schema_version'; PRAGMA user_version=4; COMMIT;"
		if err := s.exec(migration); err != nil {
			_ = s.exec("ROLLBACK")
			s.Close()
			return nil, fmt.Errorf("schema migration failed")
		}
	case 3:
		recorded, found, err := s.queryText("SELECT value FROM metadata WHERE key='schema_version'")
		if err != nil || !found || recorded != "3" {
			s.Close()
			return nil, fmt.Errorf("schema metadata rejected")
		}
		if err := s.exec("BEGIN IMMEDIATE; CREATE TABLE IF NOT EXISTS activity_indicators(job_id TEXT PRIMARY KEY REFERENCES turn_jobs(id),platform TEXT NOT NULL,endpoint_digest TEXT NOT NULL,target BLOB,state TEXT NOT NULL,reaction_id BLOB,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ms INTEGER NOT NULL DEFAULT 0,created_ms INTEGER NOT NULL,expires_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL); CREATE INDEX IF NOT EXISTS activity_due ON activity_indicators(state,next_attempt_ms); CREATE TABLE IF NOT EXISTS main_rotations(operation_id TEXT PRIMARY KEY,old_thread_id TEXT NOT NULL,new_thread_id TEXT,old_generation INTEGER NOT NULL,committed_generation INTEGER NOT NULL DEFAULT 0,phase TEXT NOT NULL,cleanup_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_incomplete IN(0,1)),created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL); UPDATE metadata SET value='4' WHERE key='schema_version'; PRAGMA user_version=4; COMMIT;"); err != nil {
			_ = s.exec("ROLLBACK")
			s.Close()
			return nil, fmt.Errorf("control-plane migration failed")
		}
	case 4:
		recorded, found, err := s.queryText("SELECT value FROM metadata WHERE key='schema_version'")
		if err != nil || !found || recorded != "4" {
			s.Close()
			return nil, fmt.Errorf("schema metadata rejected")
		}
		if err := s.validateControlPlaneSchema(); err != nil {
			s.Close()
			return nil, err
		}
		if err := s.setPersona(); err != nil {
			s.Close()
			return nil, err
		}
		if err := s.migrateHistoryV5(); err != nil {
			s.Close()
			return nil, err
		}
	case schemaVersion:
		recorded, found, err := s.queryText("SELECT value FROM metadata WHERE key='schema_version'")
		if err != nil || !found || recorded != "5" {
			s.Close()
			return nil, fmt.Errorf("schema metadata rejected")
		}
		if err := s.validateHistorySchema(); err != nil {
			s.Close()
			return nil, err
		}
		if err := s.validateControlPlaneSchema(); err != nil {
			s.Close()
			return nil, err
		}
	default:
		s.Close()
		return nil, fmt.Errorf("unsupported schema version")
	}
	if err := s.setPersona(); err != nil {
		s.Close()
		return nil, err
	}
	currentVersion, _, err := s.queryInt("PRAGMA user_version")
	if err != nil {
		s.Close()
		return nil, err
	}
	if currentVersion == 4 {
		if err := s.migrateHistoryV5(); err != nil {
			s.Close()
			return nil, err
		}
	}
	if err := s.validateHistoryState(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) validateControlPlaneSchema() error {
	for _, table := range []string{"activity_indicators", "main_rotations"} {
		count, _, err := s.queryInt("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table)
		if err != nil || count != 1 {
			return fmt.Errorf("control-plane schema rejected")
		}
	}
	return nil
}

func (s *Store) hasColumn(table, column string) (bool, error) {
	if table != "turn_jobs" && table != "media_leases" {
		return false, fmt.Errorf("unsupported schema table")
	}
	st, err := s.prepare("PRAGMA table_info(" + table + ")")
	if err != nil {
		return false, err
	}
	defer C.sqlite3_finalize(st)
	for {
		rc := C.sqlite3_step(st)
		if rc == C.SQLITE_DONE {
			return false, nil
		}
		if rc != C.SQLITE_ROW {
			return false, fmt.Errorf("schema inspection failed")
		}
		name := C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(st, 1))))
		if name == column {
			return true, nil
		}
	}
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() {
		for i := range s.key {
			s.key[i] = 0
		}
		s.key = nil
	}()
	if s.db == nil {
		return nil
	}
	rc := C.sqlite3_close(s.db)
	s.db = nil
	if rc != C.SQLITE_OK {
		return fmt.Errorf("sqlite close failed")
	}
	return nil
}

func (s *Store) Checkpoint() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return fmt.Errorf("closed")
	}
	if C.sqlite3_wal_checkpoint_v2(s.db, nil, C.SQLITE_CHECKPOINT_FULL, nil, nil) != C.SQLITE_OK {
		return fmt.Errorf("checkpoint failed")
	}
	return nil
}

func (s *Store) exec(sql string) error {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var msg *C.char
	if C.sqlite3_exec(s.db, cs, nil, nil, &msg) != C.SQLITE_OK {
		if msg != nil {
			C.sqlite3_free(unsafe.Pointer(msg))
		}
		return fmt.Errorf("sqlite operation failed")
	}
	return nil
}

func (s *Store) setPersona() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found, err := s.queryText("SELECT value FROM metadata WHERE key='persona'")
	if err != nil {
		return err
	}
	if found && current != s.persona {
		return model.ErrCrossPersona
	}
	if !found {
		return s.execParams("INSERT INTO metadata(key,value) VALUES('persona',?)", s.persona)
	}
	return nil
}

func (s *Store) Digest(parts ...string) string {
	mac := hmac.New(sha256.New, s.key)
	for _, p := range parts {
		mac.Write([]byte{0})
		mac.Write([]byte(p))
	}
	return hex.EncodeToString(mac.Sum(nil))
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func (s *Store) InitMain(threadID string) error {
	if strings.TrimSpace(threadID) == "" {
		return fmt.Errorf("thread id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias='main'")
	if err != nil {
		return err
	}
	if found {
		if existing != threadID {
			return model.ErrProtectedAlias
		}
		return nil
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
	if err := s.execParams("INSERT INTO thread_aliases(alias,thread_id,generation,protected,status) VALUES('main',?,1,1,'active')", threadID); err != nil {
		return err
	}
	if err := s.appendMainEpochLocked(1, threadID, "initialization", "store-init-main"); err != nil {
		return err
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Store) MainRef() (model.MainRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.prepare("SELECT thread_id,generation FROM thread_aliases WHERE alias='main' AND protected=1 AND status='active'")
	if err != nil {
		return model.MainRef{}, err
	}
	defer C.sqlite3_finalize(st)
	if C.sqlite3_step(st) != C.SQLITE_ROW {
		return model.MainRef{}, model.ErrEndpoint
	}
	return model.MainRef{
		ThreadID:   C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(st, 0)))),
		Generation: int64(C.sqlite3_column_int64(st, 1)),
	}, nil
}

// ControlPlaneQuiescent rejects every state that can still refer to the old
// protected main. Recoverable uncertainty also blocks rotation until an
// operator has explicitly reconciled it.
func (s *Store) ControlPlaneQuiescent(now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobs, _, err := s.queryInt("SELECT COUNT(*) FROM turn_jobs WHERE state NOT IN ('delivered','dead_letter')")
	if err != nil {
		return false, err
	}
	outbox, _, err := s.queryInt("SELECT COUNT(*) FROM outbox WHERE state!='delivered'")
	if err != nil {
		return false, err
	}
	activity, _, err := s.queryInt("SELECT COUNT(*) FROM activity_indicators")
	if err != nil {
		return false, err
	}
	external, _, err := s.queryInt("SELECT COUNT(*) FROM external_leases WHERE expires_ms>?", now.UnixMilli())
	if err != nil {
		return false, err
	}
	return jobs == 0 && outbox == 0 && activity == 0 && external == 0, nil
}

func validRotationPhase(phase string) bool {
	switch phase {
	case "prepared", "creating", "created", "initializing", "initialization_starting", "initialized", "committed", "verified",
		"cleanup_incomplete", "complete", "aborted", "aborted_cleanup_incomplete",
		"rolled_back", "rollback_cleanup_incomplete",
		"init_prepared", "init_creating", "init_created", "init_initializing",
		"init_initialization_starting", "init_initialized", "init_committed",
		"init_complete", "init_aborted", "init_aborted_cleanup_incomplete":
		return true
	default:
		return false
	}
}

// BeginMainInitialization records the one permitted creation attempt before
// thread/start. It reuses the durable control journal schema but gives every
// phase an init_ prefix so rotation and first-main recovery cannot be confused.
func (s *Store) BeginMainInitialization(operationID string) (model.Rotation, bool, error) {
	if strings.TrimSpace(operationID) == "" {
		return model.Rotation{}, false, fmt.Errorf("initialization request rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	existingOperation, _, err := s.queryInt(
		"SELECT COUNT(*) FROM main_rotations WHERE operation_id=?", operationID)
	if err != nil {
		return model.Rotation{}, false, err
	}
	if existingOperation == 1 {
		record, err := s.rotationLocked(operationID)
		if err != nil || record.OldThreadID != initialMainSentinel ||
			!strings.HasPrefix(record.Phase, "init_") {
			return model.Rotation{}, false, model.ErrUnsafeTransition
		}
		return record, false, nil
	}
	if _, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias='main'"); err != nil {
		return model.Rotation{}, false, err
	} else if found {
		return model.Rotation{}, false, model.ErrProtectedAlias
	}
	active, _, err := s.queryInt(`SELECT COUNT(*) FROM main_rotations
		WHERE old_thread_id=? AND phase NOT IN ('init_aborted') AND operation_id!=?`,
		initialMainSentinel, operationID)
	if err != nil || active != 0 {
		return model.Rotation{}, false, model.ErrUnsafeTransition
	}
	now := time.Now().UnixMilli()
	err = s.execParams(`INSERT INTO main_rotations
		(operation_id,old_thread_id,old_generation,phase,created_ms,updated_ms)
		VALUES(?,?,0,'init_prepared',?,?)`,
		operationID, initialMainSentinel, now, now)
	if err == nil {
		return model.Rotation{
			OperationID: operationID, OldThreadID: initialMainSentinel,
			Phase: "init_prepared",
		}, true, nil
	}
	if !s.isConstraint() {
		return model.Rotation{}, false, err
	}
	return model.Rotation{}, false, model.ErrUnsafeTransition
}

func (s *Store) AdvanceMainInitialization(operationID, expectedPhase, nextPhase, threadID string) error {
	if !strings.HasPrefix(expectedPhase, "init_") ||
		!strings.HasPrefix(nextPhase, "init_") {
		return fmt.Errorf("initialization transition rejected")
	}
	return s.AdvanceRotation(operationID, expectedPhase, nextPhase, threadID)
}

func (s *Store) MarkMainInitializationAborted(operationID, expectedPhase string, cleanupComplete bool) error {
	next := "init_aborted_cleanup_incomplete"
	incomplete := 1
	if cleanupComplete {
		next, incomplete = "init_aborted", 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.execParams(`UPDATE main_rotations
		SET phase=?,cleanup_incomplete=?,updated_ms=?
		WHERE operation_id=? AND old_thread_id=? AND phase=?`,
		next, incomplete, time.Now().UnixMilli(), operationID,
		initialMainSentinel, expectedPhase); err != nil {
		return err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.ErrUnsafeTransition
	}
	return nil
}

// CommitInitialMain atomically installs generation 1, its approved endpoint
// bindings and the committed journal phase. There is therefore no state in
// which platform traffic can resolve an unjournaled protected main.
func (s *Store) CommitInitialMain(operationID, threadID string, owners []OwnerBinding, endpoints []EndpointBinding) (model.MainRef, error) {
	if operationID == "" || strings.TrimSpace(threadID) == "" {
		return model.MainRef{}, fmt.Errorf("initial main commit rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return model.MainRef{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	if _, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias='main'"); err != nil {
		return model.MainRef{}, err
	} else if found {
		return model.MainRef{}, model.ErrProtectedAlias
	}
	if err := s.execParams(`INSERT INTO thread_aliases
		(alias,thread_id,generation,protected,status)
		VALUES('main',?,1,1,'active')`, threadID); err != nil {
		return model.MainRef{}, err
	}
	if err := s.applyInitialBindingsLocked(owners, endpoints); err != nil {
		return model.MainRef{}, err
	}
	if err := s.execParams(`UPDATE main_rotations
		SET phase='init_committed',committed_generation=1,updated_ms=?
		WHERE operation_id=? AND old_thread_id=? AND new_thread_id=?
		AND phase='init_initialized'`,
		time.Now().UnixMilli(), operationID, initialMainSentinel, threadID); err != nil {
		return model.MainRef{}, err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.MainRef{}, model.ErrUnsafeTransition
	}
	if err := s.appendMainEpochLocked(1, threadID, "initialization", operationID); err != nil {
		return model.MainRef{}, err
	}
	if err := s.exec("COMMIT"); err != nil {
		return model.MainRef{}, err
	}
	ok = true
	return model.MainRef{ThreadID: threadID, Generation: 1}, nil
}

func (s *Store) CompleteMainInitialization(operationID string) error {
	return s.AdvanceMainInitialization(operationID, "init_committed", "init_complete", "")
}

func (s *Store) MarkRotationAborted(operationID, expectedPhase string, cleanupComplete bool) error {
	next := "aborted_cleanup_incomplete"
	incomplete := 1
	if cleanupComplete {
		next, incomplete = "aborted", 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.execParams("UPDATE main_rotations SET phase=?,cleanup_incomplete=?,updated_ms=? WHERE operation_id=? AND phase=?",
		next, incomplete, time.Now().UnixMilli(), operationID, expectedPhase); err != nil {
		return err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.ErrUnsafeTransition
	}
	return nil
}

func (s *Store) BeginRotation(operationID string, old model.MainRef) (model.Rotation, bool, error) {
	if strings.TrimSpace(operationID) == "" || old.ThreadID == "" || old.Generation < 1 {
		return model.Rotation{}, false, fmt.Errorf("rotation request rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	currentThread, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias='main' AND generation=?", old.Generation)
	if err != nil || !found || currentThread != old.ThreadID {
		return model.Rotation{}, false, model.ErrProtectedAlias
	}
	now := time.Now().UnixMilli()
	err = s.execParams("INSERT INTO main_rotations(operation_id,old_thread_id,old_generation,phase,created_ms,updated_ms) VALUES(?,?,?,'prepared',?,?)",
		operationID, old.ThreadID, old.Generation, now, now)
	if err == nil {
		return model.Rotation{OperationID: operationID, OldThreadID: old.ThreadID, OldGeneration: old.Generation, Phase: "prepared"}, true, nil
	}
	if !s.isConstraint() {
		return model.Rotation{}, false, err
	}
	r, err := s.rotationLocked(operationID)
	return r, false, err
}

func (s *Store) Rotation(operationID string) (model.Rotation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotationLocked(operationID)
}

func (s *Store) rotationLocked(operationID string) (model.Rotation, error) {
	st, err := s.prepare("SELECT operation_id,old_thread_id,COALESCE(new_thread_id,''),old_generation,committed_generation,phase,cleanup_incomplete FROM main_rotations WHERE operation_id=?")
	if err != nil {
		return model.Rotation{}, err
	}
	defer C.sqlite3_finalize(st)
	if err := bind(st, operationID); err != nil {
		return model.Rotation{}, err
	}
	if C.sqlite3_step(st) != C.SQLITE_ROW {
		return model.Rotation{}, fmt.Errorf("rotation record missing")
	}
	text := func(i int) string {
		return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(st, C.int(i)))))
	}
	return model.Rotation{
		OperationID: operationID, OldThreadID: text(1), NewThreadID: text(2),
		OldGeneration:       int64(C.sqlite3_column_int64(st, 3)),
		CommittedGeneration: int64(C.sqlite3_column_int64(st, 4)),
		Phase:               text(5), CleanupIncomplete: C.sqlite3_column_int(st, 6) == 1,
	}, nil
}

func (s *Store) AdvanceRotation(operationID, expectedPhase, nextPhase, newThreadID string) error {
	if !validRotationPhase(expectedPhase) || !validRotationPhase(nextPhase) {
		return fmt.Errorf("rotation transition rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if newThreadID == "" {
		if err := s.execParams("UPDATE main_rotations SET phase=?,updated_ms=? WHERE operation_id=? AND phase=?",
			nextPhase, time.Now().UnixMilli(), operationID, expectedPhase); err != nil {
			return err
		}
	} else if err := s.execParams("UPDATE main_rotations SET new_thread_id=?,phase=?,updated_ms=? WHERE operation_id=? AND phase=?",
		newThreadID, nextPhase, time.Now().UnixMilli(), operationID, expectedPhase); err != nil {
		return err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.ErrUnsafeTransition
	}
	return nil
}

// CommitMainRotation is the only protected-main mutation. The mapping CAS and
// rollback record phase change are one SQLite transaction.
func (s *Store) CommitMainRotation(operationID string, old model.MainRef, newThreadID string) (model.MainRef, error) {
	if newThreadID == "" {
		return model.MainRef{}, fmt.Errorf("rotation commit rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return model.MainRef{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	if err := s.execParams(`UPDATE thread_aliases SET thread_id=?,generation=generation+1
		WHERE alias='main' AND protected=1 AND status='active' AND thread_id=? AND generation=?`,
		newThreadID, old.ThreadID, old.Generation); err != nil {
		return model.MainRef{}, err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.MainRef{}, model.ErrProtectedAlias
	}
	nextGeneration := old.Generation + 1
	if err := s.execParams(`UPDATE main_rotations SET phase='committed',committed_generation=?,updated_ms=?
		WHERE operation_id=? AND phase='initialized' AND old_thread_id=? AND old_generation=? AND new_thread_id=?`,
		nextGeneration, time.Now().UnixMilli(), operationID, old.ThreadID, old.Generation, newThreadID); err != nil {
		return model.MainRef{}, err
	}
	changed, _, err = s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.MainRef{}, model.ErrUnsafeTransition
	}
	if err := s.appendMainEpochLocked(nextGeneration, newThreadID, "rotation", operationID); err != nil {
		return model.MainRef{}, err
	}
	if err := s.exec("COMMIT"); err != nil {
		return model.MainRef{}, err
	}
	ok = true
	return model.MainRef{ThreadID: newThreadID, Generation: nextGeneration}, nil
}

func (s *Store) MarkRotationCleanup(operationID, expectedPhase string, complete bool) error {
	next := "cleanup_incomplete"
	incomplete := 1
	if complete {
		next, incomplete = "complete", 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.execParams("UPDATE main_rotations SET phase=?,cleanup_incomplete=?,updated_ms=? WHERE operation_id=? AND phase=?",
		next, incomplete, time.Now().UnixMilli(), operationID, expectedPhase); err != nil {
		return err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.ErrUnsafeTransition
	}
	return nil
}

func (s *Store) RollbackMainRotation(operationID string) (model.MainRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.rotationLocked(operationID)
	if err != nil || r.NewThreadID == "" || r.CommittedGeneration < 1 {
		return model.MainRef{}, fmt.Errorf("rotation rollback rejected")
	}
	switch r.Phase {
	case "committed", "verified", "cleanup_incomplete", "complete":
	default:
		return model.MainRef{}, fmt.Errorf("rotation rollback rejected")
	}
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return model.MainRef{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	if err := s.execParams(`UPDATE thread_aliases SET thread_id=?,generation=generation+1
		WHERE alias='main' AND protected=1 AND status='active' AND thread_id=? AND generation=?`,
		r.OldThreadID, r.NewThreadID, r.CommittedGeneration); err != nil {
		return model.MainRef{}, err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.MainRef{}, model.ErrProtectedAlias
	}
	if err := s.execParams("UPDATE main_rotations SET phase='rolled_back',cleanup_incomplete=0,updated_ms=? WHERE operation_id=?",
		time.Now().UnixMilli(), operationID); err != nil {
		return model.MainRef{}, err
	}
	rollbackGeneration := r.CommittedGeneration + 1
	if err := s.appendMainEpochLocked(rollbackGeneration, r.OldThreadID, "rollback", operationID); err != nil {
		return model.MainRef{}, err
	}
	if err := s.exec("COMMIT"); err != nil {
		return model.MainRef{}, err
	}
	ok = true
	return model.MainRef{ThreadID: r.OldThreadID, Generation: rollbackGeneration}, nil
}

func (s *Store) MarkRollbackCleanupIncomplete(operationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.execParams("UPDATE main_rotations SET phase='rollback_cleanup_incomplete',cleanup_incomplete=1,updated_ms=? WHERE operation_id=? AND phase='rolled_back'",
		time.Now().UnixMilli(), operationID); err != nil {
		return err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.ErrUnsafeTransition
	}
	return nil
}

func (s *Store) MarkRollbackCleanupComplete(operationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.execParams("UPDATE main_rotations SET phase='rolled_back',cleanup_incomplete=0,updated_ms=? WHERE operation_id=? AND phase='rollback_cleanup_incomplete'",
		time.Now().UnixMilli(), operationID); err != nil {
		return err
	}
	changed, _, err := s.queryInt("SELECT changes()")
	if err != nil || changed != 1 {
		return model.ErrUnsafeTransition
	}
	return nil
}
func (s *Store) Bootstrap(threadID string, owners []OwnerBinding, endpoints []EndpointBinding) error {
	if strings.TrimSpace(threadID) == "" {
		return fmt.Errorf("thread id required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	existing, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias='main'")
	if err != nil {
		return err
	}
	if found && existing != threadID {
		return model.ErrProtectedAlias
	}
	if !found {
		if err := s.execParams("INSERT INTO thread_aliases(alias,thread_id,generation,protected,status) VALUES('main',?,1,1,'active')", threadID); err != nil {
			return err
		}
		if err := s.appendMainEpochLocked(1, threadID, "initialization", "bootstrap"); err != nil {
			return err
		}
	}
	if err := s.applyInitialBindingsLocked(owners, endpoints); err != nil {
		return err
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Store) applyInitialBindingsLocked(owners []OwnerBinding, endpoints []EndpointBinding) error {
	for _, binding := range owners {
		if binding.Platform == "" || binding.Account == "" || binding.Owner == "" {
			return model.ErrEndpoint
		}
		ad, od := s.Digest(binding.Platform, binding.Account), s.Digest(binding.Platform, binding.Account, binding.Owner)
		current, found, err := s.queryText("SELECT owner_digest FROM owner_bindings WHERE platform=? AND account_digest=?", binding.Platform, ad)
		if err != nil {
			return err
		}
		if found && current != od {
			return model.ErrOwnerConflict
		}
		if !found {
			if err := s.execParams("INSERT INTO owner_bindings(platform,account_digest,owner_digest) VALUES(?,?,?)", binding.Platform, ad, od); err != nil {
				return err
			}
		}
	}
	for _, binding := range endpoints {
		alias := binding.Alias
		if alias == "" {
			alias = "main"
		}
		if alias != "main" {
			return model.ErrProtectedAlias
		}
		ad, cd, td := s.Digest(binding.Platform, binding.Account), s.Digest(binding.Platform, binding.Conversation), s.Digest(binding.Platform, binding.Topic)
		current, found, err := s.queryText("SELECT alias FROM endpoint_bindings WHERE platform=? AND account_digest=? AND conversation_digest=? AND topic_digest=?", binding.Platform, ad, cd, td)
		if err != nil {
			return err
		}
		if found && current != alias {
			return model.ErrProtectedAlias
		}
		if !found {
			if err := s.execParams("INSERT INTO endpoint_bindings(platform,account_digest,conversation_digest,topic_digest,alias) VALUES(?,?,?,?,?)", binding.Platform, ad, cd, td, alias); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) SetAlias(alias, threadID string, replace bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	protected, found, err := s.queryInt("SELECT protected FROM thread_aliases WHERE alias=?", alias)
	if err != nil {
		return err
	}
	if found && protected == 1 {
		return model.ErrProtectedAlias
	}
	if found && !replace {
		return fmt.Errorf("alias exists")
	}
	if found {
		return s.execParams("UPDATE thread_aliases SET thread_id=?,generation=generation+1 WHERE alias=?", threadID, alias)
	}
	return s.execParams("INSERT INTO thread_aliases(alias,thread_id,generation,protected,status) VALUES(?,?,1,0,'active')", alias, threadID)
}

func (s *Store) BindEndpoint(platform, account, conversation, topic, alias string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias=? AND status='active'", alias)
	if err != nil {
		return err
	}
	if !found {
		return model.ErrEndpoint
	}
	ad, cd, td := s.Digest(platform, account), s.Digest(platform, conversation), s.Digest(platform, topic)
	return s.execParams(`INSERT INTO endpoint_bindings(platform,account_digest,conversation_digest,topic_digest,alias)
		VALUES(?,?,?,?,?) ON CONFLICT(platform,account_digest,conversation_digest,topic_digest)
		DO UPDATE SET alias=excluded.alias`, platform, ad, cd, td, alias)
}
func (s *Store) BindOwner(platform, account, owner string) error {
	if platform == "" || account == "" || owner == "" {
		return model.ErrEndpoint
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ad, od := s.Digest(platform, account), s.Digest(platform, account, owner)
	existing, found, err := s.queryText("SELECT owner_digest FROM owner_bindings WHERE platform=? AND account_digest=?", platform, ad)
	if err != nil {
		return err
	}
	if found {
		if existing != od {
			return model.ErrOwnerConflict
		}
		return nil
	}
	return s.execParams("INSERT INTO owner_bindings(platform,account_digest,owner_digest) VALUES(?,?,?)", platform, ad, od)
}
func (s *Store) OwnerAuthorized(platform, account, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	expected, found, err := s.queryText("SELECT owner_digest FROM owner_bindings WHERE platform=? AND account_digest=?", platform, s.Digest(platform, account))
	if err != nil || !found {
		return false, err
	}
	return hmac.Equal([]byte(expected), []byte(s.Digest(platform, account, owner))), nil
}

func (s *Store) ResolveEndpoint(platform, account, conversation, topic string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ad, cd, td := s.Digest(platform, account), s.Digest(platform, conversation), s.Digest(platform, topic)
	alias, found, err := s.queryText(`SELECT alias FROM endpoint_bindings WHERE platform=? AND account_digest=? AND conversation_digest=? AND topic_digest=?`, platform, ad, cd, td)
	if err != nil {
		return "", err
	}
	if !found {
		return "", model.ErrEndpoint
	}
	return alias, nil
}

func (s *Store) Enqueue(platform, account, eventKey, alias string, payload, deliveryTarget []byte, mediaPaths ...string) (model.Job, error) {
	return s.enqueue(platform, account, eventKey, alias, "", payload, deliveryTarget, mediaPaths...)
}

// EnqueueInbound persists the endpoint activity key in the same transaction as
// the accepted job. Its digest excludes source-message locators, so concurrent
// inputs in one authenticated chat/topic share one Telegram keepalive.
func (s *Store) EnqueueInbound(in model.Inbound, alias string, activityEnabled bool, activityTTL time.Duration) (model.Job, error) {
	if !activityEnabled {
		return s.enqueue(in.Platform, in.Account, in.EventKey, alias, "", in.Payload, in.DeliveryTarget, in.MediaPaths...)
	}
	if activityTTL <= 0 {
		return model.Job{}, fmt.Errorf("activity ttl required")
	}
	endpoint := s.Digest("activity-endpoint", in.Platform, in.Account, in.Conversation, in.Topic)
	return s.enqueueWithActivityTTL(in.Platform, in.Account, in.EventKey, alias, endpoint, activityTTL, in.Payload, in.DeliveryTarget, in.MediaPaths...)
}

func (s *Store) enqueue(platform, account, eventKey, alias, activityEndpoint string, payload, deliveryTarget []byte, mediaPaths ...string) (model.Job, error) {
	return s.enqueueWithActivityTTL(platform, account, eventKey, alias, activityEndpoint, 5*time.Minute, payload, deliveryTarget, mediaPaths...)
}

func (s *Store) enqueueWithActivityTTL(platform, account, eventKey, alias, activityEndpoint string, activityTTL time.Duration, payload, deliveryTarget []byte, mediaPaths ...string) (model.Job, error) {
	id, err := randomID()
	if err != nil {
		return model.Job{}, err
	}
	digest := s.Digest(platform, account, eventKey)
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return model.Job{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	threadID, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias=? AND status='active'", alias)
	if err != nil || !found {
		if err == nil {
			err = model.ErrEndpoint
		}
		return model.Job{}, err
	}
	seq, _, err := s.queryInt("SELECT COALESCE(MAX(sequence),0)+1 FROM turn_jobs WHERE alias=?", alias)
	if err != nil {
		return model.Job{}, err
	}
	if err := s.execParams(`INSERT INTO inbound_events(event_digest,platform,state,created_ms) VALUES(?,?,'received',?)`, digest, platform, now); err != nil {
		if s.isConstraint() {
			return model.Job{}, model.ErrDuplicate
		}
		return model.Job{}, err
	}
	target := deliveryTarget
	mediaJSON, err := json.Marshal(mediaPaths)
	if err != nil {
		return model.Job{}, err
	}
	mediaNames, err := durableMediaNames(mediaPaths)
	if err != nil {
		return model.Job{}, err
	}
	for _, name := range mediaNames {
		count, _, err := s.queryInt("SELECT COUNT(*) FROM media_leases WHERE relative_path=?", name)
		if err != nil || count != 1 {
			if err == nil {
				err = fmt.Errorf("durable media is not registered")
			}
			return model.Job{}, err
		}
		if err := s.execParams("UPDATE media_leases SET reference_count=reference_count+1 WHERE relative_path=?", name); err != nil {
			return model.Job{}, err
		}
	}
	if err := s.execParams(`INSERT INTO turn_jobs(id,event_digest,alias,state,sequence,payload,platform,delivery_target,media_paths,created_ms,updated_ms)
		VALUES(?,?,?,'received',?,?,?,?,?,?,?)`, id, digest, alias, seq, payload, platform, target, mediaJSON, now, now); err != nil {
		return model.Job{}, err
	}
	if activityEndpoint != "" && (platform == "telegram" || platform == "feishu") {
		if err := s.execParams(`INSERT INTO activity_indicators(job_id,platform,endpoint_digest,target,state,created_ms,expires_ms,updated_ms)
			VALUES(?,?,?,?,'pending',?,?,?)`, id, platform, activityEndpoint, target, now, time.Now().Add(activityTTL).UnixMilli(), now); err != nil {
			return model.Job{}, err
		}
	}
	for _, st := range []model.State{model.Normalized, model.Queued} {
		if err := s.execParams("UPDATE turn_jobs SET state=?,updated_ms=? WHERE id=?", string(st), now, id); err != nil {
			return model.Job{}, err
		}
	}
	if err := s.execParams("UPDATE inbound_events SET state='queued' WHERE event_digest=?", digest); err != nil {
		return model.Job{}, err
	}
	if err := s.exec("COMMIT"); err != nil {
		return model.Job{}, err
	}
	ok = true
	return model.Job{ID: id, EventDigest: digest, Alias: alias, ThreadID: threadID, State: model.Queued, Sequence: seq, Payload: append([]byte(nil), payload...), Platform: platform, DeliveryTarget: append([]byte(nil), target...), MediaPaths: append([]string(nil), mediaPaths...)}, nil
}

func (s *Store) Acquire(worker string, lease time.Duration) (model.Job, error) {
	now := time.Now().UnixMilli()
	until := now + lease.Milliseconds()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return model.Job{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	row, found, err := s.queryJob(`SELECT j.id,j.event_digest,j.alias,a.thread_id,j.state,j.sequence,j.payload,j.result,j.turn_id,j.attempts
		,j.platform,j.delivery_target,j.media_paths FROM turn_jobs j JOIN thread_aliases a ON a.alias=j.alias
		WHERE j.state IN ('queued','retryable_pre_submit') AND j.next_attempt_ms<=?
		AND NOT EXISTS (SELECT 1 FROM external_leases x WHERE x.alias=j.alias AND x.expires_ms>?)
		AND NOT EXISTS (SELECT 1 FROM turn_jobs r WHERE r.alias=j.alias AND r.state IN ('leased','submitted','running'))
		AND NOT EXISTS (SELECT 1 FROM turn_jobs p WHERE p.alias=j.alias AND p.sequence<j.sequence AND p.state NOT IN ('delivered','dead_letter'))
		ORDER BY j.created_ms,j.sequence LIMIT 1`, now, now)
	if err != nil {
		return model.Job{}, err
	}
	if !found {
		return model.Job{}, model.ErrNoWork
	}
	if err := s.execParams("UPDATE turn_jobs SET state='leased',lease_owner=?,lease_until_ms=?,attempts=attempts+1,updated_ms=? WHERE id=?", worker, until, now, row.ID); err != nil {
		return model.Job{}, err
	}
	if err := s.exec("COMMIT"); err != nil {
		return model.Job{}, err
	}
	ok = true
	row.State = model.Leased
	row.Attempts++
	return row, nil
}

func (s *Store) Transition(id string, from []model.State, to model.State, turnID string, result []byte) error {
	if len(from) == 0 {
		return model.ErrUnsafeTransition
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, found, err := s.queryText("SELECT state FROM turn_jobs WHERE id=?", id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("job missing")
	}
	allowed := false
	for _, st := range from {
		if current == string(st) {
			allowed = true
		}
	}
	if !allowed {
		return model.ErrUnsafeTransition
	}
	now := time.Now().UnixMilli()
	if to == model.RetryablePreSubmit {
		attempts, _, err := s.queryInt("SELECT attempts FROM turn_jobs WHERE id=?", id)
		if err != nil {
			return err
		}
		if attempts >= 12 {
			to = model.DeadLetter
		} else {
			shift := attempts - 1
			if shift < 0 {
				shift = 0
			}
			if shift > 8 {
				shift = 8
			}
			next := time.Now().Add(s.retryBase * time.Duration(1<<shift)).UnixMilli()
			return s.execParams("UPDATE turn_jobs SET state=?,next_attempt_ms=?,lease_owner=NULL,lease_until_ms=NULL,updated_ms=? WHERE id=?",
				string(to), next, now, id)
		}
	}
	if to == model.DeadLetter {
		if err := s.exec("BEGIN IMMEDIATE"); err != nil {
			return err
		}
		ok := false
		defer func() {
			if !ok {
				_ = s.exec("ROLLBACK")
			}
		}()
		if err := s.releaseMediaReferencesLocked(id); err != nil {
			return err
		}
		if err := s.execParams("UPDATE turn_jobs SET state=?,turn_id=CASE WHEN ?='' THEN turn_id ELSE ? END,payload=NULL,result=NULL,delivery_target=NULL,media_paths=NULL,lease_owner=NULL,lease_until_ms=NULL,updated_ms=? WHERE id=?",
			string(to), turnID, turnID, now, id); err != nil {
			return err
		}
		if err := s.exec("COMMIT"); err != nil {
			return err
		}
		ok = true
		return nil
	}
	return s.execParams("UPDATE turn_jobs SET state=?,turn_id=CASE WHEN ?='' THEN turn_id ELSE ? END,result=CASE WHEN ? IS NULL THEN result ELSE ? END,lease_owner=NULL,lease_until_ms=NULL,updated_ms=? WHERE id=?",
		string(to), turnID, turnID, nullableBytes(result), nullableBytes(result), now, id)
}

func nullableBytes(b []byte) any {
	if b == nil {
		return nil
	}
	return b
}

func durableMediaNames(paths []string) ([]string, error) {
	seen := make(map[string]bool, len(paths))
	names := make([]string, 0, len(paths))
	for _, path := range paths {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, fmt.Errorf("unsafe durable media path")
		}
		name := filepath.Base(path)
		if name == "." || name == string(filepath.Separator) {
			return nil, fmt.Errorf("unsafe durable media path")
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	return names, nil
}

// releaseMediaReferencesLocked requires s.mu and an active transaction.
func (s *Store) releaseMediaReferencesLocked(id string) error {
	raw, found, err := s.queryBlob("SELECT media_paths FROM turn_jobs WHERE id=?", id)
	if err != nil || !found || len(raw) == 0 {
		return err
	}
	var paths []string
	if json.Unmarshal(raw, &paths) != nil {
		return fmt.Errorf("invalid durable media paths")
	}
	names, err := durableMediaNames(paths)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := s.execParams("UPDATE media_leases SET reference_count=MAX(reference_count-1,0) WHERE relative_path=?", name); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) CompleteToOutbox(id, platform string, deliveryTarget, result []byte) error {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	state, found, err := s.queryText("SELECT state FROM turn_jobs WHERE id=?", id)
	if err != nil || !found {
		if err == nil {
			err = fmt.Errorf("job missing")
		}
		return err
	}
	if state != "running" {
		return model.ErrUnsafeTransition
	}
	if err := s.releaseMediaReferencesLocked(id); err != nil {
		return err
	}
	if err := s.execParams("UPDATE turn_jobs SET state='completed',result=?,media_paths=NULL,updated_ms=? WHERE id=?", result, now, id); err != nil {
		return err
	}
	if err := s.execParams("INSERT INTO outbox(job_id,platform,endpoint_digest,delivery_target,state,payload,attempts,next_attempt_ms,created_ms,updated_ms) VALUES(?,?,?,?,'outbox_pending',?,0,?,?,?)", id, platform, s.Digest(platform, string(deliveryTarget)), deliveryTarget, result, now, now, now); err != nil {
		return err
	}
	if err := s.execParams("UPDATE turn_jobs SET state='outbox_pending',updated_ms=? WHERE id=?", now, id); err != nil {
		return err
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Store) NextOutbox() (model.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return model.Job{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	j, found, err := s.queryJob(`SELECT j.id,j.event_digest,j.alias,a.thread_id,j.state,j.sequence,NULL,o.payload,j.turn_id,o.attempts
	,o.platform,o.delivery_target,j.media_paths FROM outbox o JOIN turn_jobs j ON j.id=o.job_id JOIN thread_aliases a ON a.alias=j.alias
	WHERE o.state IN ('outbox_pending','delivery_retryable') AND o.next_attempt_ms<=? ORDER BY o.created_ms LIMIT 1`, time.Now().UnixMilli())
	if err != nil {
		return model.Job{}, err
	}
	if !found {
		return model.Job{}, model.ErrNoWork
	}
	if err := s.execParams("UPDATE outbox SET state='sending',attempts=attempts+1,updated_ms=? WHERE job_id=?", time.Now().UnixMilli(), j.ID); err != nil {
		return model.Job{}, err
	}
	if err := s.exec("COMMIT"); err != nil {
		return model.Job{}, err
	}
	ok = true
	j.Attempts++
	return j, nil
}

func (s *Store) MarkOutbox(id string, state model.State) error {
	valid := state == model.Delivered || state == model.DeliveryRetryable || state == model.DeliveryUncertain || state == model.DeadLetter
	if !valid {
		return model.ErrUnsafeTransition
	}
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	next := now
	if state == model.DeliveryRetryable {
		attempts, _, err := s.queryInt("SELECT attempts FROM outbox WHERE job_id=?", id)
		if err != nil {
			return err
		}
		if attempts >= 12 {
			state = model.DeadLetter
		} else {
			shift := attempts
			if shift > 8 {
				shift = 8
			}
			next = time.Now().Add(s.retryBase * time.Duration(1<<shift)).UnixMilli()
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
	scrub := 0
	if state == model.Delivered || state == model.DeadLetter {
		scrub = 1
	}
	if err := s.execParams("UPDATE outbox SET state=?,next_attempt_ms=?,updated_ms=?,payload=CASE WHEN ?=1 THEN NULL ELSE payload END,delivery_target=CASE WHEN ?=1 THEN NULL ELSE delivery_target END WHERE job_id=?", string(state), next, now, scrub, scrub, id); err != nil {
		return err
	}
	if scrub == 1 {
		if err := s.releaseMediaReferencesLocked(id); err != nil {
			return err
		}
	}
	if err := s.execParams("UPDATE turn_jobs SET state=?,updated_ms=?,payload=CASE WHEN ?=1 THEN NULL ELSE payload END,result=CASE WHEN ?=1 THEN NULL ELSE result END,delivery_target=CASE WHEN ?=1 THEN NULL ELSE delivery_target END,media_paths=CASE WHEN ?=1 THEN NULL ELSE media_paths END WHERE id=?", string(state), now, scrub, scrub, scrub, scrub, id); err != nil {
		return err
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}
func (s *Store) Reconcile(id, evidenceToken, disposition string) error {
	if len(evidenceToken) < 16 {
		return fmt.Errorf("evidence token rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, found, err := s.queryText("SELECT state FROM turn_jobs WHERE id=?", id)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("job missing")
	}
	var target model.State
	switch model.State(state) {
	case model.UncertainPostSubmit:
		switch disposition {
		case "retry_pre_submit":
			target = model.RetryablePreSubmit
		case "dead_letter":
			target = model.DeadLetter
		default:
			return model.ErrUnsafeTransition
		}
	case model.DeliveryUncertain:
		switch disposition {
		case "delivered":
			target = model.Delivered
		case "delivery_retryable":
			target = model.DeliveryRetryable
		case "dead_letter":
			target = model.DeadLetter
		default:
			return model.ErrUnsafeTransition
		}
	default:
		return model.ErrUnsafeTransition
	}
	now := time.Now().UnixMilli()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	if err := s.execParams("INSERT INTO reconciliations(job_id,evidence_digest,disposition,created_ms) VALUES(?,?,?,?)", id, s.Digest("evidence", evidenceToken), disposition, now); err != nil {
		return err
	}
	if target == model.Delivered || target == model.DeadLetter {
		if err := s.releaseMediaReferencesLocked(id); err != nil {
			return err
		}
		if err := s.execParams("UPDATE outbox SET state=?,payload=NULL,delivery_target=NULL,updated_ms=? WHERE job_id=?", string(target), now, id); err != nil {
			return err
		}
		if err := s.execParams("UPDATE turn_jobs SET state=?,payload=NULL,result=NULL,delivery_target=NULL,media_paths=NULL,updated_ms=? WHERE id=?", string(target), now, id); err != nil {
			return err
		}
	} else {
		if err := s.execParams("UPDATE turn_jobs SET state=?,updated_ms=? WHERE id=?", string(target), now, id); err != nil {
			return err
		}
		if state == string(model.DeliveryUncertain) {
			if err := s.execParams("UPDATE outbox SET state=?,next_attempt_ms=?,updated_ms=? WHERE job_id=?", string(target), now, now, id); err != nil {
				return err
			}
		}
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}

func (s *Store) Recover() error {
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.exec("BEGIN IMMEDIATE"); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			_ = s.exec("ROLLBACK")
		}
	}()
	if err := s.execParams("UPDATE turn_jobs SET state='retryable_pre_submit',next_attempt_ms=?,lease_owner=NULL,lease_until_ms=NULL,updated_ms=? WHERE state='leased'", time.Now().Add(s.retryBase).UnixMilli(), now); err != nil {
		return err
	}
	if err := s.execParams("UPDATE turn_jobs SET state='uncertain_post_submit',lease_owner=NULL,lease_until_ms=NULL,updated_ms=? WHERE state IN ('submitted','running')", now); err != nil {
		return err
	}
	if err := s.execParams("UPDATE outbox SET state='delivery_uncertain',updated_ms=? WHERE state='sending'", now); err != nil {
		return err
	}
	if err := s.execParams("UPDATE turn_jobs SET state='delivery_uncertain',updated_ms=? WHERE id IN (SELECT job_id FROM outbox WHERE state='delivery_uncertain')", now); err != nil {
		return err
	}
	if err := s.execParams("DELETE FROM external_leases WHERE expires_ms<=?", now); err != nil {
		return err
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	ok = true
	return nil
}

var recoverableActivityStates = map[model.State]bool{
	model.UncertainPostSubmit: true,
	model.DeliveryUncertain:   true,
}
var terminalActivityStates = map[model.State]bool{
	model.Delivered:  true,
	model.DeadLetter: true,
}

// Activities returns opaque lifecycle records. Target and ReactionID are
// deliberately absent from logs and health; only platform drivers consume them.
func (s *Store) Activities() ([]model.Activity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.prepare(`SELECT a.job_id,a.platform,a.endpoint_digest,a.state,a.target,a.reaction_id,
		a.attempts,a.next_attempt_ms,a.created_ms,a.expires_ms,j.state
		FROM activity_indicators a JOIN turn_jobs j ON j.id=a.job_id
		WHERE a.state!='done' ORDER BY a.created_ms,a.job_id`)
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(st)
	var out []model.Activity
	for {
		rc := C.sqlite3_step(st)
		if rc == C.SQLITE_DONE {
			return out, nil
		}
		if rc != C.SQLITE_ROW {
			return nil, fmt.Errorf("activity query failed")
		}
		textAt := func(i int) string {
			p := C.sqlite3_column_text(st, C.int(i))
			if p == nil {
				return ""
			}
			return C.GoString((*C.char)(unsafe.Pointer(p)))
		}
		blobAt := func(i int) []byte {
			n := int(C.sqlite3_column_bytes(st, C.int(i)))
			p := C.sqlite3_column_blob(st, C.int(i))
			if p == nil || n == 0 {
				return nil
			}
			return C.GoBytes(p, C.int(n))
		}
		jobState := model.State(textAt(10))
		out = append(out, model.Activity{
			JobID: textAt(0), Platform: textAt(1), EndpointDigest: textAt(2), State: textAt(3),
			Target: blobAt(4), ReactionID: blobAt(5),
			Attempts: int(C.sqlite3_column_int64(st, 6)), NextAttemptMS: int64(C.sqlite3_column_int64(st, 7)),
			CreatedMS: int64(C.sqlite3_column_int64(st, 8)), ExpiresMS: int64(C.sqlite3_column_int64(st, 9)),
			Terminal: terminalActivityStates[jobState], RecoverableTerminal: recoverableActivityStates[jobState],
		})
	}
}

// ActivityAttempt crosses the side-effect ambiguity boundary before a platform
// call. "creating" is reconciled through Feishu list after a crash.
func (s *Store) ActivityAttempt(jobID, state string, next time.Time) error {
	if state != "creating" && state != "deleting" && state != "pulsing" {
		return model.ErrUnsafeTransition
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams(`UPDATE activity_indicators SET state=?,attempts=attempts+1,next_attempt_ms=?,updated_ms=?
		WHERE job_id=? AND state!='done'`, state, next.UnixMilli(), time.Now().UnixMilli(), jobID)
}

func (s *Store) ActivityActive(jobID string, reactionID []byte, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams(`UPDATE activity_indicators SET state='active',reaction_id=?,next_attempt_ms=?,updated_ms=?
		WHERE job_id=? AND state!='done'`, nullableBytes(reactionID), next.UnixMilli(), time.Now().UnixMilli(), jobID)
}

func (s *Store) ActivityRetry(jobID string, next time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams(`UPDATE activity_indicators SET state=CASE
		WHEN state IN ('creating','deleting') THEN state
		WHEN reaction_id IS NULL THEN 'pending' ELSE 'active' END,
		next_attempt_ms=?,updated_ms=? WHERE job_id=? AND state!='done'`, next.UnixMilli(), time.Now().UnixMilli(), jobID)
}

func (s *Store) ActivityPause(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams(`UPDATE activity_indicators SET state='pending',reaction_id=NULL,next_attempt_ms=0,updated_ms=?
		WHERE job_id=? AND state!='done'`, time.Now().UnixMilli(), jobID)
}

// ActivityDone deletes the lifecycle record, preventing both raw locator
// retention and unbounded terminal-row growth.
func (s *Store) ActivityDone(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams("DELETE FROM activity_indicators WHERE job_id=?", jobID)
}

func (s *Store) ActiveActivityCount() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, _, err := s.queryInt("SELECT COUNT(*) FROM activity_indicators WHERE state!='done'")
	return n, err
}

func (s *Store) SetExternalLease(alias, owner string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found, err := s.queryText("SELECT thread_id FROM thread_aliases WHERE alias=?", alias)
	if err != nil {
		return err
	}
	if !found {
		return model.ErrEndpoint
	}
	return s.execParams(`INSERT INTO external_leases(alias,owner_digest,expires_ms) VALUES(?,?,?)
	ON CONFLICT(alias) DO UPDATE SET owner_digest=excluded.owner_digest,expires_ms=excluded.expires_ms`, alias, s.Digest(owner), time.Now().Add(ttl).UnixMilli())
}
func (s *Store) ClearExternalLease(alias, owner string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams("DELETE FROM external_leases WHERE alias=? AND owner_digest=?", alias, s.Digest(owner))
}
func (s *Store) AliasForThread(threadID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	alias, found, err := s.queryText("SELECT alias FROM thread_aliases WHERE thread_id=? AND status='active'", threadID)
	if err != nil {
		return "", err
	}
	if !found {
		return "", model.ErrEndpoint
	}
	return alias, nil
}

func (s *Store) RegisterMedia(relative string, size int64, created, expires time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams("INSERT INTO media_leases(path_digest,relative_path,size,created_ms,expires_ms,lease_count) VALUES(?,?,?,?,?,0)", s.Digest("media", relative), relative, size, created.UnixMilli(), expires.UnixMilli())
}
func (s *Store) ChangeMediaLease(relative string, delta int, hardExpiry time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if delta < -1 || delta > 1 {
		return fmt.Errorf("invalid lease delta")
	}
	switch delta {
	case -1:
		return s.execParams("UPDATE media_leases SET lease_count=MAX(lease_count-1,0),lease_hard_expires_ms=CASE WHEN lease_count<=1 THEN 0 ELSE lease_hard_expires_ms END WHERE relative_path=?", relative)
	case 0:
		if hardExpiry.IsZero() {
			return fmt.Errorf("renewal expiry required")
		}
		return s.execParams("UPDATE media_leases SET lease_hard_expires_ms=? WHERE relative_path=? AND lease_count>0", hardExpiry.UnixMilli(), relative)
	default:
		if hardExpiry.IsZero() {
			return fmt.Errorf("lease expiry required")
		}
		return s.execParams("UPDATE media_leases SET lease_count=lease_count+1,lease_hard_expires_ms=? WHERE relative_path=?", hardExpiry.UnixMilli(), relative)
	}
}
func (s *Store) ExpireMediaLease(relative string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams("UPDATE media_leases SET lease_count=0,lease_hard_expires_ms=0 WHERE relative_path=? AND lease_count>0 AND lease_hard_expires_ms>0 AND lease_hard_expires_ms<=?", relative, now.UnixMilli())
}
func (s *Store) DeleteMedia(relative string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.execParams("DELETE FROM media_leases WHERE relative_path=?", relative)
}
func (s *Store) MediaRecords() ([]model.MediaRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.prepare("SELECT relative_path,size,created_ms,expires_ms,lease_count,lease_hard_expires_ms,reference_count FROM media_leases")
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(st)
	var out []model.MediaRecord
	for {
		rc := C.sqlite3_step(st)
		if rc == C.SQLITE_DONE {
			break
		}
		if rc != C.SQLITE_ROW {
			return nil, fmt.Errorf("sqlite read failed")
		}
		out = append(out, model.MediaRecord{RelativePath: C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(st, 0)))), Size: int64(C.sqlite3_column_int64(st, 1)), CreatedMS: int64(C.sqlite3_column_int64(st, 2)), ExpiresMS: int64(C.sqlite3_column_int64(st, 3)), Leases: int(C.sqlite3_column_int(st, 4)), LeaseHardExpiresMS: int64(C.sqlite3_column_int64(st, 5)), References: int(C.sqlite3_column_int(st, 6))})
	}
	return out, nil
}

func (s *Store) Job(id string) (model.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok, err := s.queryJob(`SELECT j.id,j.event_digest,j.alias,a.thread_id,j.state,j.sequence,j.payload,j.result,j.turn_id,j.attempts,j.platform,j.delivery_target,j.media_paths FROM turn_jobs j JOIN thread_aliases a ON a.alias=j.alias WHERE j.id=?`, id)
	if err != nil {
		return j, err
	}
	if !ok {
		return j, fmt.Errorf("job missing")
	}
	return j, nil
}

func (s *Store) Counts() (map[string]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]int64{}
	stmt, err := s.prepare("SELECT state,COUNT(*) FROM turn_jobs GROUP BY state")
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(stmt)
	for C.sqlite3_step(stmt) == C.SQLITE_ROW {
		out[C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(stmt, 0))))] = int64(C.sqlite3_column_int64(stmt, 1))
	}
	return out, nil
}

// DrainPendingCount returns work that must either finish or cross an explicit
// uncertainty boundary before a graceful shutdown can close the app-server
// connection. Recoverable uncertainty and operator-resolved terminal states do
// not block shutdown and are never automatically resubmitted here.
func (s *Store) DrainPendingCount() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, _, err := s.queryInt(`SELECT COUNT(*) FROM turn_jobs
		WHERE state NOT IN ('delivered','dead_letter','uncertain_post_submit','delivery_uncertain')`)
	return n, err
}

func (s *Store) isConstraint() bool {
	return C.sqlite3_extended_errcode(s.db) == C.SQLITE_CONSTRAINT_UNIQUE || C.sqlite3_extended_errcode(s.db) == C.SQLITE_CONSTRAINT_PRIMARYKEY
}
func (s *Store) prepare(sql string) (*C.sqlite3_stmt, error) {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var st *C.sqlite3_stmt
	if C.sqlite3_prepare_v2(s.db, cs, -1, &st, nil) != C.SQLITE_OK {
		return nil, fmt.Errorf("sqlite prepare failed")
	}
	return st, nil
}
func bind(st *C.sqlite3_stmt, args ...any) error {
	for i, v := range args {
		idx := C.int(i + 1)
		var rc C.int
		switch x := v.(type) {
		case string:
			cs := C.CString(x)
			rc = C.bind_text_copy(st, idx, cs)
			C.free(unsafe.Pointer(cs))
		case int:
			rc = C.sqlite3_bind_int64(st, idx, C.sqlite3_int64(x))
		case int64:
			rc = C.sqlite3_bind_int64(st, idx, C.sqlite3_int64(x))
		case []byte:
			if x == nil {
				rc = C.sqlite3_bind_null(st, idx)
			} else if len(x) == 0 {
				rc = C.sqlite3_bind_zeroblob(st, idx, 0)
			} else {
				rc = C.bind_blob_copy(st, idx, unsafe.Pointer(&x[0]), C.int(len(x)))
			}
		case nil:
			rc = C.sqlite3_bind_null(st, idx)
		default:
			return fmt.Errorf("unsupported bind")
		}
		if rc != C.SQLITE_OK {
			return fmt.Errorf("sqlite bind failed")
		}
	}
	return nil
}
func (s *Store) execParams(sql string, args ...any) error {
	st, err := s.prepare(sql)
	if err != nil {
		return err
	}
	defer C.sqlite3_finalize(st)
	if err = bind(st, args...); err != nil {
		return err
	}
	if C.sqlite3_step(st) != C.SQLITE_DONE {
		return fmt.Errorf("sqlite write failed")
	}
	return nil
}
func (s *Store) queryText(sql string, args ...any) (string, bool, error) {
	st, err := s.prepare(sql)
	if err != nil {
		return "", false, err
	}
	defer C.sqlite3_finalize(st)
	if err = bind(st, args...); err != nil {
		return "", false, err
	}
	rc := C.sqlite3_step(st)
	if rc == C.SQLITE_DONE {
		return "", false, nil
	}
	if rc != C.SQLITE_ROW {
		return "", false, fmt.Errorf("sqlite read failed")
	}
	return C.GoString((*C.char)(unsafe.Pointer(C.sqlite3_column_text(st, 0)))), true, nil
}
func (s *Store) queryInt(sql string, args ...any) (int64, bool, error) {
	st, err := s.prepare(sql)
	if err != nil {
		return 0, false, err
	}
	defer C.sqlite3_finalize(st)
	if err = bind(st, args...); err != nil {
		return 0, false, err
	}
	rc := C.sqlite3_step(st)
	if rc == C.SQLITE_DONE {
		return 0, false, nil
	}
	if rc != C.SQLITE_ROW {
		return 0, false, fmt.Errorf("sqlite read failed")
	}
	return int64(C.sqlite3_column_int64(st, 0)), true, nil
}
func (s *Store) queryBlob(sql string, args ...any) ([]byte, bool, error) {
	st, err := s.prepare(sql)
	if err != nil {
		return nil, false, err
	}
	defer C.sqlite3_finalize(st)
	if err = bind(st, args...); err != nil {
		return nil, false, err
	}
	rc := C.sqlite3_step(st)
	if rc == C.SQLITE_DONE {
		return nil, false, nil
	}
	if rc != C.SQLITE_ROW {
		return nil, false, fmt.Errorf("sqlite read failed")
	}
	n := C.sqlite3_column_bytes(st, 0)
	p := C.sqlite3_column_blob(st, 0)
	if p == nil || n == 0 {
		return nil, true, nil
	}
	return C.GoBytes(p, n), true, nil
}
func (s *Store) queryJob(sql string, args ...any) (model.Job, bool, error) {
	st, err := s.prepare(sql)
	if err != nil {
		return model.Job{}, false, err
	}
	defer C.sqlite3_finalize(st)
	if err = bind(st, args...); err != nil {
		return model.Job{}, false, err
	}
	rc := C.sqlite3_step(st)
	if rc == C.SQLITE_DONE {
		return model.Job{}, false, nil
	}
	if rc != C.SQLITE_ROW {
		return model.Job{}, false, fmt.Errorf("sqlite read failed")
	}
	text := func(i int) string {
		p := C.sqlite3_column_text(st, C.int(i))
		if p == nil {
			return ""
		}
		return C.GoString((*C.char)(unsafe.Pointer(p)))
	}
	blob := func(i int) []byte {
		n := C.sqlite3_column_bytes(st, C.int(i))
		p := C.sqlite3_column_blob(st, C.int(i))
		if p == nil || n == 0 {
			return nil
		}
		return C.GoBytes(p, n)
	}
	var mediaPaths []string
	if raw := blob(12); len(raw) > 0 {
		if json.Unmarshal(raw, &mediaPaths) != nil {
			return model.Job{}, false, fmt.Errorf("invalid durable media paths")
		}
	}
	return model.Job{ID: text(0), EventDigest: text(1), Alias: text(2), ThreadID: text(3), State: model.State(text(4)), Sequence: int64(C.sqlite3_column_int64(st, 5)), Payload: blob(6), Result: blob(7), TurnID: text(8), Attempts: int(C.sqlite3_column_int(st, 9)), Platform: text(10), DeliveryTarget: blob(11), MediaPaths: mediaPaths}, true, nil
}

const schema = `
CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS thread_aliases(alias TEXT PRIMARY KEY,thread_id TEXT NOT NULL,generation INTEGER NOT NULL,protected INTEGER NOT NULL CHECK(protected IN(0,1)),status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS endpoint_bindings(platform TEXT NOT NULL,account_digest TEXT NOT NULL,conversation_digest TEXT NOT NULL,topic_digest TEXT NOT NULL,alias TEXT NOT NULL REFERENCES thread_aliases(alias),PRIMARY KEY(platform,account_digest,conversation_digest,topic_digest));
CREATE TABLE IF NOT EXISTS owner_bindings(platform TEXT NOT NULL,account_digest TEXT NOT NULL,owner_digest TEXT NOT NULL,PRIMARY KEY(platform,account_digest));
CREATE TABLE IF NOT EXISTS inbound_events(event_digest TEXT PRIMARY KEY,platform TEXT NOT NULL,state TEXT NOT NULL,created_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS turn_jobs(id TEXT PRIMARY KEY,event_digest TEXT UNIQUE NOT NULL REFERENCES inbound_events(event_digest),alias TEXT NOT NULL REFERENCES thread_aliases(alias),state TEXT NOT NULL,sequence INTEGER NOT NULL,payload BLOB,result BLOB,turn_id TEXT,platform TEXT NOT NULL,delivery_target BLOB,media_paths BLOB,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ms INTEGER NOT NULL DEFAULT 0,lease_owner TEXT,lease_until_ms INTEGER,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL,UNIQUE(alias,sequence));
CREATE TABLE IF NOT EXISTS outbox(job_id TEXT PRIMARY KEY REFERENCES turn_jobs(id),platform TEXT NOT NULL,endpoint_digest TEXT NOT NULL,delivery_target BLOB,state TEXT NOT NULL,payload BLOB,attempts INTEGER NOT NULL,next_attempt_ms INTEGER NOT NULL,created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS external_leases(alias TEXT PRIMARY KEY REFERENCES thread_aliases(alias),owner_digest TEXT NOT NULL,expires_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS media_leases(path_digest TEXT PRIMARY KEY,relative_path TEXT NOT NULL UNIQUE,size INTEGER NOT NULL,created_ms INTEGER NOT NULL,expires_ms INTEGER NOT NULL,lease_count INTEGER NOT NULL DEFAULT 0,lease_hard_expires_ms INTEGER NOT NULL DEFAULT 0,reference_count INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS reconciliations(id INTEGER PRIMARY KEY AUTOINCREMENT,job_id TEXT NOT NULL REFERENCES turn_jobs(id),evidence_digest TEXT NOT NULL,disposition TEXT NOT NULL,created_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS activity_indicators(job_id TEXT PRIMARY KEY REFERENCES turn_jobs(id),platform TEXT NOT NULL,endpoint_digest TEXT NOT NULL,target BLOB,state TEXT NOT NULL,reaction_id BLOB,attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ms INTEGER NOT NULL DEFAULT 0,created_ms INTEGER NOT NULL,expires_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS main_rotations(operation_id TEXT PRIMARY KEY,old_thread_id TEXT NOT NULL,new_thread_id TEXT,old_generation INTEGER NOT NULL,committed_generation INTEGER NOT NULL DEFAULT 0,phase TEXT NOT NULL,cleanup_incomplete INTEGER NOT NULL DEFAULT 0 CHECK(cleanup_incomplete IN(0,1)),created_ms INTEGER NOT NULL,updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS main_history_baseline(singleton INTEGER PRIMARY KEY CHECK(singleton=1),generation INTEGER NOT NULL CHECK(generation>0),thread_id TEXT NOT NULL,observed_from_ns INTEGER NOT NULL CHECK(observed_from_ns>0),baseline_digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS main_epochs(generation INTEGER PRIMARY KEY CHECK(generation>0),thread_id TEXT NOT NULL,effective_from_ns INTEGER NOT NULL CHECK(effective_from_ns>0),transition_kind TEXT NOT NULL CHECK(transition_kind IN('initialization','rotation','rollback')),operation_id TEXT NOT NULL,previous_epoch_digest TEXT NOT NULL,epoch_digest TEXT NOT NULL UNIQUE,UNIQUE(operation_id,transition_kind));
CREATE INDEX IF NOT EXISTS jobs_sched ON turn_jobs(state,created_ms,sequence);
CREATE INDEX IF NOT EXISTS activity_due ON activity_indicators(state,next_attempt_ms);
`

var _ = errors.Is
