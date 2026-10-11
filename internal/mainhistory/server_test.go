package mainhistory

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"

	"codex-persona-router/internal/model"
)

type fakeReader struct {
	history model.MainEpochHistory
	err     error
	fromNS  int64
	toNS    int64
}

func (f *fakeReader) MainEpochHistory(fromNS, toNS int64) (model.MainEpochHistory, error) {
	f.fromNS, f.toNS = fromNS, toNS
	return f.history, f.err
}

func startServer(t *testing.T, reader Reader, ready bool) (string, *Server) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "mh-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "history.sock")
	server := New(path, reader)
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	server.SetReady(ready)
	t.Cleanup(func() { _ = server.Close() })
	return path, server
}

func requestServer(t *testing.T, path, request string) map[string]any {
	t.Helper()
	connection, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if _, err := connection.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(connection).ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if json.Unmarshal(line, &response) != nil {
		t.Fatalf("invalid response: %s", line)
	}
	return response
}

func TestHistoryServerExactSuccess(t *testing.T) {
	reader := &fakeReader{history: model.MainEpochHistory{
		Persona: "test-persona", Origin: "exact_initialization", AvailableFromNS: 100,
		PriorHistoryComplete: true, HeadGeneration: 1,
		HeadDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Epochs: []model.MainEpoch{{
			Generation: 1, ThreadID: "synthetic-thread", EffectiveFromNS: 100,
			TransitionKind: "initialization",
			Digest:         "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}},
	}}
	path, _ := startServer(t, reader, true)
	response := requestServer(t, path, `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"200","to_unix_ns":"300"}`+"\n")
	if response["status"] != "ok" || response["persona"] != "test-persona" {
		t.Fatalf("unexpected response: %+v", response)
	}
	if reader.fromNS != 200 || reader.toNS != 300 {
		t.Fatalf("range changed: %d %d", reader.fromNS, reader.toNS)
	}
	epochs, ok := response["epochs"].([]any)
	if !ok || len(epochs) != 1 {
		t.Fatalf("epochs missing: %+v", response)
	}
}

func TestHistoryEncodingMatchesVersionOneFixture(t *testing.T) {
	history := model.MainEpochHistory{
		Persona: "test-persona", Origin: "exact_initialization", AvailableFromNS: 50,
		PriorHistoryComplete: true, HeadGeneration: 3,
		HeadDigest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Epochs: []model.MainEpoch{
			{Generation: 1, ThreadID: "synthetic-thread-a", EffectiveFromNS: 50, EffectiveToNS: 180,
				TransitionKind: "initialization", Digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
			{Generation: 2, ThreadID: "synthetic-thread-b", EffectiveFromNS: 180, EffectiveToNS: 240,
				TransitionKind: "rotation", PreviousDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Digest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
			{Generation: 3, ThreadID: "synthetic-thread-a", EffectiveFromNS: 240,
				TransitionKind: "rollback", PreviousDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				Digest: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},
		},
	}
	raw, err := json.Marshal(encodeHistory(history, "150", "300"))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"schema":"codex.router.main-epoch-history.response","version":1,"status":"ok","persona":"test-persona","time_unit":"unix_ns_utc","request":{"from_unix_ns":"150","to_unix_ns":"300"},"coverage":{"complete":true,"origin":"exact_initialization","available_from_unix_ns":"50","prior_history_complete":true},"baseline":null,"history_head":{"generation":3,"epoch_digest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"},"epochs":[{"generation":1,"thread_id":"synthetic-thread-a","effective_from_unix_ns":"50","effective_to_unix_ns":"180","transition_kind":"initialization","disposition":"committed","previous_epoch_digest":null,"epoch_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"generation":2,"thread_id":"synthetic-thread-b","effective_from_unix_ns":"180","effective_to_unix_ns":"240","transition_kind":"rotation","disposition":"committed","previous_epoch_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","epoch_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},{"generation":3,"thread_id":"synthetic-thread-a","effective_from_unix_ns":"240","effective_to_unix_ns":null,"transition_kind":"rollback","disposition":"committed","previous_epoch_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","epoch_digest":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}]}`
	if string(raw) != want {
		t.Fatalf("version-one fixture drifted:\n%s", raw)
	}
}

func TestHistoryServerBaselineAlwaysReturnedAsProvenance(t *testing.T) {
	reader := &fakeReader{history: model.MainEpochHistory{
		Persona: "test-persona", Origin: "migration_baseline", AvailableFromNS: 100,
		Baseline: &model.MainHistoryBaseline{
			Generation: 4, ThreadID: "legacy", ObservedFromNS: 100, ObservedToNS: 150,
			Digest: "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
		},
		HeadGeneration: 9,
		HeadDigest:     "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		Epochs: []model.MainEpoch{{
			Generation: 9, ThreadID: "later", EffectiveFromNS: 200,
			TransitionKind: "rotation",
			PreviousDigest: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			Digest:         "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
		}},
	}}
	path, _ := startServer(t, reader, true)
	response := requestServer(t, path, `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"210","to_unix_ns":"300"}`+"\n")
	baseline, ok := response["baseline"].(map[string]any)
	if !ok || baseline["generation"] != float64(4) {
		t.Fatalf("baseline provenance missing: %+v", response)
	}
}

func TestHistoryServerErrorsAreContentFree(t *testing.T) {
	for _, test := range []struct {
		name, request, code string
		ready               bool
		err                 error
	}{
		{"not-ready", `{}` + "\n", "not_ready", false, nil},
		{"unknown-field", `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2","persona":"other"}` + "\n", "invalid_request", true, nil},
		{"unsupported", `{"schema":"codex.router.main-epoch-history.request","version":2,"from_unix_ns":"1","to_unix_ns":"2"}` + "\n", "unsupported_version", true, nil},
		{"incomplete", `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2"}` + "\n", "history_incomplete", true, model.ErrHistoryIncomplete},
		{"corrupt", `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2"}` + "\n", "history_corrupt", true, model.ErrHistoryCorrupt},
		{"busy", `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2"}` + "\n", "busy", true, model.ErrHistoryBusy},
		{"internal", `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2"}` + "\n", "internal_unavailable", true, errors.New("secret thread and database path")},
	} {
		t.Run(test.name, func(t *testing.T) {
			path, _ := startServer(t, &fakeReader{err: test.err}, test.ready)
			response := requestServer(t, path, test.request)
			if response["status"] != "error" || response["code"] != test.code || len(response) != 5 {
				t.Fatalf("unsafe error response: %+v", response)
			}
		})
	}
}

func TestHistoryServerBoundsRequestsAndResponses(t *testing.T) {
	path, _ := startServer(t, &fakeReader{}, true)
	oversized := `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2","padding":"` +
		string(make([]byte, maxRequestBytes)) + `"}` + "\n"
	response := requestServer(t, path, oversized)
	if response["code"] != "invalid_request" {
		t.Fatalf("oversized request accepted: %+v", response)
	}

	epochs := make([]model.MainEpoch, maxEpochs+1)
	path, _ = startServer(t, &fakeReader{history: model.MainEpochHistory{Epochs: epochs}}, true)
	response = requestServer(t, path, `{"schema":"codex.router.main-epoch-history.request","version":1,"from_unix_ns":"1","to_unix_ns":"2"}`+"\n")
	if response["code"] != "response_too_large" {
		t.Fatalf("oversized response accepted: %+v", response)
	}
}

func TestHistoryServerPrivateSocketAndPathSafety(t *testing.T) {
	path, server := startServer(t, &fakeReader{}, false)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0600 {
		t.Fatalf("unsafe socket: %v %+v", err, info)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("socket retained after close")
	}

	root, err := os.MkdirTemp("/tmp", "mh-path-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	occupied := filepath.Join(root, "history.sock")
	if err := os.WriteFile(occupied, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := New(occupied, &fakeReader{}).Start(); err == nil {
		t.Fatal("regular history path accepted")
	}
}
