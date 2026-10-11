package router

import (
	"codex-persona-router/internal/adapters"
	"codex-persona-router/internal/model"
	"codex-persona-router/internal/store"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type fakeCodex struct {
	mu                  sync.Mutex
	calls               int
	turn, result        string
	err                 error
	seenThread, seenJob string
	seenSource          string
	seenMedia           []string
}

func TestMediaOnlyNormalizeAcceptsEmptyPayload(t *testing.T) {
	s := testStore(t)
	policy := adapters.Policy{Persona: "p", Owner: "owner", AllowedPlatforms: map[string]bool{"fake": true}}
	in, err := adapters.Normalize(policy, adapters.Event{Persona: "p", Platform: "fake", EventKey: "media-event", Account: "a", Conversation: "c", Owner: "owner", Media: []adapters.MediaRef{{Kind: "image", OpaqueKey: "opaque", MIME: "image/png", Size: 8}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Payload) != 0 {
		t.Fatal("media-only event gained text")
	}
	r := New(s, nil, nil, "w", 1)
	job, err := r.Accept(in)
	if err != nil {
		t.Fatalf("empty blob enqueue failed: %v", err)
	}
	if job.ID == "" {
		t.Fatal("job missing")
	}
}

func TestAcceptRequiresDurableAccountOwnerBinding(t *testing.T) {
	s := testStore(t)
	r := New(s, nil, nil, "w", 1)
	base := model.Inbound{
		Platform: "fake", EventKey: "event", Account: "a", Owner: "owner",
		Conversation: "c", Payload: []byte("x"),
	}
	if _, err := r.Accept(base); err != nil {
		t.Fatal(err)
	}
	base.EventKey = "wrong-owner"
	base.Owner = "other"
	if _, err := r.Accept(base); !errors.Is(err, model.ErrEndpoint) {
		t.Fatal("owner mismatch accepted")
	}
	base.EventKey = "wrong-account"
	base.Owner = "owner"
	base.Account = "other"
	if _, err := r.Accept(base); !errors.Is(err, model.ErrEndpoint) {
		t.Fatal("cross-account owner accepted")
	}
}

func TestActivityCapabilityIsPostAuthAndDisabledByDefault(t *testing.T) {
	s := testStore(t)
	if err := s.BindOwner("telegram", "account", "owner"); err != nil {
		t.Fatal(err)
	}
	if err := s.BindEndpoint("telegram", "account", "conversation", "topic", "main"); err != nil {
		t.Fatal(err)
	}
	r := New(s, nil, nil, "w", 1)
	in := model.Inbound{
		Platform: "telegram", Account: "account", Owner: "wrong",
		Conversation: "conversation", Topic: "topic", EventKey: "unauthorized",
		Payload: []byte("x"), DeliveryTarget: []byte(`{"chat_id":"opaque","reply_to_message_id":1}`),
	}
	if _, err := r.Accept(in); !errors.Is(err, model.ErrEndpoint) {
		t.Fatal("unauthorized input accepted")
	}
	if count, err := s.ActiveActivityCount(); err != nil || count != 0 {
		t.Fatalf("unauthorized activity count=%d err=%v", count, err)
	}
	in.Owner, in.EventKey = "owner", "disabled"
	if _, err := r.Accept(in); err != nil {
		t.Fatal(err)
	}
	if count, err := s.ActiveActivityCount(); err != nil || count != 0 {
		t.Fatalf("disabled capability created activity: count=%d err=%v", count, err)
	}
	r.SetActivity(nil, "telegram")
	in.EventKey = "enabled"
	in.DeliveryTarget = []byte(`{"chat_id":"opaque","reply_to_message_id":2}`)
	if _, err := r.Accept(in); err != nil {
		t.Fatal(err)
	}
	if count, err := s.ActiveActivityCount(); err != nil || count != 1 {
		t.Fatalf("enabled durable activity count=%d err=%v", count, err)
	}
}

func (f *fakeCodex) Turn(_ context.Context, thread, job, source string, _ []byte, media []string, started func(string) error) (string, []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.seenThread = thread
	f.seenJob = job
	f.seenSource = source
	f.seenMedia = append([]string(nil), media...)
	if f.turn != "" {
		if err := started(f.turn); err != nil {
			return f.turn, nil, err
		}
	}
	return f.turn, []byte(f.result), f.err
}

type fakeDelivery struct{ err error }

func (f fakeDelivery) Send(context.Context, model.Job) error { return f.err }

type fakeMedia struct{ leased, renewed, released []string }

func (f *fakeMedia) Lease(p string) error { f.leased = append(f.leased, p); return nil }
func (f *fakeMedia) Renew(p string) error { f.renewed = append(f.renewed, p); return nil }
func (f *fakeMedia) Release(p string)     { f.released = append(f.released, p) }
func testStore(t *testing.T) *store.Store {
	t.Helper()
	d := filepath.Join(t.TempDir(), "s")
	os.Mkdir(d, 0700)
	s, err := store.Open(filepath.Join(d, "r.db"), "p", make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	s.InitMain("thread")
	s.BindOwner("fake", "a", "owner")
	s.BindEndpoint("fake", "a", "c", "", "main")
	return s
}
func TestCertifiedPreSubmitCanRetry(t *testing.T) {
	s := testStore(t)
	j, _ := s.Enqueue("fake", "a", "e", "main", []byte("x"), []byte("target"))
	f := &fakeCodex{err: &model.PreSubmitError{Err: errors.New("offline")}}
	r := New(s, f, fakeDelivery{}, "w", 1)
	a, _ := s.Acquire("w", time.Minute)
	r.process(context.Background(), a)
	got, _ := s.Job(j.ID)
	if got.State != model.RetryablePreSubmit {
		t.Fatalf("%s", got.State)
	}
	if _, err := s.Acquire("w", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("pre-submit retry ignored backoff")
	}
}
func TestUncertainNeverResubmits(t *testing.T) {
	s := testStore(t)
	j, _ := s.Enqueue("fake", "a", "e", "main", []byte("x"), []byte("target"))
	f := &fakeCodex{err: &model.UncertainError{Err: errors.New("lost ack")}}
	r := New(s, f, fakeDelivery{}, "w", 1)
	a, _ := s.Acquire("w", time.Minute)
	r.process(context.Background(), a)
	got, _ := s.Job(j.ID)
	if got.State != model.UncertainPostSubmit {
		t.Fatalf("%s", got.State)
	}
	s.Recover()
	if _, err := s.Acquire("w", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("uncertain resubmitted")
	}
	if f.calls != 1 {
		t.Fatal("wrong call count")
	}
}
func TestSuccessfulCorrelationInputsPersist(t *testing.T) {
	s := testStore(t)
	j, _ := s.Enqueue("fake", "a", "e", "main", []byte("x"), []byte("target"))
	f := &fakeCodex{turn: "turn", result: "answer"}
	r := New(s, f, fakeDelivery{}, "w", 1)
	a, _ := s.Acquire("w", time.Minute)
	r.process(context.Background(), a)
	got, _ := s.Job(j.ID)
	if got.State != model.OutboxPending || f.seenThread != "thread" || f.seenJob != j.ID ||
		f.seenSource != "fake" {
		t.Fatal("routing mismatch")
	}
	o, err := s.NextOutbox()
	if err != nil || string(o.DeliveryTarget) != "target" {
		t.Fatal("durable target missing")
	}
}
func TestDeliveryUncertainIsNotRetried(t *testing.T) {
	s := testStore(t)
	j, _ := s.Enqueue("fake", "a", "e", "main", []byte("x"), nil)
	_, _ = s.Acquire("w", time.Minute)
	s.Transition(j.ID, []model.State{model.Leased}, model.Submitted, "", nil)
	s.Transition(j.ID, []model.State{model.Submitted}, model.Running, "turn", nil)
	s.CompleteToOutbox(j.ID, "fake", []byte("target"), []byte("a"))
	if err := s.MarkOutbox(j.ID, model.DeliveryUncertain); err != nil {
		t.Fatal(err)
	}
	if _, err := s.NextOutbox(); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("uncertain delivery retried")
	}
}
func TestExternalGUIRacePolicy(t *testing.T) {
	s := testStore(t)
	s.Enqueue("fake", "a", "e", "main", []byte("x"), nil)
	o := ExternalObserver{Store: s, TTL: time.Minute}
	o.Started("thread", "gui-turn")
	if _, err := s.Acquire("w", time.Minute); !errors.Is(err, model.ErrNoWork) {
		t.Fatal("bridge raced external turn")
	}
	o.Completed("thread", "gui-turn")
	if _, err := s.Acquire("w", time.Minute); err != nil {
		t.Fatal("queue did not resume")
	}
	o.Started("unknown", "x")
}
func TestMediaPathsReachCodexAndReleaseOnCompletion(t *testing.T) {
	s := testStore(t)
	if err := s.RegisterMedia("image.png", 1, time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	j, err := s.Enqueue("fake", "a", "media", "main", []byte("caption"), []byte("target"), "/private/tmp/image.png")
	if err != nil {
		t.Fatal(err)
	}
	fc := &fakeCodex{turn: "turn", result: "ok"}
	fm := &fakeMedia{}
	r := New(s, fc, fakeDelivery{}, "w", 1)
	r.SetMedia(fm)
	a, _ := s.Acquire("w", time.Minute)
	r.process(context.Background(), a)
	if len(fc.seenMedia) != 1 || fc.seenMedia[0] != "/private/tmp/image.png" {
		t.Fatal("local image not passed to Codex")
	}
	if len(fm.leased) != 1 || len(fm.released) != 1 {
		t.Fatal("media lifecycle incomplete")
	}
	got, _ := s.Job(j.ID)
	if got.State != model.OutboxPending {
		t.Fatalf("%s", got.State)
	}
}
func TestPostSubmitUncertainRetainsBoundedMediaLease(t *testing.T) {
	s := testStore(t)
	if err := s.RegisterMedia("image.png", 1, time.Now(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Enqueue("fake", "a", "media-uncertain", "main", []byte("x"), []byte("target"), "/private/tmp/image.png")
	fc := &fakeCodex{err: &model.UncertainError{Err: errors.New("unknown")}}
	fm := &fakeMedia{}
	r := New(s, fc, fakeDelivery{}, "w", 1)
	r.SetMedia(fm)
	a, _ := s.Acquire("w", time.Minute)
	r.process(context.Background(), a)
	if len(fm.leased) != 1 || len(fm.released) != 0 {
		t.Fatal("uncertain media should remain bounded-leased")
	}
	got, _ := s.Job(j.ID)
	if got.State != model.UncertainPostSubmit {
		t.Fatalf("%s", got.State)
	}
}
