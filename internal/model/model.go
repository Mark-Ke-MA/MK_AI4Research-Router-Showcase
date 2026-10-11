package model

import "errors"

type State string

const (
	Received            State = "received"
	Normalized          State = "normalized"
	Queued              State = "queued"
	Leased              State = "leased"
	Submitted           State = "submitted"
	Running             State = "running"
	Completed           State = "completed"
	OutboxPending       State = "outbox_pending"
	Delivered           State = "delivered"
	RetryablePreSubmit  State = "retryable_pre_submit"
	UncertainPostSubmit State = "uncertain_post_submit"
	DeliveryRetryable   State = "delivery_retryable"
	DeliveryUncertain   State = "delivery_uncertain"
	DeadLetter          State = "dead_letter"
)

type Job struct {
	ID             string
	EventDigest    string
	Alias          string
	ThreadID       string
	State          State
	Sequence       int64
	Payload        []byte
	Result         []byte
	TurnID         string
	Attempts       int
	Platform       string
	DeliveryTarget []byte
	MediaPaths     []string
}
type MediaRecord struct {
	RelativePath                                   string
	Size, CreatedMS, ExpiresMS, LeaseHardExpiresMS int64
	Leases, References                             int
}

type Inbound struct {
	Platform       string
	EventKey       string
	Account        string
	Owner          string
	Conversation   string
	Topic          string
	Payload        []byte
	DeliveryTarget []byte
	MediaPaths     []string
}

// Activity is durable best-effort UI state. Target and ReactionID are opaque
// platform locators and must never be logged or exposed through health.
type Activity struct {
	JobID, Platform, EndpointDigest, State string
	Target, ReactionID                     []byte
	CreatedMS, ExpiresMS, NextAttemptMS    int64
	Attempts                               int
	Terminal                               bool
	RecoverableTerminal                    bool
}

// MainRef is the generation-scoped protected alias. Thread IDs are protected
// state and must never be emitted in logs, health responses, or CLI errors.
type MainRef struct {
	ThreadID   string
	Generation int64
}

// MainEpoch is one exact, committed protected-main generation. EffectiveToNS
// is zero only for the currently open epoch. Digests are integrity-chain
// values, not credentials; callers must still treat ThreadID as protected
// operational state.
type MainEpoch struct {
	Generation                     int64
	ThreadID, TransitionKind       string
	EffectiveFromNS, EffectiveToNS int64
	PreviousDigest, Digest         string
}

// MainHistoryBaseline is the deliberately non-historical observation created
// when a schema-v4 database is first migrated. It proves which generation was
// canonical at ObservedFromNS without inventing its original commit time.
type MainHistoryBaseline struct {
	Generation                   int64
	ThreadID, Digest             string
	ObservedFromNS, ObservedToNS int64
}

type MainEpochHistory struct {
	Persona, Origin      string
	AvailableFromNS      int64
	PriorHistoryComplete bool
	Baseline             *MainHistoryBaseline
	HeadGeneration       int64
	HeadDigest           string
	Epochs               []MainEpoch
}

type Rotation struct {
	OperationID                        string
	OldThreadID, NewThreadID           string
	OldGeneration, CommittedGeneration int64
	Phase                              string
	CleanupIncomplete                  bool
}

var (
	ErrDuplicate         = errors.New("duplicate event")
	ErrNoWork            = errors.New("no eligible work")
	ErrProtectedAlias    = errors.New("protected alias")
	ErrEndpoint          = errors.New("endpoint is not bound")
	ErrCrossPersona      = errors.New("persona mismatch")
	ErrExternalLease     = errors.New("external lease active")
	ErrUnsafeTransition  = errors.New("unsafe state transition")
	ErrOwnerConflict     = errors.New("owner binding conflict")
	ErrHistoryIncomplete = errors.New("main history incomplete")
	ErrHistoryCorrupt    = errors.New("main history corrupt")
	ErrHistoryBusy       = errors.New("main history busy")
)

type PreSubmitError struct{ Err error }

func (e *PreSubmitError) Error() string { return "pre-submit transport failure" }
func (e *PreSubmitError) Unwrap() error { return e.Err }

type UncertainError struct{ Err error }

func (e *UncertainError) Error() string { return "post-submit outcome uncertain" }
func (e *UncertainError) Unwrap() error { return e.Err }

type DeliveryUncertainError struct{ Err error }

func (e *DeliveryUncertainError) Error() string { return "delivery outcome uncertain" }
func (e *DeliveryUncertainError) Unwrap() error { return e.Err }
