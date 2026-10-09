// Package state provides a transactional, typed state store on top of an
// Actae channel. Commit runs the classic optimistic-concurrency loop:
//
//	load latest snapshot → mutate → Transition (atomic event + snapshot,
//	guarded by expected_version/expected_cursor) → retry on conflict
//
// Every commit publishes an event AND its resulting state snapshot in one
// server-side transaction, so the event stream and the state object can
// never disagree. Transport failures are retried with the SAME request
// content, whose digest is folded into a deterministic operation id —
// server-side idempotency then returns the original result rather than
// duplicating event or snapshot. Caller-side operation keys are declared
// once per logical operation.
package state

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/BViganotti/actae-go"
)

const (
	commitScope = "state.commit"
	maxAttempts = 5
)

// Command describes the event a Commit publishes alongside the state
// snapshot it produces.
type Command struct {
	// EventType is the transition's event type (e.g. "counter.incremented").
	// Required.
	EventType string
	// OperationKey identifies the logical operation this commit represents.
	// It MUST be stable across retries and restarts of the same logical
	// operation (see the Store.New contract), and it MUST be distinct for
	// distinct operations.
	//
	// The final server-side operation id also folds in a digest of the
	// request content (event type, payload, state, metadata, actor), so:
	//   - retries that send the exact same content reuse the original
	//     operation id and the server replays the original transition;
	//   - a re-application that produces different content (e.g. after a
	//     conflict reload bumped the state) gets a NEW operation id and is
	//     recorded as its own transition.
	// OperationKey is the caller-owned identity; the digest prevents
	// accidentally reusing it for different content.
	OperationKey string
	// Payload is the event payload (anything json.Marshal accepts). It must
	// be deterministic for a given logical operation — a payload that
	// embeds a random nonce would make every retry look like a new
	// operation. May be nil for events without a payload.
	Payload any
	// Metadata is merged into the transition metadata (optional).
	Metadata map[string]any
	// Actor is recorded in the transition metadata (optional).
	Actor string
}

// Result is the outcome of a Commit.
type Result[T any] struct {
	// Event is the persisted transition event.
	Event actae.Event
	// StateVersion is the new snapshot's immutable version number.
	StateVersion int64
	// State is the committed snapshot decoded back into T.
	State T
}

// Store is a typed, concurrency-safe, transactional state store backed by a
// single Actae channel. T is the Go type the state is encoded as.
//
// The Mutate function passed to New MUST be:
//   - pure with respect to its input: given the same current state it must
//     produce the same Command, because the conflicted path re-runs it
//     against a freshly loaded snapshot (deterministic re-application
//     semantics, exactly like a compare-and-swap CAS loop);
//   - a deliberate no-op when its operation was already applied, i.e. it
//     must report the same OperationKey for the same logical operation even
//     when the state already carries its effect. With identical request
//     content the derived operation id matches the original, and the server
//     replays the original transition (exactly-once across restarts).
//   - free of content that varies per invocation: the command payload is
//     part of the operation id digest, so a payload that embeds a random
//     nonce or timestamp would make every retry look like a new operation
//     (see Command).
type Store[T any] struct {
	client  *actae.Client
	channel string
	mutate  func(current *T) (Command, error)
}

// New validates the wiring and returns a Store bound to a channel.
func New[T any](client *actae.Client, channel string, mutate func(current *T) (Command, error)) (*Store[T], error) {
	if client == nil {
		return nil, errors.New("state: nil client")
	}
	if channel == "" {
		return nil, errors.New("state: empty channel id")
	}
	if mutate == nil {
		return nil, errors.New("state: nil mutate function")
	}
	return &Store[T]{client: client, channel: channel, mutate: mutate}, nil
}

// Commit loads the latest snapshot, runs Mutate, and atomically publishes
// the resulting event + snapshot. On VersionConflictError the snapshot is
// reloaded and Mutate re-applied (up to maxAttempts). On transport errors
// the transition is retried once with the same operation id — because that
// id is derived from the request content (commandDigest), the server
// resolves the retry idempotently when the first attempt actually persisted
// (returning the original event and state version without duplicating
// either).
//
// The server replays transitions whose operation id already exists on the
// channel, so a Commit whose Mutate no-ops against already-applied state
// (restart recovery) returns the original result instead of re-recording —
// exactly-once semantics for the same logical operation with the same
// content. Distinct content under the same OperationKey derives a distinct
// operation id (a NEW transition); the server separately refuses raw API
// operation-id reuse with different content (409 idempotency_conflict),
// which Commit propagates as *actae.IdempotencyConflictError.
func (s *Store[T]) Commit(ctx context.Context) (Result[T], error) {
	var zero Result[T]
	for attempt := 0; attempt < maxAttempts; attempt++ {
		current, version, cursor, err := s.loadCurrent(ctx)
		if err != nil {
			return zero, err
		}
		cmd, err := s.mutate(&current)
		if err != nil {
			return zero, err
		}
		if cmd.EventType == "" {
			return zero, errors.New("state: mutate returned a command with empty EventType")
		}
		if cmd.OperationKey == "" {
			return zero, errors.New("state: mutate returned a command with empty OperationKey")
		}

		digest, err := commandDigest(cmd, current)
		if err != nil {
			return zero, fmt.Errorf("state: cannot serialize command content: %w", err)
		}
		opID := actae.DeterministicOperationKey(commitScope, cmd.EventType, s.channel, cmd.OperationKey, digest)
		opts := actae.TransitionOptions{
			Actor:       cmd.Actor,
			Metadata:    cmd.Metadata,
			OperationID: &opID,
		}
		if version > 0 {
			opts.ExpectedVersion = &version
			opts.ExpectedCursor = &cursor
		} else {
			// Guard semantics: Some(0) means "no snapshot yet".
			zero2, zero3 := int64(0), int64(0)
			opts.ExpectedVersion = &zero2
			opts.ExpectedCursor = &zero3
		}

		res, err := s.transition(ctx, current, cmd, opts)
		if err == nil {
			return Result[T]{Event: res.Event, StateVersion: res.StateVersion, State: current}, nil
		}
		var conflict *actae.VersionConflictError
		if errors.As(err, &conflict) {
			// Another writer won the race; reload and re-apply Mutate.
			continue
		}
		if isTransportError(err) {
			// Same operation id: if the first attempt persisted, the server
			// returns the original transition instead of duplicating it.
			res, err2 := s.transition(ctx, current, cmd, opts)
			if err2 == nil {
				return Result[T]{Event: res.Event, StateVersion: res.StateVersion, State: current}, nil
			}
			return zero, err2
		}
		return zero, err
	}
	return zero, actae.NewVersionConflictError(
		fmt.Sprintf("state: commit failed after %d attempts (channel %q: concurrent writers or permanently conflicting mutations)", maxAttempts, s.channel))
}

func (s *Store[T]) transition(ctx context.Context, state T, cmd Command, opts actae.TransitionOptions) (actae.TransitionResult, error) {
	return s.client.Transition(ctx, s.channel, cmd.EventType, cmd.Payload, state, opts)
}

// commandDigest hashes the transition request content (the parts the
// client sends that are NOT guards or the operation id itself). Same
// content → same digest → same operation id (idempotent replay); changed
// content → new operation id (recorded as its own transition). json.Marshal
// sorts map keys, so the digest is deterministic for equal values.
func commandDigest(cmd Command, state any) (string, error) {
	raw, err := json.Marshal(map[string]any{
		"type":     cmd.EventType,
		"payload":  cmd.Payload,
		"state":    state,
		"metadata": cmd.Metadata,
		"actor":    cmd.Actor,
	})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

// isTransportError reports whether err is a connection-level failure from
// the client (never a server status). Context errors pass through
// unwrapped, so a cancelled/deadlined Commit is NOT retried.
func isTransportError(err error) bool {
	var connErr *actae.ConnectionError
	return errors.As(err, &connErr)
}

// loadCurrent returns the latest committed state (zero T and version/cursor
// 0 when the channel has no snapshot yet).
func (s *Store[T]) loadCurrent(ctx context.Context) (T, int64, int64, error) {
	var zero T
	versions, err := s.client.ListStates(ctx, s.channel, actae.ListStatesOptions{Limit: 1})
	if err != nil {
		return zero, 0, 0, err
	}
	if len(versions) == 0 {
		return zero, 0, 0, nil
	}
	top := versions[0]
	snap, err := s.client.GetState(ctx, s.channel, top.Version)
	if err != nil {
		return zero, 0, 0, err
	}
	if snap == nil || snap.State == nil {
		// Raced with a delete; treat as a fresh channel. The guard (Some(0))
		// makes the transition fail loudly if a writer reappeared mid-flight.
		return zero, 0, 0, nil
	}
	cur, err := decodeState[T](snap.State)
	if err != nil {
		return zero, 0, 0, fmt.Errorf("state: decode snapshot v%d: %w", top.Version, err)
	}
	return cur, top.Version, snap.Cursor, nil
}

// Load returns the latest committed state, or nil when the channel has no
// snapshot yet.
func (s *Store[T]) Load(ctx context.Context) (*T, error) {
	cur, version, _, err := s.loadCurrent(ctx)
	if err != nil {
		return nil, err
	}
	if version == 0 {
		return nil, nil
	}
	return &cur, nil
}

// LoadVersion returns the snapshot at a specific version. The server
// 404s unknown versions, which surfaces as a *actae.APIError with
// StatusCode 404.
//
// Note: state versions come from the server's GLOBAL version sequence, so
// they are not gapless per channel — always address snapshots by a version
// returned from Commit/ListStates, never by arithmetic (e.g. latest − 1).
func (s *Store[T]) LoadVersion(ctx context.Context, version int64) (*T, error) {
	if version <= 0 {
		return nil, errors.New("state: version must be >= 1")
	}
	snap, err := s.client.GetState(ctx, s.channel, version)
	if err != nil {
		return nil, err
	}
	if snap == nil || snap.State == nil {
		return nil, nil
	}
	cur, err := decodeState[T](snap.State)
	if err != nil {
		return nil, fmt.Errorf("state: decode snapshot v%d: %w", version, err)
	}
	return &cur, nil
}

// decodeState re-marshals a snapshot's raw JSON object into T. The SDK
// decodes payloads with int64/float64 fidelity, so int64 fields round-trip
// exactly; values beyond int64 precision follow the SDK's documented
// float64 semantics.
func decodeState[T any](m map[string]any) (T, error) {
	var out T
	raw, err := json.Marshal(m)
	if err != nil {
		return out, err
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, err
	}
	return out, nil
}
