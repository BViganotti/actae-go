package actae

import (
	"context"
	"fmt"
)

// StateManager manages versioned cursor-aligned state snapshots for any
// agent framework. Each Save creates a new immutable version; use
// ListVersions to browse history and GetVersion to inspect a specific
// snapshot.
type StateManager struct {
	actae   *Client
	channel string
}

// NewStateManager creates a StateManager for the given Actae channel.
func NewStateManager(actae *Client, channel string) *StateManager {
	return &StateManager{actae: actae, channel: channel}
}

// Channel returns the managed channel ID.
func (m *StateManager) Channel() string { return m.channel }

// Save persists a state snapshot aligned to the channel's latest cursor.
// Returns the assigned version number.
func (m *StateManager) Save(ctx context.Context, state map[string]any) (int64, error) {
	var cursor int64
	if latest, err := m.actae.LatestCursor(ctx, m.channel); err != nil {
		return 0, err
	} else if latest != nil {
		cursor = *latest
	}
	return m.actae.SaveState(ctx, m.channel, cursor, state, SaveStateOptions{})
}

// Load returns the latest state snapshot's state dict, or nil when no state
// has been saved yet.
func (m *StateManager) Load(ctx context.Context) (map[string]any, error) {
	snapshot, err := m.actae.LatestState(ctx, m.channel)
	if err != nil {
		return nil, err
	}
	if snapshot == nil {
		return nil, nil
	}
	return snapshot.State, nil
}

// LoadSnapshot returns the full latest StateSnapshot (cursor, version,
// state), or nil when no state has been saved yet.
func (m *StateManager) LoadSnapshot(ctx context.Context) (*StateSnapshot, error) {
	return m.actae.LatestState(ctx, m.channel)
}

// Resume loads the latest state, falling back to DefaultState when none
// exists.
func (m *StateManager) Resume(ctx context.Context, defaultState map[string]any) (map[string]any, error) {
	state, err := m.Load(ctx)
	if err != nil {
		return nil, err
	}
	if state == nil {
		if defaultState == nil {
			return map[string]any{}, nil
		}
		return defaultState, nil
	}
	return state, nil
}

// StateManagerForkOptions configures StateManager.Fork.
type StateManagerForkOptions struct {
	// Reason is an optional human-readable reason recorded on the fork.
	Reason string
}

// Fork forks this state into a new channel, returning a StateManager bound
// to it — the framework-agnostic "fork at the current point, refine from
// here" primitive. Load() on the returned manager returns the inherited
// state.
//
// The server forks the latest state at-or-before the channel's latest
// cursor. If no snapshot boundary exists at that cursor
// (SnapshotBoundaryError), the fork is retried once at at_cursor=0 (latest
// state) so event-only channels still fork.
func (m *StateManager) Fork(ctx context.Context, newChannel string, opts StateManagerForkOptions) (*StateManager, error) {
	var cursor int64
	if latest, err := m.actae.LatestCursor(ctx, m.channel); err != nil {
		return nil, err
	} else if latest != nil {
		cursor = *latest
	}
	reason := opts.Reason
	if reason == "" {
		reason = fmt.Sprintf("Forked %s → %s", m.channel, newChannel)
	}
	doFork := func(atCursor int64) error {
		_, err := m.actae.Fork(ctx, m.channel, newChannel, atCursor, ForkOptions{
			Reason:      &reason,
			DisplayName: &newChannel,
		})
		return err
	}
	if err := doFork(cursor); err != nil {
		if _, ok := err.(*SnapshotBoundaryError); !ok {
			return nil, err
		}
		// No snapshot boundary at the latest cursor: try the latest state
		// (at_cursor=0). A channel with no state/events raises again.
		if err := doFork(0); err != nil {
			return nil, err
		}
	}
	return &StateManager{actae: m.actae, channel: newChannel}, nil
}

// ListVersions lists all state version history for this channel (metadata
// only, no blobs), newest-first.
func (m *StateManager) ListVersions(ctx context.Context) ([]StateVersionInfo, error) {
	return m.actae.ListStates(ctx, m.channel, ListStatesOptions{})
}

// GetVersion loads a specific state snapshot version, or nil when the
// version does not exist.
func (m *StateManager) GetVersion(ctx context.Context, version int64) (*StateSnapshot, error) {
	return m.actae.GetState(ctx, m.channel, version)
}

// DeleteVersion deletes a specific state snapshot version.
func (m *StateManager) DeleteVersion(ctx context.Context, version int64) error {
	return m.actae.DeleteState(ctx, m.channel, version)
}
