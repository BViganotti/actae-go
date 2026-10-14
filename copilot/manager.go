package copilot

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	actae "github.com/BViganotti/actae-go"
	copilotsdk "github.com/github/copilot-sdk/go"
)

// ManagerOptions configures a Manager.
type ManagerOptions struct {
	// ChannelPrefix is the Actae channel naming prefix for sessions
	// (default "copilot"), yielding "copilot:<session-id>".
	ChannelPrefix string
	// Recorder carries the default RecorderOptions for tracked sessions.
	Recorder RecorderOptions
}

// Manager owns one Recorder per tracked Copilot session and maps Copilot
// session IDs to Actae channels. It is safe for concurrent use.
type Manager struct {
	actae   *actae.Client
	prefix  string
	recOpts RecorderOptions

	mu        sync.Mutex
	recorders map[string]*Recorder // session ID → recorder
}

// NewManager creates a session manager over an Actae client.
func NewManager(db *actae.Client, opts ManagerOptions) *Manager {
	prefix := opts.ChannelPrefix
	if prefix == "" {
		prefix = "copilot"
	}
	return &Manager{
		actae:     db,
		prefix:    prefix,
		recOpts:   opts.Recorder,
		recorders: make(map[string]*Recorder),
	}
}

// NewManagerFromRuntime is the ergonomic entry point for applications using
// actae.Runtime. The manager still uses Copilot's native session APIs/hooks;
// Runtime does not take over execution.
func NewManagerFromRuntime(runtime *actae.Runtime, opts ManagerOptions) (*Manager, error) {
	if runtime == nil || runtime.Client == nil {
		return nil, errors.New("Actae runtime and client are required")
	}
	return NewManager(runtime.Client, opts), nil
}

// ChannelFor returns the Actae channel name for a Copilot session.
func (m *Manager) ChannelFor(sessionID string) string {
	return m.prefix + ":" + sessionID
}

// NewRecorder creates an unattached recorder preconfigured with the
// manager's default options (channel prefix applied on Attach).
func (m *Manager) NewRecorder(opts RecorderOptions) *Recorder {
	rec := NewRecorder(m.actae, opts)
	rec.opts.ChannelPrefix = m.prefix
	return rec
}

// SessionHandle bundles a tracked Copilot session with its recorder.
type SessionHandle struct {
	Session  *copilotsdk.Session
	Recorder *Recorder
	Channel  string
}

// StartSession creates a Copilot session with hooks wired for recording,
// tracks it, and returns the handle. Config.Hooks is replaced with the
// recording wrapper (chained over the user's hooks); Config is not
// otherwise modified.
func (m *Manager) StartSession(ctx context.Context, client *copilotsdk.Client, config *copilotsdk.SessionConfig) (*SessionHandle, error) {
	return m.createSession(ctx, client, config, false)
}

// ResumeSession resumes an existing Copilot session with hooks wired for
// recording and tracks it. See StartSession for config semantics.
func (m *Manager) ResumeSession(ctx context.Context, client *copilotsdk.Client, sessionID string, config *copilotsdk.ResumeSessionConfig) (*SessionHandle, error) {
	rec := m.NewRecorder(m.recOpts)
	config.Hooks = RecordingHooks(rec, config.Hooks)
	session, err := client.ResumeSession(ctx, sessionID, config)
	if err != nil {
		return nil, err
	}
	if err := rec.Attach(ctx, session); err != nil {
		return nil, err
	}
	m.track(session.SessionID, rec)
	m.recordSessionInfo(ctx, client, rec)
	return &SessionHandle{
		Session:  session,
		Recorder: rec,
		Channel:  rec.ChannelID(),
	}, nil
}

func (m *Manager) createSession(ctx context.Context, client *copilotsdk.Client, config *copilotsdk.SessionConfig, _ bool) (*SessionHandle, error) {
	rec := m.NewRecorder(m.recOpts)
	config.Hooks = RecordingHooks(rec, config.Hooks)
	session, err := client.CreateSession(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := rec.Attach(ctx, session); err != nil {
		return nil, err
	}
	m.track(session.SessionID, rec)
	m.recordSessionInfo(ctx, client, rec)
	return &SessionHandle{
		Session:  session,
		Recorder: rec,
		Channel:  rec.ChannelID(),
	}, nil
}

// sessionMetadataFetcher is the session-metadata surface used by
// recordSessionInfo (implemented by *copilotsdk.Client; injectable in
// tests).
type sessionMetadataFetcher interface {
	GetSessionMetadata(ctx context.Context, sessionID string) (*copilotsdk.SessionMetadata, error)
}

// recordSessionInfo fetches the session's metadata (summary, times,
// context) and records it as a copilot.session.info event. Best-effort:
// metadata is a convenience, not critical-path — failures are logged and
// session startup proceeds.
func (m *Manager) recordSessionInfo(ctx context.Context, client sessionMetadataFetcher, rec *Recorder) {
	sessionID := rec.SessionID()
	meta, err := client.GetSessionMetadata(ctx, sessionID)
	if err != nil {
		log.Printf("copilot: fetch session metadata for %s failed: %v", sessionID, err)
		return
	}
	if meta == nil {
		return
	}
	payload := map[string]any{
		"session_id":    sessionID,
		"start_time":    meta.StartTime.Format(time.RFC3339),
		"modified_time": meta.ModifiedTime.Format(time.RFC3339),
		"is_remote":     meta.IsRemote,
	}
	if meta.Summary != nil {
		payload["summary"] = *meta.Summary
	}
	if meta.Context != nil {
		payload["context"] = meta.Context
	}
	if _, err := rec.Record(ctx, "copilot.session.info", payload, map[string]any{
		"session_id": sessionID,
	}); err != nil {
		log.Printf("copilot: record session info for %s failed: %v", sessionID, err)
	}
}

// Track attaches a recorder to an already-created Copilot session and
// registers it. Idempotent per session ID — an existing recorder is
// returned as-is.
func (m *Manager) Track(ctx context.Context, session *copilotsdk.Session) (*Recorder, error) {
	m.mu.Lock()
	if rec, ok := m.recorders[session.SessionID]; ok {
		m.mu.Unlock()
		return rec, nil
	}
	m.mu.Unlock()

	rec := m.NewRecorder(m.recOpts)
	if err := rec.Attach(ctx, session); err != nil {
		return nil, err
	}
	m.track(session.SessionID, rec)
	return rec, nil
}

func (m *Manager) track(sessionID string, rec *Recorder) {
	m.mu.Lock()
	m.recorders[sessionID] = rec
	m.mu.Unlock()
}

// RecorderFor returns the recorder tracking a session, or nil.
func (m *Manager) RecorderFor(sessionID string) *Recorder {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.recorders[sessionID]
}

// EndSession records the copilot.session.ended lifecycle event for a
// tracked session (at most once; idempotent with the session_end hook).
// Does not stop the recorder — call Untrack/StopAll for that.
func (m *Manager) EndSession(ctx context.Context, sessionID, reason string) error {
	rec := m.RecorderFor(sessionID)
	if rec == nil {
		return NewError("session %s is not tracked", sessionID)
	}
	return rec.RecordSessionEnded(ctx, reason)
}

// Untrack stops and forgets a session's recorder. The Copilot session
// itself is left untouched.
func (m *Manager) Untrack(sessionID string) {
	m.mu.Lock()
	rec := m.recorders[sessionID]
	delete(m.recorders, sessionID)
	m.mu.Unlock()
	if rec != nil {
		rec.Stop()
	}
}

// StopAll stops every tracked recorder. Safe to call multiple times.
func (m *Manager) StopAll() {
	m.mu.Lock()
	recs := make([]*Recorder, 0, len(m.recorders))
	for _, rec := range m.recorders {
		recs = append(recs, rec)
	}
	m.recorders = make(map[string]*Recorder)
	m.mu.Unlock()
	for _, rec := range recs {
		rec.Stop()
	}
}

// ForkOptions configures Manager.Fork.
type ForkOptions struct {
	// NewChannelID is the Actae channel for the fork. Default:
	// ChannelFor(sessionID) + ".fork.<n>" is NOT auto-generated — pass an
	// explicit ID.
	NewChannelID string
	// DisplayName for the fork channel (defaults to NewChannelID).
	DisplayName string
	// Reason recorded on the fork (default "Forked from <channel> at event
	// <event-id>").
	Reason string
}

// Fork forks a tracked session's Actae channel at the cursor of a specific
// Copilot event (identified by its event ID). The cursor is resolved from
// the recorder's in-memory event→cursor map, falling back to a replay of
// the channel. The fork is strict: it must inherit a restorable state
// boundary at-or-before the event's cursor — a missing snapshot propagates
// SnapshotBoundaryError rather than silently forking from the latest state
// (which would contaminate the fork with post-event data). Call
// Manager.Snapshot at the desired boundary before forking.
//
// Returns the new channel ID.
func (m *Manager) Fork(ctx context.Context, sessionID, atEventID string, opts ForkOptions) (string, error) {
	rec := m.RecorderFor(sessionID)
	if rec == nil {
		return "", NewError("session %s is not tracked", sessionID)
	}
	sourceChannel := rec.ChannelID()
	if sourceChannel == "" {
		sourceChannel = m.ChannelFor(sessionID)
	}

	cursor, ok := rec.CursorForEvent(atEventID)
	if !ok {
		var err error
		cursor, err = resolveCursorByEventID(ctx, m.actae, sourceChannel, atEventID)
		if err != nil {
			return "", err
		}
	}

	newChannel := opts.NewChannelID
	if newChannel == "" {
		return "", NewError("NewChannelID is required")
	}
	displayName := opts.DisplayName
	if displayName == "" {
		displayName = newChannel
	}
	reason := opts.Reason
	if reason == "" {
		reason = "Forked from " + sourceChannel + " at event " + atEventID
	}

	experimentMetadata := map[string]any{
		"session_name":     newChannel,
		"forked_from":      sourceChannel,
		"forked_at_event":  atEventID,
		"forked_at_cursor": cursor,
		"status":           "created",
	}

	// Strict boundary: the fork must inherit a restorable checkpoint at-or-
	// before the event's cursor. A missing snapshot propagates
	// SnapshotBoundaryError — silently forking from the latest state would
	// contaminate the new session with data from after the fork event.
	if _, err := m.actae.Fork(ctx, sourceChannel, newChannel, cursor, actae.ForkOptions{
		DisplayName:        &displayName,
		Reason:             &reason,
		ExperimentMetadata: experimentMetadata,
	}); err != nil {
		return "", err
	}
	return newChannel, nil
}

// ContinueSessionOptions configures ContinueSession.
type ContinueSessionOptions struct {
	// ForkChannelID is the Actae channel for the fork and its continuation
	// (required). The new Copilot session records directly into it, so the
	// channel carries the full lineage: source events up to the fork
	// event, then the continuation's events — exactly like the Python
	// AgentSession.fork() where the fork channel is the new session.
	ForkChannelID string
	// DisplayName and Reason for the fork channel (defaults mirror Fork).
	DisplayName string
	Reason      string
}

// ContinueSession forks a tracked session at a specific event AND starts a
// new Copilot session that continues recording into the fork channel.
//
// The fork channel becomes the living channel of the new session: it
// contains the source history up to the fork event (server-side fork with
// parent_channel_id), then the new session's events, lifecycle markers
// (copilot.session.started/forked/info) and any snapshots.
//
// To seed the new session with context from the fork point, replay the
// fork channel (or LoadSnapshot) and pass the material into the first
// prompt or the SessionConfig.SystemMessage.
func (m *Manager) ContinueSession(ctx context.Context, client *copilotsdk.Client, sourceSessionID, atEventID string, config *copilotsdk.SessionConfig, opts ContinueSessionOptions) (*SessionHandle, error) {
	if opts.ForkChannelID == "" {
		return nil, NewError("ForkChannelID is required")
	}

	// 1) Fork the source channel at the event.
	if _, err := m.Fork(ctx, sourceSessionID, atEventID, ForkOptions{
		NewChannelID: opts.ForkChannelID,
		DisplayName:  opts.DisplayName,
		Reason:       opts.Reason,
	}); err != nil {
		return nil, err
	}

	// 2) Start a new Copilot session recording INTO the fork channel.
	rec := m.NewRecorder(m.recOpts)
	rec.opts.ChannelID = opts.ForkChannelID
	config.Hooks = RecordingHooks(rec, config.Hooks)
	session, err := client.CreateSession(ctx, config)
	if err != nil {
		return nil, err
	}
	if err := rec.Attach(ctx, session); err != nil {
		return nil, err
	}
	m.track(session.SessionID, rec)

	// 3) Link the fork relationship on the fork channel.
	srcRec := m.RecorderFor(sourceSessionID)
	sourceChannel := ""
	if srcRec != nil {
		sourceChannel = srcRec.ChannelID()
	}
	if sourceChannel == "" {
		sourceChannel = m.ChannelFor(sourceSessionID)
	}
	_, err = rec.Record(ctx, "copilot.session.forked", map[string]any{
		"session_id":        session.SessionID,
		"source_session_id": sourceSessionID,
		"source_channel":    sourceChannel,
		"forked_at_event":   atEventID,
	}, map[string]any{
		"session_id":        session.SessionID,
		"source_session_id": sourceSessionID,
	})
	if err != nil {
		log.Printf("copilot: record session.forked failed: %v", err)
	}

	m.recordSessionInfo(ctx, client, rec)
	return &SessionHandle{
		Session:  session,
		Recorder: rec,
		Channel:  rec.ChannelID(),
	}, nil
}

// resolveCursorByEventID replays a channel to find the Actae cursor of the
// event whose metadata.event_id matches (fallback for recorders without
// in-memory state, e.g. after a restart). The server caps replay at 1000
// events per call, so the replay is paged to cover the whole channel.
func resolveCursorByEventID(ctx context.Context, db *actae.Client, channel, eventID string) (int64, error) {
	var cursor *int64
	for {
		events, err := db.Replay(ctx, channel, actae.ReplayOptions{Cursor: cursor, Limit: 1000})
		if err != nil {
			return 0, err
		}
		for _, ev := range events {
			if id, ok := ev.Metadata["event_id"].(string); ok && id == eventID {
				return ev.Cursor, nil
			}
		}
		if len(events) == 0 {
			break
		}
		last := events[len(events)-1]
		if cursor != nil && last.Cursor <= *cursor {
			break // no progress — avoid an infinite loop on a broken server
		}
		cursor = &last.Cursor
	}
	return 0, NewError("event %s not found on channel %s", eventID, channel)
}

// Snapshot saves a cursor-aligned state snapshot for a session via the
// Actae StateManager semantics (latest channel cursor as the alignment
// point). Returns the assigned version.
func (m *Manager) Snapshot(ctx context.Context, sessionID string, state map[string]any) (int64, error) {
	channel := m.ChannelFor(sessionID)
	if rec := m.RecorderFor(sessionID); rec != nil && rec.ChannelID() != "" {
		channel = rec.ChannelID()
	}
	envelope := actae.NewCheckpointEnvelope("github-copilot-sdk", "2", channel)
	envelope.PortableState = state
	envelope.NativeCheckpoint = map[string]any{"session_id": sessionID}
	stateWithMetadata, err := actae.EmbedCheckpointMetadata(state, envelope)
	if err != nil {
		return 0, err
	}
	sm := actae.NewStateManager(m.actae, channel)
	return sm.Save(ctx, stateWithMetadata)
}

// LoadSnapshot returns the latest saved state for a session, or nil.
func (m *Manager) LoadSnapshot(ctx context.Context, sessionID string) (map[string]any, error) {
	channel := m.ChannelFor(sessionID)
	if rec := m.RecorderFor(sessionID); rec != nil && rec.ChannelID() != "" {
		channel = rec.ChannelID()
	}
	sm := actae.NewStateManager(m.actae, channel)
	state, err := sm.Load(ctx)
	if err != nil || state == nil {
		return state, err
	}
	return actae.StripCheckpointMetadata(state), nil
}

// ListSnapshots lists state version history for a session (metadata only,
// no blobs), newest-first.
func (m *Manager) ListSnapshots(ctx context.Context, sessionID string) ([]actae.StateVersionInfo, error) {
	channel := m.ChannelFor(sessionID)
	if rec := m.RecorderFor(sessionID); rec != nil && rec.ChannelID() != "" {
		channel = rec.ChannelID()
	}
	sm := actae.NewStateManager(m.actae, channel)
	return sm.ListVersions(ctx)
}
