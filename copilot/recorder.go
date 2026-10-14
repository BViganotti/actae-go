// Package copilot integrates Actae with the GitHub Copilot SDK for Go
// (github.com/github/copilot-sdk/go).
//
// Every Copilot session event — user messages, assistant turns and answers,
// tool executions, errors, sub-agents, permission requests — is recorded as
// a first-class Actae event on a per-session channel, giving you:
//
//   - a complete, replayable timeline of every Copilot session,
//   - session hooks (pre/post tool use, user prompts, session start/end,
//     errors, MCP calls) recorded as events,
//   - fork an entire session at a specific event into a new experiment
//     fork, and
//   - cursor-aligned state snapshots for crash recovery and resumption.
//
// # Quick start
//
//	import (
//	    actae "github.com/BViganotti/actae-go"
//	    copilot "github.com/BViganotti/actae-go/copilot"
//	    copilotsdk "github.com/github/copilot-sdk/go"
//	)
//
//	db, _ := actae.NewClient(actae.ClientOptions{APIKey: "sk-dev-...", Endpoint: "http://localhost:8002"})
//	cli, _ := copilotsdk.NewClient(copilotsdk.ClientOptions{Connection: "/path/to/copilot"})
//	defer cli.Stop()
//
//	mgr := copilot.NewManager(db, copilot.ManagerOptions{})
//	defer mgr.StopAll()
//
//	handle, err := mgr.StartSession(ctx, cli, &copilotsdk.SessionConfig{})
//	if err != nil { ... }
//	_, err = handle.Session.SendPromptAndWait(ctx, "Refactor the parser")
//
//	// Fork the session at a specific event:
//	eventID := "3f2f4a72-8e6a-4d1c-9b5e-1a2b3c4d5e6f"
//	newChannel, err := mgr.Fork(ctx, handle.Session.SessionID, eventID,
//	    copilot.ForkOptions{NewChannelID: "experiment/refactor-v2"})
package copilot

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	actae "github.com/BViganotti/actae-go"
	copilotsdk "github.com/github/copilot-sdk/go"
)

// Default values for RecorderOptions.
const (
	DefaultActor     = "copilot"
	DefaultQueueSize = 1024
	// maxSeenEvents bounds the in-memory dedup window (event ID → last seen).
	maxSeenEvents = 5000
)

// RecorderOptions configures a Recorder.
type RecorderOptions struct {
	// ChannelPrefix is the default Actae channel prefix for unattached
	// sessions ("copilot:<session-id>"). Ignored when ChannelID is set.
	// Managers override this with their own prefix.
	ChannelPrefix string
	// ChannelID overrides the Actae channel for the recorded session.
	ChannelID string
	// Actor stamped on every recorded event (default "copilot").
	Actor string
	// QueueSize bounds the async record queue (default 1024). When the
	// queue is full, events are dropped with a log line rather than
	// blocking the Copilot SDK's event dispatch.
	QueueSize int
	// IncludeEphemeral records transient events (default: skipped).
	IncludeEphemeral bool
	// EventTypes restricts recording to the given copilot event types
	// (e.g. "user.message", "tool_execution.complete"). Nil records all.
	EventTypes []string
}

func (o RecorderOptions) withDefaults() RecorderOptions {
	if o.Actor == "" {
		o.Actor = DefaultActor
	}
	if o.QueueSize <= 0 {
		o.QueueSize = DefaultQueueSize
	}
	if o.ChannelPrefix == "" {
		o.ChannelPrefix = "copilot"
	}
	return o
}

type recordJob struct {
	eventType string
	payload   any
	metadata  map[string]any
}

// eventHistoryFetcher is the session event-history surface used by
// Backfill (implemented by *copilotsdk.Session; injectable in tests).
type eventHistoryFetcher interface {
	GetEvents(ctx context.Context) ([]copilotsdk.SessionEvent, error)
}

// Recorder asynchronously records Copilot session events into a single Actae
// channel. Create it with NewRecorder, then Attach it to a session (or use
// Manager.StartSession which wires everything up).
//
// Recording is non-blocking: the Copilot SDK's event handlers and session
// hooks push jobs onto an internal queue and a worker goroutine persists
// them to Actae. Transient Actae failures are logged and dropped — the
// recorder must never stall the agent.
type Recorder struct {
	actae   *actae.Client
	opts    RecorderOptions
	session *copilotsdk.Session
	history eventHistoryFetcher // set at Attach; overridable for tests

	jobs       chan recordJob
	stopCh     chan struct{}
	workerDone chan struct{}
	workerOnce sync.Once
	stopOnce   sync.Once
	unsub      func()

	mu           sync.Mutex
	channel      string
	attached     bool
	sessionEnded bool
	eventCursors map[string]int64 // copilot event ID → Actae cursor
	seen         map[string]struct{}
	seenOrder    []string
}

// NewRecorder creates a recorder for a session that does not exist yet.
// Call Attach once the session has been created. Recorders created this way
// can be passed to RecordingHooks before the session exists — hook
// invocations are queued and drained as soon as Attach starts the worker.
func NewRecorder(db *actae.Client, opts RecorderOptions) *Recorder {
	opts = opts.withDefaults()
	return &Recorder{
		actae:        db,
		opts:         opts,
		jobs:         make(chan recordJob, opts.QueueSize),
		stopCh:       make(chan struct{}),
		workerDone:   make(chan struct{}),
		eventCursors: make(map[string]int64),
		seen:         make(map[string]struct{}),
	}
}

// ChannelID returns the Actae channel backing this recorder ("" until
// Attach).
func (r *Recorder) ChannelID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channel
}

// SessionID returns the attached Copilot session ID ("" before Attach).
func (r *Recorder) SessionID() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.session == nil {
		return ""
	}
	return r.session.SessionID
}

// Attach binds the recorder to a Copilot session, starts the worker,
// registers the event handler and records the session.started lifecycle
// event. Safe to call once; subsequent calls are no-ops.
func (r *Recorder) Attach(ctx context.Context, session *copilotsdk.Session) error {
	r.mu.Lock()
	if r.attached {
		r.mu.Unlock()
		return nil
	}
	r.session = session
	r.history = session
	channel := r.opts.ChannelID
	if channel == "" {
		channel = r.opts.ChannelPrefix + ":" + session.SessionID
	}
	r.channel = channel
	r.attached = true
	r.mu.Unlock()

	r.workerOnce.Do(func() { go r.worker() })

	r.unsub = session.On(r.handleEvent)

	ev, err := r.actae.Record(ctx, channel, "copilot.session.started", map[string]any{
		"session_id": session.SessionID,
	}, actae.RecordOptions{
		Actor: r.opts.Actor,
		Metadata: map[string]any{
			"session_id": session.SessionID,
			"source":     "recorder.attach",
		},
	})
	if err != nil {
		log.Printf("copilot: record session.started failed for %s: %v", session.SessionID, err)
	}
	r.mu.Lock()
	r.eventCursors["session.started:"+session.SessionID] = ev.Cursor
	r.mu.Unlock()
	return nil
}

// Stop unsubscribes the event handler, stops the worker after draining the
// queue, and closes the recorder. Safe to call multiple times. Does not
// disconnect the Copilot session.
func (r *Recorder) Stop() {
	r.stopOnce.Do(func() {
		if r.unsub != nil {
			r.unsub()
		}
		close(r.stopCh)
		<-r.workerDone
	})
}

// CursorForEvent returns the Actae cursor recorded for a Copilot event ID.
// Used by Manager.Fork to resolve fork boundaries.
func (r *Recorder) CursorForEvent(eventID string) (int64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.eventCursors[eventID]
	return c, ok
}

// Record persists an arbitrary event type on the recorder's channel (e.g.
// lifecycle markers or user annotations). Returns the persisted Event.
func (r *Recorder) Record(ctx context.Context, eventType string, payload any, metadata map[string]any) (actae.Event, error) {
	channel := r.ChannelID()
	if channel == "" {
		return actae.Event{}, NewError("recorder not attached to a session")
	}
	if metadata == nil {
		metadata = map[string]any{}
	}
	if metadata["session_id"] == nil {
		metadata["session_id"] = r.SessionID()
	}
	return r.actae.Record(ctx, channel, eventType, payload, actae.RecordOptions{
		Actor:    r.opts.Actor,
		Metadata: metadata,
	})
}

// RecordHook queues a session-hook invocation for recording (used by
// RecordingHooks). Fires immediately; the worker persists asynchronously.
func (r *Recorder) RecordHook(name string, sessionID string, input any) {
	raw, err := json.Marshal(input)
	if err != nil {
		log.Printf("copilot: marshal hook %s input failed: %v", name, err)
		return
	}
	r.push(recordJob{
		eventType: "copilot.hook." + name,
		payload: map[string]any{
			"hook":  name,
			"input": json.RawMessage(raw),
		},
		metadata: map[string]any{"session_id": sessionID},
	})
}

// RecordSessionEnded records the copilot.session.ended lifecycle event at
// most once per session (subsequent calls are no-ops), with the given
// reason. Used by RecordingHooks.OnSessionEnd and Manager.EndSession so an
// automatic hook-fired end and an explicit EndSession cannot double-record.
//
// The explicit (Manager.EndSession) path is synchronous so the caller can
// observe persistence failures; the hook path must never block the SDK's
// JSON-RPC loop, so it uses the asynchronous queueSessionEnded variant.
func (r *Recorder) RecordSessionEnded(ctx context.Context, reason string) error {
	if !r.claimSessionEnded() {
		return nil
	}
	channel := r.ChannelID()
	sessionID := r.SessionID()
	if channel == "" {
		return NewError("recorder not attached to a session")
	}
	_, err := r.actae.Record(ctx, channel, "copilot.session.ended", map[string]any{
		"session_id": sessionID,
		"reason":     reason,
	}, actae.RecordOptions{
		Actor:    r.opts.Actor,
		Metadata: map[string]any{"session_id": sessionID, "reason": reason},
	})
	if err != nil {
		return err
	}
	return nil
}

// claimSessionEnded atomically claims the once-only right to record
// copilot.session.ended. Returns false when already claimed.
func (r *Recorder) claimSessionEnded() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sessionEnded {
		return false
	}
	r.sessionEnded = true
	return true
}

// queueSessionEnded is the non-blocking variant used from the session_end
// hook (which runs on the SDK's JSON-RPC loop and must never stall). The
// once-only guard is claimed synchronously; the worker persists the event.
func (r *Recorder) queueSessionEnded(reason string, sessionID string) {
	if !r.claimSessionEnded() {
		return
	}
	r.push(recordJob{
		eventType: "copilot.session.ended",
		payload: map[string]any{
			"session_id": sessionID,
			"reason":     reason,
		},
		metadata: map[string]any{"session_id": sessionID, "reason": reason},
	})
}

// Backfill replays the session's full event history via the Copilot SDK's
// Session.GetEvents and records any events not already seen (live-attach
// recovery: a recorder attached mid-session, e.g. after a restart, catches
// up on everything that happened before it started listening).
//
// Deduplication is by Copilot event ID, so events delivered both live and
// via GetEvents are recorded exactly once. GetEvents history includes
// transient events; the same Ephemeral/EventTypes filters apply.
//
// Backfill is best-effort: it runs synchronously and returns the number of
// newly recorded events. A non-nil error means the history could not be
// fetched (e.g. the session runtime is not reachable); already-recorded
// events are never lost or duplicated.
func (r *Recorder) Backfill(ctx context.Context) (int, error) {
	r.mu.Lock()
	history := r.history
	r.mu.Unlock()
	if history == nil {
		return 0, NewError("recorder not attached to a session")
	}
	events, err := history.GetEvents(ctx)
	if err != nil {
		return 0, err
	}
	recorded := 0
	for _, event := range events {
		r.mu.Lock()
		_, seen := r.seen[event.ID]
		r.mu.Unlock()
		if seen {
			continue
		}
		r.handleEvent(event)
		recorded++
	}
	return recorded, nil
}

// handleEvent is the Copilot SDK event handler. It runs on the SDK's
// serial event-dispatch goroutine, so it must never block: events are
// deduplicated, marshalled and queued.
func (r *Recorder) handleEvent(event copilotsdk.SessionEvent) {
	if event.Data == nil {
		return
	}
	if event.Ephemeral != nil && *event.Ephemeral && !r.opts.IncludeEphemeral {
		return
	}
	if r.opts.EventTypes != nil && !containsString(r.opts.EventTypes, string(event.Type())) {
		return
	}

	r.mu.Lock()
	if _, ok := r.seen[event.ID]; ok {
		r.mu.Unlock()
		return
	}
	r.markSeenLocked(event.ID)
	sessionID := ""
	if r.session != nil {
		sessionID = r.session.SessionID
	}
	r.mu.Unlock()

	dataJSON, err := json.Marshal(event.Data)
	if err != nil {
		log.Printf("copilot: marshal event %s failed: %v", event.Type(), err)
		return
	}
	metadata := map[string]any{
		"session_id": sessionID,
		"event_id":   event.ID,
	}
	if event.ParentID != nil {
		metadata["parent_event_id"] = *event.ParentID
	}
	if event.AgentID != nil {
		metadata["agent_id"] = *event.AgentID
	}
	if event.Ephemeral != nil {
		metadata["ephemeral"] = *event.Ephemeral
	}

	r.push(recordJob{
		eventType: "copilot." + string(event.Type()),
		payload: map[string]any{
			"event_type": string(event.Type()),
			"data":       json.RawMessage(dataJSON),
		},
		metadata: metadata,
	})
}

func (r *Recorder) push(job recordJob) {
	r.workerOnce.Do(func() { go r.worker() })
	select {
	case r.jobs <- job:
	default:
		log.Printf("copilot: record queue full, dropping %s for session %s",
			job.eventType, job.metadata["session_id"])
	}
}

func (r *Recorder) worker() {
	defer close(r.workerDone)
	for {
		select {
		case job := <-r.jobs:
			r.record(job)
		case <-r.stopCh:
			for {
				select {
				case job := <-r.jobs:
					r.record(job)
				default:
					return
				}
			}
		}
	}
}

func (r *Recorder) record(job recordJob) {
	channel := r.ChannelID()
	if channel == "" {
		if sid, ok := job.metadata["session_id"].(string); ok && sid != "" {
			channel = r.opts.ChannelPrefix + ":" + sid
		}
	}
	if channel == "" {
		log.Printf("copilot: dropping %s — recorder has no channel", job.eventType)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ev, err := r.actae.Record(ctx, channel, job.eventType, job.payload, actae.RecordOptions{
		Actor:    r.opts.Actor,
		Metadata: job.metadata,
	})
	if err != nil {
		log.Printf("copilot: record %s failed: %v", job.eventType, err)
		return
	}
	if eventID, ok := job.metadata["event_id"].(string); ok && eventID != "" {
		r.mu.Lock()
		r.eventCursors[eventID] = ev.Cursor
		r.mu.Unlock()
	}
}

// markSeenLocked records the event ID for dedup, evicting the oldest entry
// beyond maxSeenEvents. Caller holds r.mu.
func (r *Recorder) markSeenLocked(id string) {
	r.seen[id] = struct{}{}
	r.seenOrder = append(r.seenOrder, id)
	if len(r.seenOrder) > maxSeenEvents {
		old := r.seenOrder[0]
		r.seenOrder = r.seenOrder[1:]
		delete(r.seen, old)
	}
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
