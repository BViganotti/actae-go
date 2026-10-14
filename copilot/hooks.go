package copilot

import (
	"errors"
	"fmt"

	copilotsdk "github.com/github/copilot-sdk/go"
)

// NewError creates a plain error with a "copilot:" prefix.
func NewError(format string, args ...any) error {
	return errors.New("copilot: " + fmt.Sprintf(format, args...))
}

// RecordingHooks wraps a user-supplied *copilotsdk.SessionHooks so every
// hook invocation is recorded to Actae (event type "copilot.hook.<name>")
// before the user's handler runs. Pass the result as SessionConfig.Hooks
// when creating a Copilot session.
//
// The user's handlers are optional — nil entries are filled with
// record-only handlers, so a nil *SessionHooks still records all hook
// types.
//
// Recording is asynchronous and never modifies the user's inputs or
// outputs.
func RecordingHooks(rec *Recorder, next *copilotsdk.SessionHooks) *copilotsdk.SessionHooks {
	h := &copilotsdk.SessionHooks{}

	h.OnPreToolUse = func(input copilotsdk.PreToolUseHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.PreToolUseHookOutput, error) {
		rec.RecordHook("pre_tool_use", inv.SessionID, input)
		if next != nil && next.OnPreToolUse != nil {
			return next.OnPreToolUse(input, inv)
		}
		return nil, nil
	}

	h.OnPostToolUse = func(input copilotsdk.PostToolUseHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.PostToolUseHookOutput, error) {
		rec.RecordHook("post_tool_use", inv.SessionID, input)
		if next != nil && next.OnPostToolUse != nil {
			return next.OnPostToolUse(input, inv)
		}
		return nil, nil
	}

	h.OnPostToolUseFailure = func(input copilotsdk.PostToolUseFailureHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.PostToolUseFailureHookOutput, error) {
		rec.RecordHook("post_tool_use_failure", inv.SessionID, input)
		if next != nil && next.OnPostToolUseFailure != nil {
			return next.OnPostToolUseFailure(input, inv)
		}
		return nil, nil
	}

	h.OnUserPromptSubmitted = func(input copilotsdk.UserPromptSubmittedHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.UserPromptSubmittedHookOutput, error) {
		rec.RecordHook("user_prompt_submitted", inv.SessionID, input)
		if next != nil && next.OnUserPromptSubmitted != nil {
			return next.OnUserPromptSubmitted(input, inv)
		}
		return nil, nil
	}

	h.OnSessionStart = func(input copilotsdk.SessionStartHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.SessionStartHookOutput, error) {
		rec.RecordHook("session_start", inv.SessionID, input)
		if next != nil && next.OnSessionStart != nil {
			return next.OnSessionStart(input, inv)
		}
		return nil, nil
	}

	h.OnSessionEnd = func(input copilotsdk.SessionEndHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.SessionEndHookOutput, error) {
		rec.RecordHook("session_end", inv.SessionID, input)
		// Record the copilot.session.ended lifecycle event once (idempotent
		// with Manager.EndSession). Queued asynchronously — this hook runs
		// on the SDK's JSON-RPC loop and must never block on Actae.
		rec.queueSessionEnded(input.Reason, inv.SessionID)
		if next != nil && next.OnSessionEnd != nil {
			return next.OnSessionEnd(input, inv)
		}
		return nil, nil
	}

	h.OnErrorOccurred = func(input copilotsdk.ErrorOccurredHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.ErrorOccurredHookOutput, error) {
		rec.RecordHook("error_occurred", inv.SessionID, input)
		if next != nil && next.OnErrorOccurred != nil {
			return next.OnErrorOccurred(input, inv)
		}
		return nil, nil
	}

	h.OnPreMCPToolCall = func(input copilotsdk.PreMCPToolCallHookInput, inv copilotsdk.HookInvocation) (*copilotsdk.PreMCPToolCallHookOutput, error) {
		rec.RecordHook("pre_mcp_tool_call", inv.SessionID, input)
		if next != nil && next.OnPreMCPToolCall != nil {
			return next.OnPreMCPToolCall(input, inv)
		}
		return nil, nil
	}

	return h
}
