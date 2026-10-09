package actae

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// GroupSession is the high-level coordination surface for independently
// executing agents. It never starts or schedules an agent runtime.
type GroupSession struct {
	Client  *Client
	GroupID string
}
type GroupMemberSession struct {
	Group    *GroupSession
	MemberID string
	lease    *MemberLease
}

func (c *Client) ExecutionGroup(groupID string) (*GroupSession, error) {
	if err := validateChannelID(groupID); err != nil {
		return nil, err
	}
	return &GroupSession{Client: c, GroupID: groupID}, nil
}

// Group is the concise alias for ExecutionGroup.
func (c *Client) Group(groupID string) (*GroupSession, error) { return c.ExecutionGroup(groupID) }
func (g *GroupSession) Member(memberID string) (*GroupMemberSession, error) {
	if err := validateChannelID(memberID); err != nil {
		return nil, err
	}
	return &GroupMemberSession{Group: g, MemberID: memberID}, nil
}
func (m *GroupMemberSession) Claim(ctx context.Context, ownerID string, leaseSeconds int) (MemberLease, error) {
	lease, err := m.Group.Client.ClaimMember(ctx, m.Group.GroupID, m.MemberID, ownerID, leaseSeconds)
	if err == nil {
		m.lease = &lease
	}
	return lease, err
}
func (m *GroupMemberSession) Heartbeat(ctx context.Context, leaseSeconds int) (MemberLease, error) {
	if m.lease == nil {
		return MemberLease{}, errors.New("claim must succeed before heartbeat")
	}
	lease, err := m.Group.Client.HeartbeatMember(ctx, m.Group.GroupID, m.MemberID, m.lease.OwnerID, m.lease.Generation, leaseSeconds)
	if err == nil {
		m.lease = &lease
	}
	return lease, err
}
func (m *GroupMemberSession) Release(ctx context.Context) error {
	if m.lease == nil {
		return nil
	}
	lease := m.lease
	m.lease = nil
	return m.Group.Client.ReleaseMember(ctx, m.Group.GroupID, m.MemberID, lease.OwnerID, lease.Generation)
}
func (m *GroupMemberSession) Send(ctx context.Context, to, messageType string, payload any, causalContext map[string]any, operationID *string) (GroupMessage, error) {
	return m.Group.Client.SendGroupMessage(ctx, m.Group.GroupID, m.MemberID, to, messageType, payload, causalContext, operationID)
}
func (m *GroupMemberSession) Messages(ctx context.Context, after *string, limit int) ([]GroupMessage, error) {
	return m.Group.Client.GroupMessages(ctx, m.Group.GroupID, m.MemberID, after, limit)
}
func (m *GroupMemberSession) channelID(ctx context.Context) (string, error) {
	members, err := m.Group.Client.ExecutionGroupMembers(ctx, m.Group.GroupID)
	if err != nil {
		return "", err
	}
	for _, member := range members {
		if member.MemberID == m.MemberID {
			return member.ChannelID, nil
		}
	}
	return "", fmt.Errorf("unknown execution-group member %q", m.MemberID)
}

// Subscribe subscribes the member's delivery channel on the live WebSocket.
func (m *GroupMemberSession) Subscribe(ctx context.Context, cursor *int64) error {
	channel, err := m.channelID(ctx)
	if err != nil {
		return err
	}
	return m.Group.Client.SubscribeAndWait(ctx, channel, cursor)
}

// Unsubscribe removes the member's delivery channel from the live WebSocket.
func (m *GroupMemberSession) Unsubscribe(ctx context.Context) error {
	channel, err := m.channelID(ctx)
	if err != nil {
		return err
	}
	return m.Group.Client.Unsubscribe(ctx, channel)
}

// Stream returns replayed then live group deliveries from the member channel.
// The caller should persist the Event cursor and pass it back after reconnect.
func (m *GroupMemberSession) Stream(ctx context.Context, cursor *int64) (<-chan GroupMessage, error) {
	channel, err := m.channelID(ctx)
	if err != nil {
		return nil, err
	}
	events, err := m.Group.Client.Stream(ctx, channel, cursor)
	if err != nil {
		return nil, err
	}
	out := make(chan GroupMessage, 256)
	go func() {
		defer close(out)
		for event := range events {
			if event.EventType != "message.received" {
				continue
			}
			message, ok := groupMessageFromEvent(event)
			if !ok || message.GroupID != m.Group.GroupID || message.ToMemberID != m.MemberID {
				continue
			}
			select {
			case out <- message:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}
func (m *GroupMemberSession) Acknowledge(ctx context.Context, message GroupMessage) (GroupMessage, error) {
	if m.lease == nil {
		return GroupMessage{}, errors.New("claim must succeed before acknowledge")
	}
	return m.Group.Client.AcknowledgeGroupMessage(ctx, message.MessageID, m.lease.OwnerID, m.lease.Generation)
}
func (m *GroupMemberSession) WaitFor(ctx context.Context, messageType string, after *string, pollInterval time.Duration) (GroupMessage, error) {
	if pollInterval <= 0 {
		pollInterval = 200 * time.Millisecond
	}
	cursor := after
	for {
		messages, err := m.Messages(ctx, cursor, 100)
		if err != nil {
			return GroupMessage{}, err
		}
		for _, message := range messages {
			value := message.MessageID
			cursor = &value
			if message.MessageType == messageType {
				return message, nil
			}
		}
		timer := time.NewTimer(pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return GroupMessage{}, ctx.Err()
		case <-timer.C:
		}
	}
}

type GroupWait struct {
	MemberID, MessageType string
	After                 *string
}
type GroupWaitResult struct {
	MemberID string
	Message  GroupMessage
}

func (g *GroupSession) WaitFor(ctx context.Context, memberID, messageType string, after *string, pollInterval time.Duration) (GroupMessage, error) {
	member, err := g.Member(memberID)
	if err != nil {
		return GroupMessage{}, err
	}
	return member.WaitFor(ctx, messageType, after, pollInterval)
}
func (g *GroupSession) WaitAny(ctx context.Context, waits []GroupWait, pollInterval time.Duration) (GroupWaitResult, error) {
	if len(waits) == 0 {
		return GroupWaitResult{}, errors.New("WaitAny requires at least one waiter")
	}
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	type outcome struct {
		result GroupWaitResult
		err    error
	}
	results := make(chan outcome, len(waits))
	for _, wait := range waits {
		wait := wait
		go func() {
			message, err := g.WaitFor(child, wait.MemberID, wait.MessageType, wait.After, pollInterval)
			results <- outcome{GroupWaitResult{wait.MemberID, message}, err}
		}()
	}
	for range waits {
		value := <-results
		if value.err == nil {
			return value.result, nil
		}
		if ctx.Err() != nil {
			return GroupWaitResult{}, ctx.Err()
		}
	}
	return GroupWaitResult{}, fmt.Errorf("all group waiters failed")
}
func (g *GroupSession) WaitAll(ctx context.Context, waits []GroupWait, pollInterval time.Duration) ([]GroupWaitResult, error) {
	results := make([]GroupWaitResult, len(waits))
	errorsChannel := make(chan error, len(waits))
	for index, wait := range waits {
		index, wait := index, wait
		go func() {
			message, err := g.WaitFor(ctx, wait.MemberID, wait.MessageType, wait.After, pollInterval)
			if err == nil {
				results[index] = GroupWaitResult{wait.MemberID, message}
			}
			errorsChannel <- err
		}()
	}
	for range waits {
		if err := <-errorsChannel; err != nil {
			return nil, err
		}
	}
	return results, nil
}
func (g *GroupSession) Promote(ctx context.Context, memberID string) (map[string]any, error) {
	return g.Client.PromoteExecutionGroupForkMember(ctx, g.GroupID, memberID)
}

func groupMessageFromEvent(event Event) (GroupMessage, bool) {
	envelope, ok := event.Payload.(map[string]any)
	if !ok {
		return GroupMessage{}, false
	}
	from, _ := envelope["from"].(map[string]any)
	to, _ := envelope["to"].(map[string]any)
	messageID, _ := envelope["message_id"].(string)
	groupID, _ := envelope["group_id"].(string)
	messageType, _ := envelope["type"].(string)
	fromMember, _ := from["member"].(string)
	toMember, _ := to["member"].(string)
	if messageID == "" || groupID == "" {
		return GroupMessage{}, false
	}
	var causal map[string]any
	if value, ok := envelope["causal_context"].(map[string]any); ok {
		causal = value
	}
	return GroupMessage{MessageID: messageID, GroupID: groupID, FromMemberID: fromMember, ToMemberID: toMember, MessageType: messageType, Payload: envelope["payload"], CausalContext: causal, DeliveryEventID: event.ID, Status: "delivered"}, true
}
