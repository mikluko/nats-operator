package sysobs

import (
	"context"
	"errors"
	"fmt"
)

const (
	subjServerEvacuate = "$JS.API.SERVER.EVACUATE"
	subjServerRemove   = "$JS.API.SERVER.REMOVE"
	subjMetaStepDown   = "$JS.API.META.LEADER.STEPDOWN"

	errCodeNotMember      = 10044
	errCodeChangeInflight = 10202
)

// ErrNotMember is returned when the meta leader does not count the named
// server among the meta group's peers.
var ErrNotMember = errors.New("server is not a member of the meta group")

// ErrChangeInflight is returned when the meta group is already changing its
// membership.
var ErrChangeInflight = errors.New("a meta group membership change is in progress")

// Evacuate asks the meta leader to move every stream and consumer replica
// off the server named server. It returns once the moves are proposed, not
// once they complete.
func (o *SystemClient) Evacuate(ctx context.Context, server string) error {
	return o.jsAdmin(ctx, subjServerEvacuate, wirePeerRequest{Server: server})
}

// RemovePeer asks the meta leader to remove the server named server from
// the meta group, and returns once the removal is committed.
func (o *SystemClient) RemovePeer(ctx context.Context, server string) error {
	return o.jsAdmin(ctx, subjServerRemove, wirePeerRequest{Server: server})
}

// StepDownMeta asks the meta leader to hand its leadership to another peer.
func (o *SystemClient) StepDownMeta(ctx context.Context) error {
	return o.jsAdmin(ctx, subjMetaStepDown, nil)
}

// jsAdmin sends a meta leader request and maps its error codes onto
// ErrNotMember, ErrChangeInflight or ErrServer.
func (o *SystemClient) jsAdmin(ctx context.Context, subject string, req any) error {
	var r wireJSAPIResponse
	if err := o.request(ctx, subject, req, &r); err != nil {
		return err
	}
	if r.Error == nil {
		return nil
	}
	var base error
	switch r.Error.ErrCode {
	case errCodeNotMember:
		base = ErrNotMember
	case errCodeChangeInflight:
		base = ErrChangeInflight
	default:
		base = ErrServer
	}
	return fmt.Errorf("%w: %s: %d %s", base, subject, r.Error.ErrCode, r.Error.Description)
}
