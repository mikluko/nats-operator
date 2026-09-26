package sysobs

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrReloadUnconfirmed is returned when a server acknowledges a reload but
// its VARZ config load time does not advance.
var ErrReloadUnconfirmed = errors.New("reload not confirmed by VARZ")

// ConfigState is the configuration a server has loaded, as its VARZ reports
// it.
type ConfigState struct {
	Digest   string
	LoadTime time.Time
}

// Config returns the ConfigState of the server with ID serverID.
func (o *Observer) Config(ctx context.Context, serverID string) (ConfigState, error) {
	var r wireVarzResponse
	if err := o.request(ctx, fmt.Sprintf(subjVarz, serverID), o.filter(), &r); err != nil {
		return ConfigState{}, err
	}
	if r.Error != nil {
		return ConfigState{}, fmt.Errorf("%w: VARZ from %s: %d %s", ErrServer, serverID, r.Error.Code, r.Error.Description)
	}
	if r.Data == nil {
		return ConfigState{}, fmt.Errorf("%w: VARZ from %s: empty response", ErrServer, serverID)
	}
	return ConfigState{Digest: r.Data.ConfigDigest, LoadTime: r.Data.ConfigLoadTime}, nil
}

// Reload asks the server with ID serverID to reload its configuration file
// and returns the ConfigState it loaded. A reload the server rejects, such
// as one changing a restart-only field, returns an error wrapping
// ErrServer and leaves the server on its previous configuration; one the
// server acknowledges without advancing its config load time returns
// ErrReloadUnconfirmed.
func (o *Observer) Reload(ctx context.Context, serverID string) (ConfigState, error) {
	before, err := o.Config(ctx, serverID)
	if err != nil {
		return ConfigState{}, err
	}
	var r wireReloadResponse
	if err := o.request(ctx, fmt.Sprintf(subjReload, serverID), o.filter(), &r); err != nil {
		return ConfigState{}, err
	}
	if r.Error != nil {
		return ConfigState{}, fmt.Errorf("%w: RELOAD on %s: %d %s", ErrServer, serverID, r.Error.Code, r.Error.Description)
	}
	after, err := o.Config(ctx, serverID)
	if err != nil {
		return ConfigState{}, err
	}
	if !after.LoadTime.After(before.LoadTime) {
		return after, fmt.Errorf("%w: %s load time stayed %s", ErrReloadUnconfirmed, serverID, after.LoadTime)
	}
	return after, nil
}
