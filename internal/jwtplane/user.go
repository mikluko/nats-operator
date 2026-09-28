package jwtplane

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// UserPreset names a fixed set of user claims that replaces Permissions.
type UserPreset string

// The user presets.
const (
	PresetClusterController   UserPreset = "cluster-controller"
	PresetJetStreamController UserPreset = "jetstream-controller"
	PresetAuthController      UserPreset = "auth-controller"
	PresetReadonly            UserPreset = "readonly"
	PresetLeafnode            UserPreset = "leafnode"
)

var (
	// ErrPresetAndPermissions is returned when a user sets both.
	ErrPresetAndPermissions = errors.New("preset and permissions are exclusive")

	// ErrPresetAccount is returned when a preset is used in an account of
	// the wrong kind.
	ErrPresetAccount = errors.New("preset not allowed in this account")

	// ErrConnectionType is returned for an unknown connection type, or for
	// connection types set beside a preset.
	ErrConnectionType = errors.New("invalid allowed connection types")
)

// User is the spec SignUser signs.
type User struct {
	Name          string
	PublicKey     string
	SystemAccount bool
	// Preset and Permissions are exclusive; with neither, the user may
	// publish and subscribe to anything.
	Preset      UserPreset
	Permissions *Permissions
	// AllowedConnectionTypes restricts how the user may connect, from the
	// jwt.ConnectionType* values; empty allows any. Exclusive with Preset.
	AllowedConnectionTypes []string
}

// Permissions are a user's publish and subscribe permissions.
type Permissions struct {
	Publish   SubjectPermissions
	Subscribe SubjectPermissions
}

// SubjectPermissions allow and deny subjects.
type SubjectPermissions struct {
	Allow []string
	Deny  []string
}

// SignUser returns the user JWT, signed by the account's active signing key
// on behalf of the account's identity. It never expires.
func SignUser(u User, account Keys) (string, error) {
	if !nkeys.IsValidPublicUserKey(u.PublicKey) {
		return "", fmt.Errorf("%w: user %q is not a user public key", ErrWrongKeyType, u.PublicKey)
	}
	acc, err := account.publicKey(nkeys.PrefixByteAccount)
	if err != nil {
		return "", err
	}
	signer, err := account.signer(nkeys.PrefixByteAccount)
	if err != nil {
		return "", err
	}
	c := jwt.NewUserClaims(u.PublicKey)
	c.Name = u.Name
	c.IssuerAccount = acc
	if err := applyUserClaims(&c.UserPermissionLimits, u); err != nil {
		return "", err
	}
	if err := validate(c); err != nil {
		return "", err
	}
	return c.Encode(signer)
}

func applyUserClaims(dst *jwt.UserPermissionLimits, u User) error {
	if u.Preset == "" {
		if err := checkConnectionTypes(u.AllowedConnectionTypes); err != nil {
			return err
		}
		dst.AllowedConnectionTypes.Add(u.AllowedConnectionTypes...)
		if p := u.Permissions; p != nil {
			dst.Pub.Allow.Add(p.Publish.Allow...)
			dst.Pub.Deny.Add(p.Publish.Deny...)
			dst.Sub.Allow.Add(p.Subscribe.Allow...)
			dst.Sub.Deny.Add(p.Subscribe.Deny...)
		}
		return nil
	}
	if u.Permissions != nil {
		return ErrPresetAndPermissions
	}
	if len(u.AllowedConnectionTypes) > 0 {
		return fmt.Errorf("%w: set beside preset %q", ErrConnectionType, u.Preset)
	}
	p, ok := userPresets[u.Preset]
	if !ok {
		return fmt.Errorf("unknown user preset %q", u.Preset)
	}
	if p.system != u.SystemAccount && !p.anyAccount {
		return fmt.Errorf("%w: %q in system account: %t", ErrPresetAccount, u.Preset, u.SystemAccount)
	}
	dst.Pub.Allow.Add(p.pub...)
	dst.Sub.Allow.Add(p.sub...)
	dst.AllowedConnectionTypes.Add(p.connectionTypes...)
	return nil
}

func checkConnectionTypes(types []string) error {
	for _, t := range types {
		switch t {
		case jwt.ConnectionTypeStandard, jwt.ConnectionTypeWebsocket,
			jwt.ConnectionTypeLeafnode, jwt.ConnectionTypeLeafnodeWS,
			jwt.ConnectionTypeMqtt, jwt.ConnectionTypeMqttWS,
			jwt.ConnectionTypeInProcess:
		default:
			return fmt.Errorf("%w: %q", ErrConnectionType, t)
		}
	}
	return nil
}

type userPreset struct {
	system          bool
	anyAccount      bool
	pub, sub        []string
	connectionTypes []string
}

// UserPresets lists every user preset, sorted.
func UserPresets() []UserPreset {
	return slices.Sorted(maps.Keys(userPresets))
}

// PresetGrant is what a user preset puts in a user's claims.
type PresetGrant struct {
	// SystemAccount and AnyAccount say which users may hold the preset:
	// system account users, ordinary account users, or, with AnyAccount,
	// either.
	SystemAccount, AnyAccount bool
	Publish, Subscribe        []string
	ConnectionTypes           []string
}

// UserPresetGrant returns what preset grants, and false for an unknown
// preset. The slices are the caller's own.
func UserPresetGrant(preset UserPreset) (PresetGrant, bool) {
	p, ok := userPresets[preset]
	if !ok {
		return PresetGrant{}, false
	}
	return PresetGrant{
		SystemAccount:   p.system,
		AnyAccount:      p.anyAccount,
		Publish:         slices.Clone(p.pub),
		Subscribe:       slices.Clone(p.sub),
		ConnectionTypes: slices.Clone(p.connectionTypes),
	}, true
}

// InboxPrefix is the prefix of the reply subjects a connection holding the
// controller preset dials with, for nats.CustomInboxPrefix; the preset
// grants subscribe under it and under no other inbox.
func InboxPrefix(preset UserPreset) string {
	return "_INBOX." + string(preset)
}

var stepdownImportSubjects = []string{
	stepdownPrefix + "*." + streamStepdownSubject,
	stepdownPrefix + "*." + consumerStepdownSubject,
}

var userPresets = map[UserPreset]userPreset{
	PresetClusterController: {
		system: true,
		pub: []string{
			"$SYS.REQ.SERVER.PING.STATSZ",
			"$SYS.REQ.SERVER.PING.JSZ",
			"$SYS.REQ.SERVER.PING.GATEWAYZ",
			"$SYS.REQ.SERVER.PING.LEAFZ",
			"$SYS.REQ.SERVER.*.VARZ",
			"$SYS.REQ.SERVER.*.RELOAD",
			"$JS.API.SERVER.EVACUATE",
			"$JS.API.SERVER.REMOVE",
			"$JS.API.META.LEADER.STEPDOWN",
		},
		sub: []string{InboxPrefix(PresetClusterController) + ".>"},
	},
	PresetJetStreamController: {
		system: true,
		pub: append([]string{
			"$SYS.REQ.SERVER.PING.STATSZ",
			"$SYS.REQ.SERVER.PING.JSZ",
			"$SYS.REQ.SERVER.*.JSZ",
			"$JS.API.ACCOUNT.STREAM.MOVE.*.*",
			"$JS.API.ACCOUNT.STREAM.CANCEL_MOVE.*.*",
		}, stepdownImportSubjects...),
		sub: []string{InboxPrefix(PresetJetStreamController) + ".>"},
	},
	PresetAuthController: {
		system: true,
		pub: []string{
			"$SYS.REQ.CLAIMS.UPDATE",
			"$SYS.REQ.CLAIMS.DELETE",
			"$SYS.REQ.ACCOUNT.*.CLAIMS.LOOKUP",
			"$SYS.REQ.SERVER.PING.STATSZ",
			"$SYS.REQ.SERVER.PING.CONNZ",
			"$SYS.REQ.SERVER.*.KICK",
		},
		sub: []string{InboxPrefix(PresetAuthController) + ".>"},
	},
	PresetReadonly: {
		pub: []string{
			"$JS.API.INFO",
			"$JS.API.STREAM.NAMES",
			"$JS.API.STREAM.LIST",
			"$JS.API.STREAM.INFO.*",
			"$JS.API.CONSUMER.NAMES.*",
			"$JS.API.CONSUMER.LIST.*",
			"$JS.API.CONSUMER.INFO.*.*",
		},
		sub: []string{">"},
	},
	PresetLeafnode: {
		anyAccount:      true,
		connectionTypes: []string{jwt.ConnectionTypeLeafnode},
	},
}
