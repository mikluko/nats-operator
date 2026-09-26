package jwtplane

import (
	"errors"
	"fmt"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// UserPreset names a fixed set of user claims that replaces Permissions.
type UserPreset string

// The user presets. The three controller presets are for system account users
// only and readonly for ordinary accounts only; leafnode is for either.
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

// User is a user of an account.
type User struct {
	Name      string
	PublicKey string
	// SystemAccount is whether the user belongs to the system account.
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

var stepdownImportSubjects = []string{
	stepdownPrefix + "*." + streamStepdownSubject,
	stepdownPrefix + "*." + consumerStepdownSubject,
}

// userPresets are the claims each preset grants.
var userPresets = map[UserPreset]userPreset{
	PresetClusterController: {
		system: true,
		pub: append([]string{
			"$SYS.REQ.SERVER.PING.STATSZ",
			"$SYS.REQ.SERVER.PING.JSZ",
			"$SYS.REQ.SERVER.PING.GATEWAYZ",
			"$SYS.REQ.SERVER.PING.LEAFZ",
			"$SYS.REQ.SERVER.*.STATSZ",
			"$SYS.REQ.SERVER.*.JSZ",
			"$SYS.REQ.SERVER.*.VARZ",
			"$SYS.REQ.SERVER.*.HEALTHZ",
			"$SYS.REQ.SERVER.*.RELOAD",
			"$JS.API.SERVER.EVACUATE",
			"$JS.API.SERVER.REMOVE",
			"$JS.API.META.LEADER.STEPDOWN",
		}, stepdownImportSubjects...),
		sub: []string{"_INBOX.>"},
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
		sub: []string{"_INBOX.>"},
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
		sub: []string{"_INBOX.>", "$SYS.SERVER.*.STATSZ"},
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
