package jwtplane

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
)

// DefaultAccountTTL is the lifetime of an account JWT whose TTL is unset.
const DefaultAccountTTL = 48 * time.Hour

var (
	// ErrNegativeLimit is returned for a limit below zero.
	ErrNegativeLimit = errors.New("limit is negative")

	// ErrActivationRequired is returned for an import of a private export
	// that carries no activation token.
	ErrActivationRequired = errors.New("import of a private export needs an activation token")

	// ErrNotImporter is returned when an activation token is asked for an
	// account the export does not list.
	ErrNotImporter = errors.New("account is not among the export's importers")

	// ErrNotPrivate is returned when an activation token is asked for a
	// public export.
	ErrNotPrivate = errors.New("export is public")
)

// Account is the spec SignAccount signs.
type Account struct {
	Name string
	Keys Keys
	// TTL is the JWT's lifetime; zero is DefaultAccountTTL.
	TTL time.Duration
	// NoExpiry signs a JWT that never expires, whatever TTL says.
	NoExpiry    bool
	Limits      Limits
	Exports     []Export
	Imports     []Import
	Revocations []Revocation
}

// Limits are an account's limits. Zero is unlimited throughout.
type Limits struct {
	Connections   int64
	Subscriptions int64
	Payload       int64
	// JetStream are the limits for the account as a whole; nil with no
	// JetStreamTiers disables JetStream for the account.
	JetStream *JetStreamLimits
	// JetStreamTiers are the limits by tier name. SignAccount refuses them
	// beside JetStream.
	JetStreamTiers map[string]JetStreamLimits
}

// JetStreamLimits are an account's JetStream limits. Zero is unlimited.
type JetStreamLimits struct {
	MemoryStorage        int64
	DiskStorage          int64
	Streams              int64
	Consumers            int64
	MaxAckPending        int64
	MemoryMaxStreamBytes int64
	DiskMaxStreamBytes   int64
	MaxBytesRequired     bool
}

// Export is a stream or service an account offers to others.
type Export struct {
	Name    string
	Type    jwt.ExportType
	Subject string
	// ResponseType applies to services; empty is Singleton.
	ResponseType jwt.ResponseType
	// Private exports need an activation token per importer.
	Private bool
	// Importers are the public keys of the accounts a private export
	// admits.
	Importers []string
}

// Import takes another account's export.
type Import struct {
	// Account is the exporting account's public key.
	Account string
	// Export is the export as the exporting account declares it; the
	// import's subject and type come from it.
	Export Export
	// LocalSubject is where the import appears; empty is the export's
	// subject.
	LocalSubject string
	// Token is the activation token SignActivation minted, or one the
	// exporter issued elsewhere; required for a private export, ignored for
	// a public one.
	Token string
	// Share lets the exporter sample the importer's request latency; valid
	// on a service import only.
	Share bool
	// AllowTrace lets message traces cross; valid on a stream import only.
	AllowTrace bool
}

// Revocation revokes a user's JWTs issued at or before At.
type Revocation struct {
	// PublicKey is the user's public key, or jwt.All for every user of the
	// account.
	PublicKey string
	At        time.Time
}

// SignAccount returns the account JWT, signed by the NATS operator's active
// signing key and expiring TTL after now unless NoExpiry is set.
func SignAccount(a Account, operator Keys, now time.Time) (string, error) {
	c, err := accountClaims(a.Name, a.Keys, a.Revocations)
	if err != nil {
		return "", err
	}
	if err := applyLimits(&c.Limits, a.Limits); err != nil {
		return "", err
	}
	for _, e := range a.Exports {
		c.Exports.Add(jwtExport(e))
	}
	for _, i := range a.Imports {
		ji, err := jwtImport(i)
		if err != nil {
			return "", err
		}
		c.Imports.Add(ji)
	}
	ttl := a.TTL
	if ttl == 0 {
		ttl = DefaultAccountTTL
	}
	if ttl < 0 {
		return "", fmt.Errorf("account TTL %s is negative", ttl)
	}
	if !a.NoExpiry {
		c.Expires = now.Add(ttl).Unix()
	}
	return signAccountClaims(c, operator)
}

// SystemAccount is the spec SignSystemAccount signs.
type SystemAccount struct {
	Name string
	Keys Keys
	// StepdownAccounts are the public keys of accounts carrying the
	// jetstream-stepdown export preset; each gets StepdownImports.
	StepdownAccounts []string
	Revocations      []Revocation
}

// SignSystemAccount returns the system account JWT, signed by the NATS
// operator's active signing key. It never expires, has JetStream disabled
// and carries MonitoringExports.
func SignSystemAccount(s SystemAccount, operator Keys) (string, error) {
	c, err := accountClaims(s.Name, s.Keys, s.Revocations)
	if err != nil {
		return "", err
	}
	c.Exports = MonitoringExports()
	for _, acc := range s.StepdownAccounts {
		for _, i := range StepdownImports(acc) {
			ji, err := jwtImport(i)
			if err != nil {
				return "", err
			}
			c.Imports.Add(ji)
		}
	}
	return signAccountClaims(c, operator)
}

// MonitoringExports returns the two exports nsc gives a system account,
// through which an importing account reaches the monitoring requests and
// events of its own key and of no other.
func MonitoringExports() jwt.Exports {
	return jwt.Exports{
		{Name: "account-monitoring-services", Subject: "$SYS.REQ.ACCOUNT.*.*", Type: jwt.Service, ResponseType: jwt.ResponseTypeStream, AccountTokenPosition: 4},
		{Name: "account-monitoring-streams", Subject: "$SYS.ACCOUNT.*.>", Type: jwt.Stream, AccountTokenPosition: 3},
	}
}

func accountClaims(name string, keys Keys, revs []Revocation) (*jwt.AccountClaims, error) {
	pub, err := keys.publicKey(nkeys.PrefixByteAccount)
	if err != nil {
		return nil, err
	}
	signing, err := keys.signingPublicKeys(nkeys.PrefixByteAccount)
	if err != nil {
		return nil, err
	}
	c := jwt.NewAccountClaims(pub)
	c.Name = name
	c.SigningKeys.Add(signing...)
	for _, r := range revs {
		if r.PublicKey != jwt.All && !nkeys.IsValidPublicUserKey(r.PublicKey) {
			return nil, fmt.Errorf("%w: revoked %q is neither a user public key nor %q", ErrWrongKeyType, r.PublicKey, jwt.All)
		}
		c.RevokeAt(r.PublicKey, r.At)
	}
	return c, nil
}

func signAccountClaims(c *jwt.AccountClaims, operator Keys) (string, error) {
	signer, err := operator.signer(nkeys.PrefixByteOperator)
	if err != nil {
		return "", err
	}
	if err := validate(c); err != nil {
		return "", err
	}
	return c.Encode(signer)
}

func applyLimits(dst *jwt.OperatorLimits, l Limits) error {
	set := func(dst *int64, v int64, name string) error {
		switch {
		case v < 0:
			return fmt.Errorf("%w: %s %d", ErrNegativeLimit, name, v)
		case v == 0:
			*dst = jwt.NoLimit
		default:
			*dst = v
		}
		return nil
	}
	errs := []error{
		set(&dst.Conn, l.Connections, "connections"),
		set(&dst.Subs, l.Subscriptions, "subscriptions"),
		set(&dst.Payload, l.Payload, "payload"),
	}
	if l.JetStream != nil {
		js, err := jetStreamLimits(*l.JetStream, "jetstream")
		dst.JetStreamLimits = js
		errs = append(errs, err)
	}
	for tier, tl := range l.JetStreamTiers {
		js, err := jetStreamLimits(tl, "jetstream tier "+tier)
		if dst.JetStreamTieredLimits == nil {
			dst.JetStreamTieredLimits = jwt.JetStreamTieredLimits{}
		}
		dst.JetStreamTieredLimits[tier] = js
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// jetStreamLimits are l as claims. An unset storage, stream or consumer limit
// is written as jwt.NoLimit, since nats-server reads a zero storage limit as
// no storage; the others stay zero, which it reads as unlimited.
func jetStreamLimits(l JetStreamLimits, name string) (jwt.JetStreamLimits, error) {
	unlimited := func(v int64) int64 {
		if v == 0 {
			return jwt.NoLimit
		}
		return v
	}
	out := jwt.JetStreamLimits{
		MemoryStorage:        unlimited(l.MemoryStorage),
		DiskStorage:          unlimited(l.DiskStorage),
		Streams:              unlimited(l.Streams),
		Consumer:             unlimited(l.Consumers),
		MaxAckPending:        l.MaxAckPending,
		MemoryMaxStreamBytes: l.MemoryMaxStreamBytes,
		DiskMaxStreamBytes:   l.DiskMaxStreamBytes,
		MaxBytesRequired:     l.MaxBytesRequired,
	}
	var errs []error
	for _, f := range []struct {
		field string
		v     int64
	}{
		{"memory storage", l.MemoryStorage},
		{"disk storage", l.DiskStorage},
		{"streams", l.Streams},
		{"consumers", l.Consumers},
		{"max ack pending", l.MaxAckPending},
		{"memory max stream bytes", l.MemoryMaxStreamBytes},
		{"disk max stream bytes", l.DiskMaxStreamBytes},
	} {
		if f.v < 0 {
			errs = append(errs, fmt.Errorf("%w: %s %s %d", ErrNegativeLimit, name, f.field, f.v))
		}
	}
	return out, errors.Join(errs...)
}

func jwtExport(e Export) *jwt.Export {
	je := &jwt.Export{
		Name:     e.Name,
		Subject:  jwt.Subject(e.Subject),
		Type:     e.Type,
		TokenReq: e.Private,
	}
	if e.Type == jwt.Service {
		je.ResponseType = e.ResponseType
		if je.ResponseType == "" {
			je.ResponseType = jwt.ResponseTypeSingleton
		}
	}
	return je
}

func jwtImport(i Import) (*jwt.Import, error) {
	if !nkeys.IsValidPublicAccountKey(i.Account) {
		return nil, fmt.Errorf("%w: import from %q, not an account public key", ErrWrongKeyType, i.Account)
	}
	ji := &jwt.Import{
		Name:       i.Export.Name,
		Account:    i.Account,
		Subject:    jwt.Subject(i.Export.Subject),
		Type:       i.Export.Type,
		Share:      i.Share,
		AllowTrace: i.AllowTrace,
	}
	if i.LocalSubject != "" && i.LocalSubject != i.Export.Subject {
		ji.LocalSubject = jwt.RenamingSubject(i.LocalSubject)
	}
	if i.Export.Private {
		if i.Token == "" {
			return nil, fmt.Errorf("%w: %q from %s", ErrActivationRequired, i.Export.Name, i.Account)
		}
		ji.Token = i.Token
	}
	return ji, nil
}

// ValidateImport returns the reasons jwt would refuse i in the JWT of the
// account importer, its activation token checked against both accounts, the
// subject and the type; nil where there are none.
func ValidateImport(i Import, importer string) error {
	ji, err := jwtImport(i)
	if err != nil {
		return err
	}
	vr := jwt.CreateValidationResults()
	ji.Validate(importer, vr)
	if vr.IsBlocking(true) {
		return errors.Join(vr.Errors()...)
	}
	return nil
}

// MonitoringImport returns the import by importer of the system account's
// monitoring export named name, which nats-server serves only with the
// importer's public key at the position the export fixes. ok is false
// where MonitoringExports has no export so named.
func MonitoringImport(name, systemAccount, importer string) (Import, bool) {
	for _, e := range MonitoringExports() {
		if e.Name != name {
			continue
		}
		tokens := strings.Split(string(e.Subject), ".")
		tokens[e.AccountTokenPosition-1] = importer
		return Import{
			Account: systemAccount,
			Export:  Export{Name: e.Name, Type: e.Type, Subject: strings.Join(tokens, "."), ResponseType: e.ResponseType},
		}, true
	}
	return Import{}, false
}

// SignActivation returns the activation token admitting importer to the
// private export e of the exporting account, signed by the exporter's active
// signing key. It never expires.
func SignActivation(exporter Keys, e Export, importer string) (string, error) {
	if !e.Private {
		return "", fmt.Errorf("%w: %q", ErrNotPrivate, e.Name)
	}
	if !nkeys.IsValidPublicAccountKey(importer) {
		return "", fmt.Errorf("%w: importer %q is not an account public key", ErrWrongKeyType, importer)
	}
	if !slices.Contains(e.Importers, importer) {
		return "", fmt.Errorf("%w: %s for %q", ErrNotImporter, importer, e.Name)
	}
	pub, err := exporter.publicKey(nkeys.PrefixByteAccount)
	if err != nil {
		return "", err
	}
	signer, err := exporter.signer(nkeys.PrefixByteAccount)
	if err != nil {
		return "", err
	}
	c := jwt.NewActivationClaims(importer)
	c.Name = e.Name
	c.ImportSubject = jwt.Subject(e.Subject)
	c.ImportType = e.Type
	c.IssuerAccount = pub
	if err := validate(c); err != nil {
		return "", err
	}
	return c.Encode(signer)
}

// RenewAt returns when an account JWT is due to be re-signed: halfway
// between its issue and its expiry. A JWT that never expires is never due,
// and the zero time is returned.
func RenewAt(accountJWT string) (time.Time, error) {
	c, err := jwt.DecodeAccountClaims(accountJWT)
	if err != nil {
		return time.Time{}, err
	}
	if c.Expires == 0 {
		return time.Time{}, nil
	}
	return time.Unix(c.IssuedAt+(c.Expires-c.IssuedAt)/2, 0), nil
}
