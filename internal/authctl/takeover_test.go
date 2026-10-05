package authctl

import (
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nkeys"
	"github.com/stretchr/testify/require"

	"github.com/mikluko/nats-operator/internal/jwtplane"
)

// TestCompareTakeover pins what the first JWT signed from spec loses of the
// one the servers hold: each claim nsc can write that no spec expresses, and
// each expressible claim the spec leaves out, is named; one the spec sets to
// another value is not.
func TestCompareTakeover(t *testing.T) {
	op := testKeys(t, nkeys.PrefixByteOperator, false)
	acc := testKeys(t, nkeys.PrefixByteAccount, false, false)
	other := testKeys(t, nkeys.PrefixByteAccount, false)
	pub, signing, retiring := testPub(t, acc.Identity), testPub(t, acc.Signing[0].Pair), testPub(t, acc.Signing[1].Pair)
	otherPub := testPub(t, other.Identity)
	user, err := nkeys.CreateUser()
	require.NoError(t, err)
	userPub := testPub(t, user)

	held := func(t *testing.T, edit func(c *jwt.AccountClaims)) string {
		t.Helper()
		c := jwt.NewAccountClaims(pub)
		c.Name = "orders"
		c.SigningKeys.Add(signing)
		edit(c)
		token, err := c.Encode(op.Signing[0].Pair)
		require.NoError(t, err)
		return token
	}
	spec := func(t *testing.T, edit func(a *jwtplane.Account)) string {
		t.Helper()
		a := jwtplane.Account{Name: "orders", Keys: jwtplane.Keys{Identity: acc.Identity, Signing: acc.Signing[:1]}}
		edit(&a)
		token, err := jwtplane.SignAccount(a, op, time.Now())
		require.NoError(t, err)
		return token
	}
	none := func(*jwtplane.Account) {}
	scope := func(key string) *jwt.UserScope {
		s := jwt.NewUserScope()
		s.Key, s.Role = key, "reader"
		s.Template.Pub.Allow.Add("in.>")
		return s
	}
	export := func(edit func(e *jwt.Export)) func(c *jwt.AccountClaims) {
		return func(c *jwt.AccountClaims) {
			e := &jwt.Export{Name: "events", Subject: "orders.events.>", Type: jwt.Stream}
			edit(e)
			c.Exports.Add(e)
		}
	}
	exported := func(a *jwtplane.Account) {
		a.Exports = []jwtplane.Export{{Name: "events", Subject: "orders.events.>", Type: jwt.Stream}}
	}
	imported := func(edit func(i *jwt.Import)) func(c *jwt.AccountClaims) {
		return func(c *jwt.AccountClaims) {
			i := &jwt.Import{Name: "feed", Subject: "feed.>", Account: otherPub, Type: jwt.Stream}
			edit(i)
			c.Imports.Add(i)
		}
	}
	importing := func(a *jwtplane.Account) {
		a.Imports = []jwtplane.Import{{Account: otherPub, Export: jwtplane.Export{Name: "feed", Subject: "feed.>", Type: jwt.Stream}}}
	}

	for _, tc := range []struct {
		name  string
		held  func(c *jwt.AccountClaims)
		spec  func(a *jwtplane.Account)
		drops []string
		tiers []string
	}{
		{name: "as nsc makes it, with nothing set", held: func(*jwt.AccountClaims) {}, spec: none},
		{name: "a name is a change", held: func(c *jwt.AccountClaims) { c.Name = "ORDERS" }, spec: none},
		{name: "an expiry added is nothing", held: func(*jwt.AccountClaims) {}, spec: func(a *jwtplane.Account) { a.TTL = time.Hour }},
		{name: "an expiry dropped", held: func(c *jwt.AccountClaims) { c.Expires = time.Now().Add(time.Hour).Unix() }, spec: func(a *jwtplane.Account) { a.NoExpiry = true }, drops: []string{"exp"}},
		{name: "not before", held: func(c *jwt.AccountClaims) { c.NotBefore = time.Now().Unix() }, spec: none, drops: []string{"nbf"}},
		{name: "audience", held: func(c *jwt.AccountClaims) { c.Audience = "x" }, spec: none, drops: []string{"aud"}},
		{name: "a NATS limit the spec omits", held: func(c *jwt.AccountClaims) { c.Limits.Subs, c.Limits.Conn = 10, 20 }, spec: none, drops: []string{"limits.conn", "limits.subs"}},
		{name: "a NATS limit the spec changes", held: func(c *jwt.AccountClaims) { c.Limits.Subs = 10 }, spec: func(a *jwtplane.Account) { a.Limits.Subscriptions = 11 }},
		{name: "a NATS limit the spec adds", held: func(*jwt.AccountClaims) {}, spec: func(a *jwtplane.Account) { a.Limits.Subscriptions = 11 }},
		{name: "the account limits no spec expresses", held: func(c *jwt.AccountClaims) {
			c.Limits.Data, c.Limits.Imports, c.Limits.Exports, c.Limits.LeafNodeConn = 1, 2, 3, 4
			c.Limits.WildcardExports, c.Limits.DisallowBearer = false, true
		}, spec: none, drops: []string{"limits.data", "limits.disallow_bearer", "limits.exports", "limits.imports", "limits.leaf", "limits.wildcards"}},
		{name: "JetStream the spec omits", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 1 << 30, DiskStorage: jwt.NoLimit, Streams: 10, Consumer: jwt.NoLimit}
		}, spec: none, drops: []string{"limits.consumer", "limits.disk_storage", "limits.mem_storage", "limits.streams"}},
		{name: "a JetStream bound the spec omits", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 1 << 30, DiskStorage: 10 << 30, Streams: 10, Consumer: 100, MaxAckPending: 5}
		}, spec: func(a *jwtplane.Account) { a.Limits.JetStream = &jwtplane.JetStreamLimits{DiskStorage: 10 << 30} },
			drops: []string{"limits.consumer", "limits.max_ack_pending", "limits.mem_storage", "limits.streams"}},
		{name: "a JetStream bound the spec changes", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: 1 << 30, DiskStorage: 10 << 30, Streams: 10, Consumer: 100, MaxAckPending: 5, MaxBytesRequired: true}
		}, spec: func(a *jwtplane.Account) {
			a.Limits.JetStream = &jwtplane.JetStreamLimits{MemoryStorage: 2 << 30, DiskStorage: 20 << 30, Streams: 20, Consumers: 200, MaxAckPending: 6, MaxBytesRequired: true}
		}},
		{name: "unlimited JetStream the spec keeps", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamLimits = jwt.JetStreamLimits{MemoryStorage: jwt.NoLimit, DiskStorage: jwt.NoLimit, Streams: jwt.NoLimit, Consumer: jwt.NoLimit}
		}, spec: func(a *jwtplane.Account) { a.Limits.JetStream = &jwtplane.JetStreamLimits{} }},
		{name: "a tier the spec omits", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamTieredLimits = jwt.JetStreamTieredLimits{"R1": {MemoryStorage: 1 << 30, DiskStorage: jwt.NoLimit}, "R3": {DiskStorage: 1 << 30}}
		}, spec: func(a *jwtplane.Account) {
			a.Limits.JetStreamTiers = map[string]jwtplane.JetStreamLimits{"R1": {MemoryStorage: 1 << 30}}
		}, drops: []string{"limits.tiered_limits.R3"}},
		{name: "a tier's bound the spec omits", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamTieredLimits = jwt.JetStreamTieredLimits{"R1": {MemoryStorage: 1 << 30, DiskStorage: 2 << 30}}
		}, spec: func(a *jwtplane.Account) {
			a.Limits.JetStreamTiers = map[string]jwtplane.JetStreamLimits{"R1": {MemoryStorage: 1 << 30}}
		}, drops: []string{"limits.tiered_limits.R1.disk_storage"}},
		{name: "tiers replaced by limits for the account", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamTieredLimits = jwt.JetStreamTieredLimits{"R1": {MemoryStorage: 1 << 30, DiskStorage: 2 << 30}}
		}, spec: func(a *jwtplane.Account) { a.Limits.JetStream = &jwtplane.JetStreamLimits{MemoryStorage: 1 << 30} },
			drops: []string{"limits.tiered_limits.R1"}},
		{name: "a tier no spec names", held: func(c *jwt.AccountClaims) {
			c.Limits.JetStreamTieredLimits = jwt.JetStreamTieredLimits{"R1": {MemoryStorage: 1 << 30}, "R7": {MemoryStorage: 1 << 30}}
		}, spec: func(a *jwtplane.Account) {
			a.Limits.JetStreamTiers = map[string]jwtplane.JetStreamLimits{"R1": {MemoryStorage: 1 << 30}}
		}, drops: []string{"limits.tiered_limits.R7"}, tiers: []string{"R7"}},
		{name: "a signing key the spec omits", held: func(c *jwt.AccountClaims) { c.SigningKeys.Add(retiring) }, spec: none,
			drops: []string{"signing_keys[" + retiring + "]"}},
		{name: "a signing key the spec adds", held: func(*jwt.AccountClaims) {}, spec: func(a *jwtplane.Account) { a.Keys.Signing = acc.Signing }},
		{name: "a scoped signing key listed plain", held: func(c *jwt.AccountClaims) { c.SigningKeys.AddScopedSigner(scope(signing)) }, spec: none,
			drops: []string{"signing_keys[" + signing + "].template"}},
		{name: "a scoped signing key given its scope", held: func(c *jwt.AccountClaims) {
			s := scope(signing)
			s.Template.Subs = 5
			c.SigningKeys.AddScopedSigner(s)
		}, spec: func(a *jwtplane.Account) {
			a.Keys.Signing = []jwtplane.SigningKey{{Name: "s", Pair: acc.Signing[0].Pair, Scope: &jwtplane.UserScope{
				Role: "writer", Permissions: &jwtplane.Permissions{Publish: jwtplane.SubjectPermissions{Allow: []string{"out.>"}}}, Subscriptions: 6,
			}}}
		}},
		{name: "a scope's claims no spec expresses", held: func(c *jwt.AccountClaims) {
			s := scope(signing)
			s.Description = "readers"
			s.Template.Resp = &jwt.ResponsePermission{MaxMsgs: 1}
			s.Template.Src.Set("10.0.0.0/8")
			s.Template.Times = []jwt.TimeRange{{Start: "08:00:00", End: "17:00:00"}}
			s.Template.Locale = "UTC"
			s.Template.BearerToken = true
			s.Template.Data = 1024
			c.SigningKeys.AddScopedSigner(s)
		}, spec: func(a *jwtplane.Account) {
			a.Keys.Signing = []jwtplane.SigningKey{{Name: "s", Pair: acc.Signing[0].Pair, Scope: &jwtplane.UserScope{Role: "reader"}}}
		}, drops: []string{
			"signing_keys[" + signing + "].description",
			"signing_keys[" + signing + "].template.bearer_token",
			"signing_keys[" + signing + "].template.data",
			"signing_keys[" + signing + "].template.pub",
			"signing_keys[" + signing + "].template.resp",
			"signing_keys[" + signing + "].template.src",
			"signing_keys[" + signing + "].template.times",
			"signing_keys[" + signing + "].template.times_location",
		}},
		{name: "a revocation is kept by the signing", held: func(c *jwt.AccountClaims) { c.Revoke(userPub) },
			spec: func(a *jwtplane.Account) { a.Revocations = []jwtplane.Revocation{{PublicKey: userPub, At: time.Now()}} }},
		{name: "a revocation the signing lacks", held: func(c *jwt.AccountClaims) { c.Revoke(userPub) }, spec: none, drops: []string{"revocations"}},
		{name: "an export the spec omits", held: export(func(*jwt.Export) {}), spec: none, drops: []string{"exports[0]"}},
		{name: "an export the spec declares", held: export(func(*jwt.Export) {}), spec: exported},
		{name: "an export renamed", held: export(func(e *jwt.Export) { e.Name = "orders" }), spec: exported},
		{name: "a service export's response type", held: func(c *jwt.AccountClaims) {
			c.Exports.Add(&jwt.Export{Name: "q", Subject: "q", Type: jwt.Service, ResponseType: jwt.ResponseTypeStream})
		}, spec: func(a *jwtplane.Account) { a.Exports = []jwtplane.Export{{Name: "q", Subject: "q", Type: jwt.Service}} }},
		{name: "an export's claims no spec expresses", held: export(func(e *jwt.Export) {
			e.Latency = &jwt.ServiceLatency{Sampling: 50, Results: "latency"}
			e.AccountTokenPosition = 2
			e.Advertise, e.AllowTrace = true, true
			e.ResponseThreshold = time.Second
			e.Description, e.InfoURL = "events", "https://example.test"
			e.Revocations = jwt.RevocationList{userPub: time.Now().Unix()}
		}), spec: exported, drops: []string{
			"exports[0].account_token_position", "exports[0].advertise", "exports[0].allow_trace", "exports[0].description",
			"exports[0].info_url", "exports[0].response_threshold", "exports[0].revocations", "exports[0].service_latency",
		}},
		{name: "a private export declared public", held: export(func(e *jwt.Export) { e.TokenReq = true }), spec: exported, drops: []string{"exports[0].token_req"}},
		{name: "an import the spec omits", held: imported(func(*jwt.Import) {}), spec: none, drops: []string{"imports[0]"}},
		{name: "an import the spec declares", held: imported(func(*jwt.Import) {}), spec: importing},
		{name: "an import's claims the spec omits", held: imported(func(i *jwt.Import) {
			i.LocalSubject, i.AllowTrace = "local.>", true
		}), spec: importing, drops: []string{"imports[0].allow_trace", "imports[0].local_subject"}},
		{name: "an import's claims the spec sets", held: imported(func(i *jwt.Import) { i.LocalSubject, i.AllowTrace = "local.>", true }),
			spec: func(a *jwtplane.Account) {
				importing(a)
				a.Imports[0].LocalSubject, a.Imports[0].AllowTrace = "elsewhere.>", true
			}},
		{name: "the claims no spec expresses", held: func(c *jwt.AccountClaims) {
			c.DefaultPermissions.Pub.Allow.Add("in.>")
			c.DefaultPermissions.Resp = &jwt.ResponsePermission{MaxMsgs: 1}
			c.Mappings = jwt.Mapping{"a": []jwt.WeightedMapping{{Subject: "b", Weight: 100}}}
			c.Authorization.AuthUsers.Add(userPub)
			c.Trace = &jwt.MsgTrace{Destination: "trace", Sampling: 10}
			c.ClusterTraffic = jwt.ClusterTrafficOwner
			c.Description, c.InfoURL = "orders", "https://example.test"
			c.Tags.Add("team:orders")
		}, spec: none, drops: []string{
			"authorization", "cluster_traffic", "default_permissions.pub", "default_permissions.resp",
			"description", "info_url", "mappings", "tags", "trace",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loss, err := compareTakeover(held(t, tc.held), spec(t, tc.spec))
			require.NoError(t, err)
			require.Equal(t, tc.drops, loss.drops)
			require.Equal(t, tc.tiers, loss.tiers)
		})
	}
}
