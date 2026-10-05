package authctl

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/nats-io/jwt/v2"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	authv1beta1 "github.com/mikluko/nats-operator/api/auth/v1beta1"
)

// takeoverField is the spec field that accepts the claims a takeover drops.
const takeoverField = "spec.takeover.droppedClaims"

// takeoverLoss is what the first JWT signed for an account would lose of the
// JWT the servers hold for its key.
type takeoverLoss struct {
	// drops are the JWT field paths, sorted, of the claims the held JWT
	// carries and the signed one would not.
	drops []string
	// tiers are the names, sorted, of the held JWT's JetStream tiers that no
	// spec can name.
	tiers []string
}

// refuseTakeover compares held, the JWT the servers hold, with signed, the
// first JWT signed from spec for the same key, and sets Ready False through
// notReady where signing would lose claims t does not accept losing,
// reporting whether it did.
func refuseTakeover(held, signed string, t *authv1beta1.Takeover, notReady func(reason, msg string)) (bool, error) {
	loss, err := compareTakeover(held, signed)
	if err != nil {
		return false, err
	}
	switch {
	case len(loss.tiers) > 0:
		notReady(ReasonTierInexpressible, fmt.Sprintf("the JWT the servers hold limits JetStream by tiers no spec can name: %s; "+
			"nats-server reads a tier by the replica count of a stream, R1 to R5, and no other", strings.Join(loss.tiers, ", ")))
		return true, nil
	case len(loss.drops) > 0 && (t == nil || t.DroppedClaims != authv1beta1.TakeoverAcceptDroppedClaims):
		notReady(ReasonTakeoverDropsClaims, fmt.Sprintf("the JWT the servers hold carries claims the one signed from spec would not: %s; "+
			"declare them in spec, or set %s to Accept to sign without them", strings.Join(loss.drops, ", "), takeoverField))
		return true, nil
	}
	return false, nil
}

// takeoverRefused reports whether conds say the last signing was refused as
// a takeover, so the servers are asked for their JWT again.
func takeoverRefused(conds []metav1.Condition) bool {
	c := meta.FindStatusCondition(conds, ConditionReady)
	return c != nil && c.Status == metav1.ConditionFalse && (c.Reason == ReasonTakeoverDropsClaims || c.Reason == ReasonTierInexpressible)
}

// compareTakeover returns what signed would lose of held, two account JWTs
// of one key. A claim is lost where held carries it and signed does not; one
// signed with another value is kept. A NATS or account limit is carried
// where it is not the default of jwt.NewAccountClaims, a JetStream limit
// where it is not zero, and a JetStream storage, stream or consumer limit
// signed as jwt.NoLimit against a held bound is lost. Exports are matched
// by subject and type, imports by account, subject and type.
func compareTakeover(held, signed string) (takeoverLoss, error) {
	hc, err := jwt.DecodeAccountClaims(held)
	if err != nil {
		return takeoverLoss{}, fmt.Errorf("decode the JWT the servers hold: %w", err)
	}
	sc, err := jwt.DecodeAccountClaims(signed)
	if err != nil {
		return takeoverLoss{}, fmt.Errorf("decode the signed JWT: %w", err)
	}
	var loss takeoverLoss
	for tier := range hc.Limits.JetStreamTieredLimits {
		if !knownTier(tier) {
			loss.tiers = append(loss.tiers, tier)
		}
	}
	slices.Sort(loss.tiers)
	loss.drops = append(loss.drops, droppedLimits(hc.Limits, sc.Limits)...)
	loss.drops = append(loss.drops, droppedSigningKeys(hc.SigningKeys, sc.SigningKeys)...)
	loss.drops = append(loss.drops, droppedExports(hc.Exports, sc.Exports)...)
	loss.drops = append(loss.drops, droppedImports(hc.Imports, sc.Imports)...)
	loss.drops = append(loss.drops, droppedFields("", claimTree(hc), claimTree(sc))...)
	slices.Sort(loss.drops)
	return loss, nil
}

func knownTier(name string) bool {
	switch authv1beta1.JetStreamTierName(name) {
	case authv1beta1.JetStreamTierR1, authv1beta1.JetStreamTierR2, authv1beta1.JetStreamTierR3, authv1beta1.JetStreamTierR4, authv1beta1.JetStreamTierR5:
		return true
	}
	return false
}

func droppedLimits(h, s jwt.OperatorLimits) []string {
	var out []string
	bound := func(path string, hv, sv int64) {
		if hv != jwt.NoLimit && sv == jwt.NoLimit {
			out = append(out, path)
		}
	}
	bound("limits.subs", h.Subs, s.Subs)
	bound("limits.data", h.Data, s.Data)
	bound("limits.payload", h.Payload, s.Payload)
	bound("limits.imports", h.Imports, s.Imports)
	bound("limits.exports", h.Exports, s.Exports)
	bound("limits.conn", h.Conn, s.Conn)
	bound("limits.leaf", h.LeafNodeConn, s.LeafNodeConn)
	if !h.WildcardExports && s.WildcardExports {
		out = append(out, "limits.wildcards")
	}
	if h.DisallowBearer && !s.DisallowBearer {
		out = append(out, "limits.disallow_bearer")
	}
	out = append(out, droppedJetStream("limits", h.JetStreamLimits, s.JetStreamLimits)...)
	for _, tier := range slices.Sorted(maps.Keys(h.JetStreamTieredLimits)) {
		path := "limits.tiered_limits." + tier
		st, ok := s.JetStreamTieredLimits[tier]
		if !ok {
			out = append(out, path)
			continue
		}
		out = append(out, droppedJetStream(path, h.JetStreamTieredLimits[tier], st)...)
	}
	return out
}

// droppedJetStream returns the limits of h that s loses. A storage, stream
// or consumer limit is lost where s has none, or where h bounds it and s
// does not; the rest where h sets it and s does not.
func droppedJetStream(path string, h, s jwt.JetStreamLimits) []string {
	var out []string
	storage := func(field string, hv, sv int64) {
		if hv != 0 && (sv == 0 || hv != jwt.NoLimit && sv == jwt.NoLimit) {
			out = append(out, path+"."+field)
		}
	}
	storage("mem_storage", h.MemoryStorage, s.MemoryStorage)
	storage("disk_storage", h.DiskStorage, s.DiskStorage)
	storage("streams", h.Streams, s.Streams)
	storage("consumer", h.Consumer, s.Consumer)
	set := func(field string, hv, sv int64) {
		if hv != 0 && sv == 0 {
			out = append(out, path+"."+field)
		}
	}
	set("max_ack_pending", h.MaxAckPending, s.MaxAckPending)
	set("mem_max_stream_bytes", h.MemoryMaxStreamBytes, s.MemoryMaxStreamBytes)
	set("disk_max_stream_bytes", h.DiskMaxStreamBytes, s.DiskMaxStreamBytes)
	if h.MaxBytesRequired && !s.MaxBytesRequired {
		out = append(out, path+".max_bytes_required")
	}
	return out
}

// droppedSigningKeys returns the keys of h that s does not list, and the
// scope of each scoped key of h that s lists plain or with less.
func droppedSigningKeys(h, s jwt.SigningKeys) []string {
	var out []string
	for _, key := range slices.Sorted(maps.Keys(h)) {
		path := "signing_keys[" + key + "]"
		ss, ok := s[key]
		if !ok {
			out = append(out, path)
			continue
		}
		hs, _ := h[key].(*jwt.UserScope)
		if hs == nil {
			continue
		}
		sScope, _ := ss.(*jwt.UserScope)
		if sScope == nil {
			out = append(out, path+".template")
			continue
		}
		out = append(out, droppedFields(path, tree(scopeCopy(hs)), tree(scopeCopy(sScope)))...)
	}
	return out
}

// scopeCopy is s with its NATS limits at jwt.NoLimit, the default of
// jwt.NewUserScope, cleared, so they read as absent.
func scopeCopy(s *jwt.UserScope) *jwt.UserScope {
	out := *s
	for _, v := range []*int64{&out.Template.Subs, &out.Template.Data, &out.Template.Payload} {
		if *v == jwt.NoLimit {
			*v = 0
		}
	}
	return &out
}

func droppedExports(h, s jwt.Exports) []string {
	var out []string
	for i, he := range h {
		path := fmt.Sprintf("exports[%d]", i)
		j := slices.IndexFunc(s, func(se *jwt.Export) bool { return se.Subject == he.Subject && se.Type == he.Type })
		if j < 0 {
			out = append(out, path)
			continue
		}
		out = append(out, droppedFields(path, tree(he), tree(s[j]))...)
	}
	return out
}

func droppedImports(h, s jwt.Imports) []string {
	var out []string
	for i, hi := range h {
		path := fmt.Sprintf("imports[%d]", i)
		j := slices.IndexFunc(s, func(si *jwt.Import) bool {
			return si.Account == hi.Account && si.Subject == hi.Subject && si.Type == hi.Type
		})
		if j < 0 {
			out = append(out, path)
			continue
		}
		out = append(out, droppedFields(path, tree(hi), tree(s[j]))...)
	}
	return out
}

// claimTree is c as JSON, its nats claims at the top level beside its
// standard ones, without what compareTakeover reads apart or what differs
// between any two signings: the issuer, the issue time, the ID and the
// subject, and the expiry beyond whether there is one.
func claimTree(c *jwt.AccountClaims) map[string]any {
	c.Issuer, c.IssuedAt, c.ID, c.Subject = "", 0, "", ""
	if c.Expires != 0 {
		c.Expires = 1
	}
	c.Limits, c.SigningKeys, c.Exports, c.Imports = jwt.OperatorLimits{}, nil, nil, nil
	out := tree(c)
	nats, _ := out["nats"].(map[string]any)
	delete(out, "nats")
	maps.Copy(out, nats)
	delete(out, "limits")
	return out
}

// tree is v as JSON decodes it.
func tree(v any) map[string]any {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil
	}
	return out
}

// droppedFields returns the paths under path of what held carries, as a
// value or a subtree, and signed does not; a value signed carries
// differently is kept.
func droppedFields(path string, held, signed map[string]any) []string {
	var out []string
	for _, k := range slices.Sorted(maps.Keys(held)) {
		p := k
		if path != "" {
			p = path + "." + k
		}
		hv := held[k]
		if absent(hv) {
			continue
		}
		sv := signed[k]
		if absent(sv) {
			out = append(out, p)
			continue
		}
		hm, hOK := hv.(map[string]any)
		sm, sOK := sv.(map[string]any)
		if hOK && sOK {
			out = append(out, droppedFields(p, hm, sm)...)
		}
	}
	return out
}

// absent reports whether v, as JSON decodes it, is what omitempty would
// not have written.
func absent(v any) bool {
	switch v := v.(type) {
	case nil:
		return true
	case bool:
		return !v
	case float64:
		return v == 0
	case string:
		return v == ""
	case []any:
		return len(v) == 0
	case map[string]any:
		return len(v) == 0
	}
	return false
}
