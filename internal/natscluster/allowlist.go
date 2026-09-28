package natscluster

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/nats-io/nats-server/v2/conf"
)

// reloadRule is when a change under a reloadable key reloads.
type reloadRule int

const (
	// reloadAlways reloads any change.
	reloadAlways reloadRule = iota
	// reloadRaiseOnly reloads a number set before and after that rises.
	reloadRaiseOnly
	// reloadChildren reloads when the objects at the key, an absent one
	// counting as empty, differ only under the key's Children.
	reloadChildren
	// reloadSystemAccountEntry reloads a change to the entry under the key
	// named by system_account, itself unchanged.
	reloadSystemAccountEntry
)

// reloadKey is a config key nats-server applies on reload. Path is the
// key's dotted path in the rendered config and covers every key beneath
// it; Case is the diffOptions switch case that applies it, or the
// unexported Options field diffOptions skips.
type reloadKey struct {
	Path     string
	Case     string
	Rule     reloadRule
	Children []string
}

// reloadAllowLists are the reloadable config keys by nats-server
// major.minor, read off that version's diffOptions in server/reload.go. A
// key absent from a version's list restarts: diffOptions rejects it, or
// silently keeps the old value. resolver is absent although diffOptions
// accepts it: 2.15's reload replaces the running resolver with one it
// never starts, which keeps answering claim updates into the old directory
// while lookups read the new one. resolver_preload is a value a reload
// keeps, and reloads only for the system account: its re-signed JWT reaches
// running servers as a claims update, and the preload matters only at the
// next start. Another account's preload is replaced by nothing but a
// restart.
var reloadAllowLists = map[string][]reloadKey{
	"2.15": {
		{Path: "pid_file", Case: "pidfile", Rule: reloadAlways},
		{Path: "server_tags", Case: "tags", Rule: reloadAlways},
		{Path: "server_metadata", Case: "metadata", Rule: reloadAlways},
		{Path: "max_payload", Case: "maxpayload", Rule: reloadAlways},
		{Path: "cluster.routes", Case: "routes", Rule: reloadAlways},
		{Path: "cluster.tls", Case: "cluster", Rule: reloadAlways},
		{Path: "jetstream.max_memory_store", Case: "jetstreammaxmemory", Rule: reloadRaiseOnly},
		{Path: "jetstream.max_file_store", Case: "jetstreammaxstore", Rule: reloadRaiseOnly},
		{Path: "leafnodes", Case: "leafnode", Rule: reloadChildren, Children: []string{"remotes"}},
		{Path: "resolver_preload", Case: "resolverpreloads", Rule: reloadSystemAccountEntry},
	},
}

// minorVersion returns the major.minor of a semantic version, or "" when v
// is not one.
func minorVersion(v string) string {
	major, rest, ok := strings.Cut(strings.TrimPrefix(v, "v"), ".")
	if !ok {
		return ""
	}
	minor, _, _ := strings.Cut(rest, ".")
	if major == "" || minor == "" {
		return ""
	}
	return major + "." + minor
}

// restartReason classifies the change from rendered config from to config
// to on nats-server version: "" when a reload applies all of it, otherwise
// what makes it restart-only. Configs that do not decode restart.
func restartReason(version string, from, to []byte) string {
	allow, ok := reloadAllowLists[minorVersion(version)]
	if !ok {
		return fmt.Sprintf("nats-server %s has no reload allow-list", version)
	}
	var old, next map[string]any
	if err := json.Unmarshal(from, &old); err != nil {
		return "the running config does not decode: " + err.Error()
	}
	if err := json.Unmarshal(to, &next); err != nil {
		return "the rendered config does not decode: " + err.Error()
	}
	var restart []string
	for _, path := range changedPaths("", old, next) {
		if !reloads(allow, path, old, next) {
			restart = append(restart, path)
		}
	}
	switch len(restart) {
	case 0:
		return ""
	case 1:
		return restart[0] + " is restart-only"
	default:
		return strings.Join(restart, ", ") + " are restart-only"
	}
}

// changedPaths returns the sorted dotted paths at which a and b differ,
// descending into objects both sides hold and stopping at anything else.
func changedPaths(prefix string, a, b map[string]any) []string {
	var out []string
	keys := map[string]bool{}
	for k := range a {
		keys[k] = true
	}
	for k := range b {
		keys[k] = true
	}
	for k := range keys {
		path := k
		if prefix != "" {
			path = prefix + "." + k
		}
		av, aok := a[k]
		bv, bok := b[k]
		am, amok := av.(map[string]any)
		bm, bmok := bv.(map[string]any)
		switch {
		case aok && bok && amok && bmok:
			out = append(out, changedPaths(path, am, bm)...)
		case aok != bok || !reflect.DeepEqual(av, bv):
			out = append(out, path)
		}
	}
	slices.Sort(out)
	return out
}

// reloads reports whether allow covers the change at path between old and
// next, judged by the rule of the longest key at or above path.
func reloads(allow []reloadKey, path string, old, next map[string]any) bool {
	var key *reloadKey
	for i := range allow {
		k := &allow[i]
		if (path == k.Path || strings.HasPrefix(path, k.Path+".")) && (key == nil || len(k.Path) > len(key.Path)) {
			key = k
		}
	}
	if key == nil {
		return false
	}
	switch key.Rule {
	case reloadRaiseOnly:
		a, aok := lookup(old, key.Path).(float64)
		b, bok := lookup(next, key.Path).(float64)
		return aok && bok && b > a
	case reloadChildren:
		a, _ := lookup(old, key.Path).(map[string]any)
		b, _ := lookup(next, key.Path).(map[string]any)
		return reflect.DeepEqual(without(a, key.Children), without(b, key.Children))
	case reloadSystemAccountEntry:
		sys, _ := next["system_account"].(string)
		return sys != "" && old["system_account"] == sys && path == key.Path+"."+sys
	default:
		return true
	}
}

// without returns a copy of m less keys, never nil.
func without(m map[string]any, keys []string) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if !slices.Contains(keys, k) {
			out[k] = v
		}
	}
	return out
}

// lookup returns the value at a dotted path in m, or nil.
func lookup(m map[string]any, path string) any {
	var v any = m
	for part := range strings.SplitSeq(path, ".") {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = obj[part]
	}
	return v
}

// configDigest is the config_digest nats-server reports in VARZ after
// loading config file data: the SHA-256 of the JSON encoding of the file's
// pedantic parse, as conf.ParseFileWithChecksDigest computes it.
func configDigest(data []byte) (string, error) {
	m, err := conf.ParseWithChecks(string(data))
	if err != nil {
		return "", fmt.Errorf("parse config: %w", err)
	}
	h := sha256.New()
	if err := json.NewEncoder(h).Encode(m); err != nil {
		return "", fmt.Errorf("digest config: %w", err)
	}
	return fmt.Sprintf("sha256:%x", h.Sum(nil)), nil
}
