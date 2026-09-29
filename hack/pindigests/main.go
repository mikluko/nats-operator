// Command pindigests pins the chart's controller images to the digests a
// release built, rewriting only the digest values of the values file named by
// its argument.
//
// It reads from stdin the JSON list the release workflow's images job
// outputs, [{"name": "<repository>", "digest": "sha256:<hex>"}], and fills in
// image.digest of every top-level block whose image.repository is under
// -registry.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"
)

type image struct {
	Name   string `json:"name"`
	Digest string `json:"digest"`
}

func main() {
	registry := flag.String("registry", "", "repository prefix of the images to pin")
	flag.Parse()
	if err := run(*registry, flag.Args()); err != nil {
		fmt.Fprintln(os.Stderr, "pindigests:", err)
		os.Exit(1)
	}
}

func run(registry string, args []string) error {
	if registry == "" || len(args) != 1 {
		return errors.New("usage: pindigests -registry <prefix> <values.yaml> <digests.json")
	}
	var images []image
	if err := json.NewDecoder(os.Stdin).Decode(&images); err != nil {
		return fmt.Errorf("digests: %w", err)
	}
	src, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	out, err := pin(src, registry, images)
	if err != nil {
		return fmt.Errorf("%s: %w", args[0], err)
	}
	return os.WriteFile(args[0], out, 0o644)
}

var digestPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

// pin returns src with image.digest set, in every top-level block whose
// image.repository is under registry, to the digest images lists for that
// repository. It fails unless every image under registry has an empty
// double-quoted digest and a listed digest, and every listed image is one of
// them.
func pin(src []byte, registry string, images []image) ([]byte, error) {
	digests := map[string]string{}
	for _, im := range images {
		if !digestPattern.MatchString(im.Digest) {
			return nil, fmt.Errorf("%s: digest %q is not sha256:<hex>", im.Name, im.Digest)
		}
		digests[im.Name] = im.Digest
	}

	var doc yaml.Node
	if err := yaml.Unmarshal(src, &doc); err != nil {
		return nil, err
	}
	if len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("not a mapping")
	}

	lines := bytes.SplitAfter(src, []byte("\n"))
	pinned := map[string]bool{}
	root := doc.Content[0]
	for i := 1; i < len(root.Content); i += 2 {
		key := root.Content[i-1].Value
		repository := lookup(root.Content[i], "image", "repository")
		if repository == nil || !strings.HasPrefix(repository.Value, registry+"/") {
			continue
		}
		digest, ok := digests[repository.Value]
		if !ok {
			return nil, fmt.Errorf("%s: no digest for %s", key, repository.Value)
		}
		node := lookup(root.Content[i], "image", "digest")
		if node == nil || node.Kind != yaml.ScalarNode || node.Style != yaml.DoubleQuotedStyle || node.Value != "" {
			return nil, fmt.Errorf("%s: image.digest is not \"\"", key)
		}
		line := lines[node.Line-1]
		at := runeOffset(line, node.Column-1)
		if !bytes.HasPrefix(line[at:], []byte(`""`)) {
			return nil, fmt.Errorf("%s: image.digest at line %d is not \"\"", key, node.Line)
		}
		lines[node.Line-1] = bytes.Join([][]byte{line[:at], []byte(strconv.Quote(digest)), line[at+2:]}, nil)
		pinned[repository.Value] = true
	}
	for name := range digests {
		if !pinned[name] {
			return nil, fmt.Errorf("no image pulls %s", name)
		}
	}
	return bytes.Join(lines, nil), nil
}

// lookup returns the value at path in mapping n, or nil.
func lookup(n *yaml.Node, path ...string) *yaml.Node {
	for _, key := range path {
		if n.Kind != yaml.MappingNode {
			return nil
		}
		var next *yaml.Node
		for i := 1; i < len(n.Content); i += 2 {
			if n.Content[i-1].Value == key {
				next = n.Content[i]
			}
		}
		if next == nil {
			return nil
		}
		n = next
	}
	return n
}

// runeOffset returns the byte offset of the rune at index runes in line,
// yaml's columns counting runes.
func runeOffset(line []byte, runes int) int {
	at := 0
	for range runes {
		_, size := utf8.DecodeRune(line[at:])
		at += size
	}
	return at
}
