package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// imageRepo is the repository every controller image is built under.
const imageRepo = "localhost/nats-operator"

// image is one controller image as a tarball ko built.
type image struct {
	// Key is the controller's key in the chart's values: cluster, auth or
	// jetstream.
	Key string `json:"key"`
	// Archive is the tarball's path, relative to the images.json it is
	// listed in.
	Archive string `json:"archive"`
	// Source is the name the tarball holds the image under.
	Source string `json:"source"`
	// Repository and Tag are what the image is tagged and deployed as; the
	// tag is taken from its digest, so a changed image rolls its Deployment
	// and an unchanged one does not.
	Repository string `json:"repository"`
	Tag        string `json:"tag"`
}

// buildImages builds every controller under root/cmd into a tarball in
// dir/images with ko for platform, writes their list to dir/images.json and
// returns its path.
func buildImages(ctx context.Context, root, dir, platform string) (string, error) {
	cmds, err := filepath.Glob(filepath.Join(root, "cmd", "*-controller"))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Join(dir, "images"), 0o755); err != nil {
		return "", err
	}
	var images []image
	for _, c := range cmds {
		name := filepath.Base(c)
		archive := filepath.Join("images", name+".tar")
		var stdout, stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, "ko", "build", "--push=false", "-B", "--platform", platform,
			"--tags", "e2e", "--tarball", filepath.Join(dir, archive), "./cmd/"+name)
		cmd.Dir = root
		cmd.Env = append(os.Environ(), "KO_DOCKER_REPO="+imageRepo)
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("ko build %s: %w\n%s", name, err, stderr.String())
		}
		img, err := koImage(name, archive, strings.TrimSpace(stdout.String()))
		if err != nil {
			return "", err
		}
		logf("image %s:%s", img.Repository, img.Tag)
		images = append(images, img)
	}
	path := filepath.Join(dir, "images.json")
	b, err := json.MarshalIndent(images, "", "  ")
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, b, 0o644)
}

// koImage describes controller name's tarball archive from the reference
// `ko build` printed for it, <repository>:e2e@sha256:<digest>.
func koImage(name, archive, ref string) (image, error) {
	_, digest, ok := strings.Cut(ref, "@sha256:")
	if !ok || len(digest) < 12 {
		return image{}, fmt.Errorf("ko build %s printed %q, not a digest reference", name, ref)
	}
	repo := imageRepo + "/" + name
	return image{
		Key:        strings.TrimSuffix(name, "-controller"),
		Archive:    archive,
		Source:     repo + ":e2e",
		Repository: repo,
		Tag:        digest[:12],
	}, nil
}

// readImages returns the images path lists, each Archive made absolute.
func readImages(path string) ([]image, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var images []image
	if err := json.Unmarshal(b, &images); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	for i := range images {
		images[i].Archive = filepath.Join(filepath.Dir(path), images[i].Archive)
	}
	return images, nil
}
