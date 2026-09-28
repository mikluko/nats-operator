package main

import (
	"archive/tar"
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

//go:embed machine.sh
var machineScript string

const (
	machineImage = "localhost/nats-operator/e2e-machine"
	// machineTree is where the working tree is shipped to in the machine,
	// on the machine's own disk.
	machineTree = "/var/lib/nats-operator-e2e/src"
	// machineBinary is this command built for Linux, relative to the tree.
	machineBinary = "bin/e2e/e2e-linux"
	// machineLock is held for as long as a run in the machine lasts, one
	// whose host side was killed included, so the next run does not ship
	// over it. flock -o keeps it from the podman daemons a run starts, which
	// outlive the run.
	machineLock = "/var/lib/nats-operator-e2e/run.lock"
)

// viaMachine runs the harness as root inside the Apple container machine
// cfg.machine, on the working tree at root and builds made on the host.
func viaMachine(ctx context.Context, cfg config, root string, down bool) error {
	work := filepath.Join(root, "bin", "e2e")
	platform := "linux/" + runtime.GOARCH
	args := []string{"-down"}
	if !down {
		images, err := buildImages(ctx, root, work, platform)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, images)
		if err != nil {
			return err
		}
		args = []string{"-images", rel}
	}
	build := exec.CommandContext(ctx, "go", "build", "-o", filepath.Join(root, machineBinary), "./hack/e2e")
	build.Dir = root
	build.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if err := quietly(build); err != nil {
		return err
	}
	logf("machine %s", cfg.machine)
	if err := machineUp(ctx, cfg, root); err != nil {
		return err
	}
	if err := machineRun(ctx, cfg.machine, strings.NewReader(lockFree), io.Discard, os.Stderr, "sh", "-s"); err != nil {
		return fmt.Errorf("another e2e run holds machine %s; stop it or wait for it: %w", cfg.machine, err)
	}
	files, err := shippedFiles(ctx, root)
	if err != nil {
		return err
	}
	logf("ship %d files to %s:%s", len(files), cfg.machine, machineTree)
	if err := ship(ctx, cfg.machine, root, files); err != nil {
		return err
	}
	script := fmt.Sprintf("cd %s && exec flock -n -o %s env %s %s %s\n", shellQuote(machineTree), shellQuote(machineLock),
		strings.Join(e2eEnv(os.Environ()), " "), shellQuote("./"+machineBinary), strings.Join(args, " "))
	return machineRun(ctx, cfg.machine, strings.NewReader(script), os.Stdout, os.Stderr, "sh", "-s")
}

// lockFree exits 0 when no run holds machineLock.
var lockFree = fmt.Sprintf("mkdir -p %s && exec flock -n %s true\n", shellQuote(filepath.Dir(machineLock)), shellQuote(machineLock))

// machineUp creates the machine from hack/machine.Containerfile when it
// does not exist, and runs machineScript in it with the resolver set to
// cfg.dns. A new machine is stopped once created: the first `container
// machine run` after create fails and stops it. The gateway resolver Apple
// container hands out does not answer on every host.
func machineUp(ctx context.Context, cfg config, root string) error {
	if exec.CommandContext(ctx, "container", "machine", "inspect", cfg.machine).Run() != nil {
		if err := quietly(exec.CommandContext(ctx, "container", "build", "--dns", cfg.dns, "-t", machineImage,
			"-f", filepath.Join(root, "hack", "machine.Containerfile"), filepath.Join(root, "hack"))); err != nil {
			return err
		}
		if err := quietly(exec.CommandContext(ctx, "container", "machine", "create", machineImage,
			"--name", cfg.machine, "--cpus", "6", "--memory", "12G", "--home-mount", "none")); err != nil {
			return err
		}
		if err := quietly(exec.CommandContext(ctx, "container", "machine", "stop", cfg.machine)); err != nil {
			return err
		}
	}
	var out bytes.Buffer
	if err := machineRun(ctx, cfg.machine, strings.NewReader(machineScript), &out, &out, "env", "M_DNS="+cfg.dns, "sh", "-s"); err != nil {
		return fmt.Errorf("machine %s: %w\n%s", cfg.machine, err, out.String())
	}
	return nil
}

// machineRun runs args as root in the machine, from /. `container machine
// run` joins and re-splits its command line, so no argument may hold a
// space; scripts go through stdin.
func machineRun(ctx context.Context, machine string, stdin io.Reader, stdout, stderr io.Writer, args ...string) error {
	cmd := exec.CommandContext(ctx, "container", append([]string{"machine", "run", "-i", "-n", machine, "--root", "-w", "/", "--"}, args...)...)
	cmd.Stdin, cmd.Stdout = stdin, stdout
	cmd.Stderr = &dropLines{w: stderr, substr: "Operation not supported by device"}
	return cmd.Run()
}

// dropLines writes to w every line that does not contain substr.
// `container machine run` prints one such line on every run.
type dropLines struct {
	w      io.Writer
	substr string
	buf    []byte
}

func (d *dropLines) Write(p []byte) (int, error) {
	d.buf = append(d.buf, p...)
	for {
		i := bytes.IndexByte(d.buf, '\n')
		if i < 0 {
			return len(p), nil
		}
		line := d.buf[:i+1]
		if !bytes.Contains(line, []byte(d.substr)) {
			if _, err := d.w.Write(line); err != nil {
				return len(p), err
			}
		}
		d.buf = d.buf[i+1:]
	}
}

// shippedFiles are the files under root the machine needs, relative to it:
// every file git tracks or would track, and every file under bin/e2e.
func shippedFiles(ctx context.Context, root string) ([]string, error) {
	cmd := exec.CommandContext(ctx, "git", "ls-files", "-z", "--cached", "--others", "--exclude-standard")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w", err)
	}
	var files []string
	for f := range strings.SplitSeq(string(out), "\x00") {
		if f == "" {
			continue
		}
		if _, err := os.Lstat(filepath.Join(root, f)); err == nil {
			files = append(files, f)
		}
	}
	err = filepath.WalkDir(filepath.Join(root, "bin", "e2e"), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(files)
	return slices.Compact(files), nil
}

// ship replaces machineTree in the machine with files, relative to root.
func ship(ctx context.Context, machine, root string, files []string) error {
	clear := fmt.Sprintf("rm -rf %[1]s && mkdir -p %[1]s\n", shellQuote(machineTree))
	if err := machineRun(ctx, machine, strings.NewReader(clear), io.Discard, os.Stderr, "sh", "-s"); err != nil {
		return err
	}
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(writeTar(pw, root, files)) }()
	return machineRun(ctx, machine, pr, io.Discard, os.Stderr, "tar", "-x", "-f", "-", "-C", machineTree)
}

// writeTar writes files, relative to root, to w as a tar archive; symbolic
// links are archived as links.
func writeTar(w io.Writer, root string, files []string) error {
	tw := tar.NewWriter(w)
	for _, f := range files {
		p := filepath.Join(root, f)
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		link := ""
		if fi.Mode()&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		}
		h, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		h.Name = f
		h.Uid, h.Gid, h.Uname, h.Gname = 0, 0, "", ""
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			continue
		}
		if err := copyFile(tw, p); err != nil {
			return err
		}
	}
	return tw.Close()
}

func copyFile(w io.Writer, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	_, err = io.Copy(w, f)
	return err
}

// e2eEnv returns the E2E_* variables of environ as shell-quoted
// assignments.
func e2eEnv(environ []string) []string {
	var out []string
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, "E2E_") {
			out = append(out, k+"="+shellQuote(v))
		}
	}
	return out
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
