// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package local

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/ax/internal/install/cluster"
	"github.com/google/ax/internal/tunnel"
)

const (
	defaultPostgresImage = "postgres:17-alpine"
	defaultPostgresPort  = 5432
	defaultSubstratePort = 50051
	defaultServerPort    = 8090
	startupWaitTimeout   = 25 * time.Second
)

// AteLocalState is persisted under $AX_HOME/local/<name>/state.json so
// subsequent start/status/stop/delete invocations remember ports, runtime,
// and per-environment container naming.
type AteLocalState struct {
	Name            string `json:"name"`
	Runtime         string `json:"runtime"` // "apple" or "podman"
	PostgresName    string `json:"postgresName"`
	PostgresImage   string `json:"postgresImage"`
	ContainerPrefix string `json:"containerPrefix"`
	PostgresPort    int    `json:"postgresPort"`
	SubstratePort   int    `json:"substratePort"`
	ServerPort      int    `json:"serverPort"`
}

// AteLocalBackend runs Postgres + ate-local + ax-server natively without
// Kubernetes, using Apple `container` microVMs (default on macOS) or Podman.
type AteLocalBackend struct {
	Out io.Writer
}

// NewAteLocalBackend constructs an AteLocalBackend writing progress to out.
func NewAteLocalBackend(out io.Writer) *AteLocalBackend {
	if out == nil {
		out = io.Discard
	}
	return &AteLocalBackend{Out: out}
}

func (b *AteLocalBackend) Name() string { return BackendAteLocal }

func sanitizeEnvName(name string) string {
	if name == "" {
		return cluster.DefaultName
	}
	var sb strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			sb.WriteRune(r)
		} else {
			sb.WriteRune('-')
		}
	}
	s := strings.Trim(sb.String(), "-")
	if s == "" {
		return cluster.DefaultName
	}
	return s
}

// AteLocalStateDir returns $AX_HOME/local/<name>.
func AteLocalStateDir(name string) (string, error) {
	home, err := tunnel.AXHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "local", sanitizeEnvName(name)), nil
}

// HasAteLocalState reports whether an ate-local environment named name has
// persisted state under $AX_HOME/local/<name>/state.json.
func HasAteLocalState(name string) bool {
	dir, err := AteLocalStateDir(name)
	if err != nil {
		return false
	}
	_, err = os.Stat(filepath.Join(dir, "state.json"))
	return err == nil
}

func loadAteLocalState(dir string) (*AteLocalState, error) {
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var st AteLocalState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func saveAteLocalState(dir string, st AteLocalState) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "state.json"), append(data, '\n'), 0o600)
}

func pickPort(requested, fallback int) (int, error) {
	if requested > 0 {
		return requested, nil
	}
	if portFree(fallback) {
		return fallback, nil
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

func portFree(port int) bool {
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	_ = ln.Close()
	return true
}

func portListening(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 300*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func resolveAteRuntime(requested string) (string, error) {
	rt := strings.ToLower(strings.TrimSpace(requested))
	if rt == "" {
		if runtime.GOOS == "darwin" {
			if _, err := exec.LookPath("container"); err == nil {
				rt = "apple"
			}
		}
		if rt == "" {
			if _, err := exec.LookPath("podman"); err == nil {
				rt = "podman"
			}
		}
	}
	switch rt {
	case "apple":
		if _, err := exec.LookPath("container"); err != nil {
			return "", fmt.Errorf("Apple container CLI not found on PATH (install from https://github.com/apple/container or pass --runtime=podman)")
		}
		return "apple", nil
	case "podman":
		if _, err := exec.LookPath("podman"); err != nil {
			return "", fmt.Errorf("podman CLI not found on PATH")
		}
		return "podman", nil
	default:
		return "", fmt.Errorf("unsupported ate-local runtime %q (expected: apple or podman)", rt)
	}
}

func renderAteLocalConfig(st AteLocalState, snapshotDir, runnerBin string) string {
	return fmt.Sprintf(`address: "127.0.0.1:%d"
runtime: "%s"
container_prefix: "%s"
snapshot_dir: "%s"
ready_timeout: "20s"
templates:
  default:
    image: "debian:12"
    port: 80
    volumes:
      - "%s:/usr/local/bin/ax-runner"
      - "%s:/usr/local/bin/ax-task-runner"
    cpus: 2
    memory: "1024M"
`, st.SubstratePort, st.Runtime, st.ContainerPrefix, snapshotDir, runnerBin, runnerBin)
}

type ateLocalBinaries struct {
	ateLocal string
	axServer string
	axRunner string
}

// resolveBinaries locates ate-local, ax-server, and linux/<arch> ax-runner in
// $AX_LOCAL_BIN_DIR, $AX_HOME/bin, sibling to the running `ax` binary, or
// builds them with `go build` when invoked from a google/ax source tree.
func (b *AteLocalBackend) resolveBinaries(ctx context.Context, stateDir string) (ateLocalBinaries, error) {
	guestArch := runtime.GOARCH
	if guestArch != "arm64" && guestArch != "amd64" {
		guestArch = "arm64"
	}
	runnerRel := filepath.Join("linux_"+guestArch, "ax-runner")

	var searchDirs []string
	if envDir := strings.TrimSpace(os.Getenv("AX_LOCAL_BIN_DIR")); envDir != "" {
		searchDirs = append(searchDirs, envDir)
	}
	if exe, err := os.Executable(); err == nil {
		searchDirs = append(searchDirs, filepath.Dir(exe))
	}
	if home, err := tunnel.AXHomeDir(); err == nil {
		searchDirs = append(searchDirs, filepath.Join(home, "bin"))
	}
	searchDirs = append(searchDirs, filepath.Join(stateDir, "bin"))

	for _, dir := range searchDirs {
		bins := ateLocalBinaries{
			ateLocal: filepath.Join(dir, "ate-local"),
			axServer: filepath.Join(dir, "ax-server"),
			axRunner: filepath.Join(dir, runnerRel),
		}
		if executableExists(bins.ateLocal) && executableExists(bins.axServer) && executableExists(bins.axRunner) {
			return bins, nil
		}
	}

	// Fallback: build from the current repo root if cmd/ate-local exists.
	repoRoot := findAXRepoRoot()
	if repoRoot == "" {
		return ateLocalBinaries{}, fmt.Errorf("could not locate ate-local, ax-server, and %s binaries; set AX_LOCAL_BIN_DIR or build them with examples/ate_local/local-stack.sh", runnerRel)
	}
	binDir := filepath.Join(stateDir, "bin")
	if err := os.MkdirAll(filepath.Join(binDir, "linux_"+guestArch), 0o755); err != nil {
		return ateLocalBinaries{}, err
	}
	bins := ateLocalBinaries{
		ateLocal: filepath.Join(binDir, "ate-local"),
		axServer: filepath.Join(binDir, "ax-server"),
		axRunner: filepath.Join(binDir, runnerRel),
	}
	fmt.Fprintf(b.Out, "==> Building ate-local, ax-server, and linux/%s ax-runner into %s...\n", guestArch, binDir)
	for _, spec := range []struct {
		out  string
		pkg  string
		env  []string
		args []string
	}{
		{out: bins.ateLocal, pkg: "./cmd/ate-local"},
		{out: bins.axServer, pkg: "./cmd/ax-server"},
		{
			out:  bins.axRunner,
			pkg:  "./cmd/ax-runner",
			env:  []string{"GOOS=linux", "GOARCH=" + guestArch, "CGO_ENABLED=0"},
			args: []string{"-trimpath", "-ldflags=-s -w"},
		},
	} {
		cmdArgs := append([]string{"build"}, spec.args...)
		cmdArgs = append(cmdArgs, "-o", spec.out, spec.pkg)
		cmd := exec.CommandContext(ctx, "go", cmdArgs...)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), spec.env...)
		if out, err := cmd.CombinedOutput(); err != nil {
			return ateLocalBinaries{}, fmt.Errorf("go build %s: %w\n%s", spec.pkg, err, strings.TrimSpace(string(out)))
		}
	}
	return bins, nil
}

func executableExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir() && info.Mode()&0o111 != 0
}

func findAXRepoRoot() string {
	candidates := []string{}
	if wd, err := os.Getwd(); err == nil {
		candidates = append(candidates, wd)
	}
	if exe, err := os.Executable(); err == nil {
		d := filepath.Dir(exe)
		candidates = append(candidates, d, filepath.Dir(d), filepath.Dir(filepath.Dir(d)))
	}
	for _, dir := range candidates {
		for cur := dir; cur != "" && cur != filepath.Dir(cur); cur = filepath.Dir(cur) {
			if _, err := os.Stat(filepath.Join(cur, "cmd", "ate-local", "main.go")); err == nil {
				return cur
			}
		}
	}
	return ""
}

func (b *AteLocalBackend) Start(ctx context.Context, opts StartOptions) (Endpoint, error) {
	name := sanitizeEnvName(opts.Name)
	stateDir, err := AteLocalStateDir(name)
	if err != nil {
		return Endpoint{}, err
	}
	pidsDir := filepath.Join(stateDir, "pids")
	logsDir := filepath.Join(stateDir, "logs")
	snapDir := filepath.Join(stateDir, "snapshots")
	for _, d := range []string{pidsDir, logsDir, snapDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return Endpoint{}, err
		}
	}

	existing, err := loadAteLocalState(stateDir)
	if err != nil {
		return Endpoint{}, err
	}

	rtName, err := resolveAteRuntime(opts.Runtime)
	if err != nil {
		if existing != nil && existing.Runtime != "" {
			rtName = existing.Runtime
		} else {
			return Endpoint{}, err
		}
	}

	var st AteLocalState
	if existing != nil {
		st = *existing
		if opts.PostgresPort > 0 {
			st.PostgresPort = opts.PostgresPort
		}
		if opts.SubstratePort > 0 {
			st.SubstratePort = opts.SubstratePort
		}
		if opts.ServerPort > 0 {
			st.ServerPort = opts.ServerPort
		}
	} else {
		pgPort, err := pickPort(opts.PostgresPort, defaultPostgresPort)
		if err != nil {
			return Endpoint{}, fmt.Errorf("allocating postgres port: %w", err)
		}
		subPort, err := pickPort(opts.SubstratePort, defaultSubstratePort)
		if err != nil {
			return Endpoint{}, fmt.Errorf("allocating ate-local port: %w", err)
		}
		srvPort, err := pickPort(opts.ServerPort, defaultServerPort)
		if err != nil {
			return Endpoint{}, fmt.Errorf("allocating ax-server port: %w", err)
		}
		st = AteLocalState{
			Name:            name,
			Runtime:         rtName,
			PostgresName:    "ax-" + name + "-postgres",
			PostgresImage:   defaultPostgresImage,
			ContainerPrefix: "ate-" + name,
			PostgresPort:    pgPort,
			SubstratePort:   subPort,
			ServerPort:      srvPort,
		}
	}
	if err := saveAteLocalState(stateDir, st); err != nil {
		return Endpoint{}, err
	}

	bins, err := b.resolveBinaries(ctx, stateDir)
	if err != nil {
		return Endpoint{}, err
	}

	if err := b.ensurePostgres(ctx, st); err != nil {
		return Endpoint{}, err
	}

	cfgPath := filepath.Join(stateDir, "ate-local.yaml")
	cfgContent := renderAteLocalConfig(st, snapDir, bins.axRunner)
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0o600); err != nil {
		return Endpoint{}, err
	}

	if err := b.startDaemon(
		"ate-local",
		filepath.Join(pidsDir, "ate-local.pid"),
		filepath.Join(logsDir, "ate-local.log"),
		st.SubstratePort,
		bins.ateLocal,
		[]string{"--config", cfgPath},
	); err != nil {
		return Endpoint{}, err
	}

	dsn := fmt.Sprintf("postgres://ax:ax@127.0.0.1:%d/ax?sslmode=disable", st.PostgresPort)
	routerURL := fmt.Sprintf("http://127.0.0.1:%d", st.SubstratePort)
	if err := b.startDaemon(
		"ax-server",
		filepath.Join(pidsDir, "ax-server.pid"),
		filepath.Join(logsDir, "ax-server.log"),
		st.ServerPort,
		bins.axServer,
		[]string{
			"--addr", fmt.Sprintf("127.0.0.1:%d", st.ServerPort),
			"--router-url", routerURL,
			"--store", dsn,
			"--substrate-endpoint", fmt.Sprintf("127.0.0.1:%d", st.SubstratePort),
			"--substrate-plaintext",
		},
	); err != nil {
		return Endpoint{}, err
	}

	if err := waitHTTPHealthz(ctx, st.ServerPort, startupWaitTimeout); err != nil {
		return Endpoint{}, fmt.Errorf("waiting for ax-server /healthz on :%d: %w", st.ServerPort, err)
	}

	ep := Endpoint{
		Name:        name,
		Backend:     BackendAteLocal,
		ServerURL:   fmt.Sprintf("http://127.0.0.1:%d", st.ServerPort),
		RouterAddr:  fmt.Sprintf("127.0.0.1:%d", st.SubstratePort),
		KubeContext: name,
	}
	if err := tunnel.SaveLocalEndpoint(tunnel.LocalEndpoint{
		Name:       ep.Name,
		Backend:    ep.Backend,
		ServerURL:  ep.ServerURL,
		RouterAddr: ep.RouterAddr,
	}); err != nil {
		return Endpoint{}, err
	}
	if err := tunnel.SetActiveLocalContext(ep.Name); err != nil {
		return Endpoint{}, err
	}
	return ep, nil
}

func (b *AteLocalBackend) ensurePostgres(ctx context.Context, st AteLocalState) error {
	cli := "container"
	if st.Runtime == "podman" {
		cli = "podman"
	}
	if postgresRunning(cli, st.PostgresName) {
		fmt.Fprintf(b.Out, "==> PostgreSQL (%s) already running on 127.0.0.1:%d.\n", st.PostgresName, st.PostgresPort)
	} else if postgresExists(cli, st.PostgresName) {
		fmt.Fprintf(b.Out, "==> Starting stopped PostgreSQL (%s) on 127.0.0.1:%d...\n", st.PostgresName, st.PostgresPort)
		if out, err := exec.CommandContext(ctx, cli, "start", st.PostgresName).CombinedOutput(); err != nil {
			return fmt.Errorf("%s start %s: %w (%s)", cli, st.PostgresName, err, strings.TrimSpace(string(out)))
		}
	} else {
		fmt.Fprintf(b.Out, "==> Starting PostgreSQL (%s) in %s on 127.0.0.1:%d...\n", st.PostgresImage, st.Runtime, st.PostgresPort)
		args := []string{"run", "-d"}
		if cli == "container" {
			args = append(args, "--progress", "none", "--memory", "512M")
		} else {
			args = append(args, "--memory", "512m")
		}
		args = append(args,
			"--name", st.PostgresName,
			"-e", "POSTGRES_USER=ax",
			"-e", "POSTGRES_PASSWORD=ax",
			"-e", "POSTGRES_DB=ax",
			"-p", fmt.Sprintf("127.0.0.1:%d:5432", st.PostgresPort),
			st.PostgresImage,
		)
		if out, err := exec.CommandContext(ctx, cli, args...).CombinedOutput(); err != nil {
			return fmt.Errorf("%s run %s: %w (%s)", cli, st.PostgresName, err, strings.TrimSpace(string(out)))
		}
	}

	if err := waitPort(ctx, st.PostgresPort, startupWaitTimeout); err != nil {
		return fmt.Errorf("waiting for postgres port %d: %w", st.PostgresPort, err)
	}
	deadline := time.Now().Add(startupWaitTimeout)
	for time.Now().Before(deadline) {
		if exec.CommandContext(ctx, cli, "exec", st.PostgresName, "psql", "-U", "ax", "-d", "ax", "-c", "SELECT 1").Run() == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return fmt.Errorf("PostgreSQL %s did not accept SQL queries within %s", st.PostgresName, startupWaitTimeout)
}

func postgresRunning(cli, name string) bool {
	if cli == "podman" {
		out, err := exec.Command(cli, "inspect", "-f", "{{.State.Running}}", name).Output()
		return err == nil && strings.TrimSpace(string(out)) == "true"
	}
	out, err := exec.Command(cli, "list", "--format", "json").Output()
	if err != nil {
		return false
	}
	return containerStateFromJSON(out, name) == "running"
}

func postgresExists(cli, name string) bool {
	if cli == "podman" {
		return exec.Command(cli, "inspect", name).Run() == nil
	}
	out, err := exec.Command(cli, "list", "--all", "--format", "json").Output()
	if err != nil {
		return false
	}
	return containerStateFromJSON(out, name) != ""
}

func containerStateFromJSON(raw []byte, id string) string {
	var items []struct {
		Status        json.RawMessage `json:"status"`
		Configuration struct {
			ID string `json:"id"`
		} `json:"configuration"`
	}
	if err := json.Unmarshal(raw, &items); err != nil {
		return ""
	}
	for _, it := range items {
		if it.Configuration.ID != id {
			continue
		}
		var s string
		if json.Unmarshal(it.Status, &s) == nil && s != "" {
			return strings.ToLower(strings.TrimSpace(s))
		}
		var obj struct {
			State string `json:"state"`
		}
		if json.Unmarshal(it.Status, &obj) == nil {
			return strings.ToLower(strings.TrimSpace(obj.State))
		}
		return "unknown"
	}
	return ""
}

func (b *AteLocalBackend) startDaemon(label, pidFile, logFile string, port int, bin string, args []string) error {
	if pid := readPID(pidFile); pid > 0 && pidAlive(pid) && portListening(port) {
		fmt.Fprintf(b.Out, "==> %s already running (pid %d, 127.0.0.1:%d).\n", label, pid, port)
		return nil
	}
	if !portFree(port) {
		return fmt.Errorf("port 127.0.0.1:%d needed by %s is already in use", port, label)
	}
	fmt.Fprintf(b.Out, "==> Starting %s on 127.0.0.1:%d...\n", label, port)
	lf, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	cmd := exec.Command(bin, args...)
	cmd.Stdout = lf
	cmd.Stderr = lf
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = lf.Close()
		return fmt.Errorf("starting %s: %w", label, err)
	}
	_ = lf.Close()
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(cmd.Process.Pid)+"\n"), 0o600); err != nil {
		return err
	}
	// Detach so the daemon outlives `ax local start`.
	_ = cmd.Process.Release()

	if err := waitPort(context.Background(), port, startupWaitTimeout); err != nil {
		return fmt.Errorf("%s did not listen on 127.0.0.1:%d (see %s): %w", label, port, logFile, err)
	}
	return nil
}

func waitPort(ctx context.Context, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if portListening(port) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out after %s", timeout)
}

func waitHTTPHealthz(ctx context.Context, port int, timeout time.Duration) error {
	url := fmt.Sprintf("http://127.0.0.1:%d/healthz", port)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	return fmt.Errorf("timed out waiting for %s", url)
}

func readPID(path string) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0
	}
	return n
}

func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	return syscall.Kill(pid, 0) == nil
}

func stopDaemon(pidFile string) {
	pid := readPID(pidFile)
	if pid <= 0 || !pidAlive(pid) {
		_ = os.Remove(pidFile)
		return
	}
	_ = syscall.Kill(pid, syscall.SIGTERM)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !pidAlive(pid) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if pidAlive(pid) {
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}
	_ = os.Remove(pidFile)
}

func (b *AteLocalBackend) Status(_ context.Context, name string) (cluster.Status, error) {
	clean := sanitizeEnvName(name)
	stateDir, err := AteLocalStateDir(clean)
	if err != nil {
		return cluster.Status{}, err
	}
	st, err := loadAteLocalState(stateDir)
	if err != nil {
		return cluster.Status{}, err
	}
	if st == nil {
		return cluster.Status{
			Name:            clean,
			Backend:         BackendAteLocal,
			State:           cluster.StateAbsent,
			KubeContext:     clean,
			SubstrateStatus: "NotFound",
			AXStatus:        "NotFound",
		}, nil
	}

	pidsDir := filepath.Join(stateDir, "pids")
	subUp := pidAlive(readPID(filepath.Join(pidsDir, "ate-local.pid"))) && portListening(st.SubstratePort)
	srvUp := pidAlive(readPID(filepath.Join(pidsDir, "ax-server.pid"))) && portListening(st.ServerPort)
	pgUp := portListening(st.PostgresPort)

	overall := cluster.StateStopped
	if subUp && srvUp && pgUp {
		overall = cluster.StateRunning
	}

	subStatus := fmt.Sprintf("Stopped (%s, :%d)", st.Runtime, st.SubstratePort)
	if subUp {
		subStatus = fmt.Sprintf("Running (%s, 127.0.0.1:%d)", st.Runtime, st.SubstratePort)
	}
	axStatus := fmt.Sprintf("Stopped (:%d)", st.ServerPort)
	if srvUp {
		axStatus = fmt.Sprintf("Running (http://127.0.0.1:%d)", st.ServerPort)
	}

	return cluster.Status{
		Name:            st.Name,
		Backend:         BackendAteLocal,
		State:           overall,
		KubeContext:     st.Name,
		Nodes:           []string{fmt.Sprintf("%s (postgres:%d)", st.Runtime, st.PostgresPort)},
		SubstrateStatus: subStatus,
		AXStatus:        axStatus,
	}, nil
}

func (b *AteLocalBackend) Stop(ctx context.Context, name string) error {
	clean := sanitizeEnvName(name)
	stateDir, err := AteLocalStateDir(clean)
	if err != nil {
		return err
	}
	st, err := loadAteLocalState(stateDir)
	if err != nil {
		return err
	}
	if st == nil {
		return nil
	}
	pidsDir := filepath.Join(stateDir, "pids")
	stopDaemon(filepath.Join(pidsDir, "ax-server.pid"))
	stopDaemon(filepath.Join(pidsDir, "ate-local.pid"))
	cli := "container"
	if st.Runtime == "podman" {
		cli = "podman"
	}
	_ = exec.CommandContext(ctx, cli, "stop", st.PostgresName).Run()
	return nil
}

func (b *AteLocalBackend) Delete(ctx context.Context, name string) error {
	clean := sanitizeEnvName(name)
	stateDir, err := AteLocalStateDir(clean)
	if err != nil {
		return err
	}
	st, err := loadAteLocalState(stateDir)
	if err != nil {
		return err
	}
	if st != nil {
		_ = b.Stop(ctx, clean)
		cli := "container"
		if st.Runtime == "podman" {
			cli = "podman"
		}
		if cli == "podman" {
			_ = exec.CommandContext(ctx, "podman", "rm", "--force", st.PostgresName).Run()
		} else {
			_ = exec.CommandContext(ctx, "container", "delete", "--force", st.PostgresName).Run()
		}
		removeActorContainers(ctx, cli, st.ContainerPrefix+"-")
	}
	_ = tunnel.DeleteLocalEndpoint(clean)
	if err := os.RemoveAll(stateDir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func removeActorContainers(ctx context.Context, cli, prefix string) {
	if prefix == "" || prefix == "-" {
		return
	}
	if cli == "podman" {
		out, err := exec.CommandContext(ctx, "podman", "ps", "-a", "--format", "{{.Names}}").Output()
		if err != nil {
			return
		}
		for _, line := range strings.Split(string(out), "\n") {
			cname := strings.TrimSpace(line)
			if strings.HasPrefix(cname, prefix) {
				_ = exec.CommandContext(ctx, "podman", "rm", "--force", cname).Run()
			}
		}
		return
	}
	out, err := exec.CommandContext(ctx, "container", "list", "--all", "--format", "json").Output()
	if err != nil {
		return
	}
	var items []struct {
		Configuration struct {
			ID string `json:"id"`
		} `json:"configuration"`
	}
	if err := json.Unmarshal(out, &items); err != nil {
		return
	}
	for _, it := range items {
		if strings.HasPrefix(it.Configuration.ID, prefix) {
			_ = exec.CommandContext(ctx, "container", "delete", "--force", it.Configuration.ID).Run()
		}
	}
}
