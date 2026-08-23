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

package agent

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/ax/internal/harness"
	"github.com/google/ax/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ContainerRunner abstracts command execution of the Apple container CLI.
// This allows mocking container operations in unit tests.
type ContainerRunner interface {
	Run(ctx context.Context, args ...string) (string, error)
	Exec(ctx context.Context, stdin io.Reader, stdout io.Writer, stderr io.Writer, args ...string) error
}

type defaultContainerRunner struct {
	binaryPath string
}

func (r *defaultContainerRunner) getBinary() string {
	if r.binaryPath != "" {
		return r.binaryPath
	}
	if p, err := exec.LookPath("container"); err == nil {
		return p
	}
	return "/usr/local/bin/container"
}

func (r *defaultContainerRunner) Run(ctx context.Context, args ...string) (string, error) {
	bin := r.getBinary()
	cmd := exec.CommandContext(ctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("container %s failed: %w (stderr: %s)", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func (r *defaultContainerRunner) Exec(ctx context.Context, stdin io.Reader, stdout io.Writer, stderr io.Writer, args ...string) error {
	bin := r.getBinary()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("container %s failed: %w", strings.Join(args, " "), err)
	}
	return nil
}

// AppleContainerAgentConfig defines configuration for an Apple Container Sandbox Agent.
type AppleContainerAgentConfig struct {
	ID           string            `yaml:"id"`
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Image        string            `yaml:"image"`
	Mode         string            `yaml:"mode,omitempty"`        // "service" (default if Port > 0) or "sandbox" / "exec"
	Port         int               `yaml:"port,omitempty"`        // Container gRPC port for service mode (e.g. 50051)
	HostPort     int               `yaml:"host_port,omitempty"`   // Host port to map (0 for auto-allocated dynamic port)
	Command      []string          `yaml:"command,omitempty"`     // Command / entrypoint arguments
	WorkDir      string            `yaml:"work_dir,omitempty"`    // Working directory in container
	Env          map[string]string `yaml:"env,omitempty"`         // Environment variables
	Volumes      []string          `yaml:"volumes,omitempty"`     // Volume mappings (e.g. "hostPath:containerPath")
	CPUs         int               `yaml:"cpus,omitempty"`        // CPUs allocation
	Memory       string            `yaml:"memory,omitempty"`      // Memory limit (e.g. "1024M", "2G")
	KeepAlive    bool              `yaml:"keep_alive,omitempty"`  // Preserve container across turns in a conversation
	BinaryPath   string            `yaml:"binary_path,omitempty"` // Path to container CLI binary
	ReadyTimeout time.Duration     `yaml:"ready_timeout,omitempty"`
	Metadata     map[string]string `yaml:"metadata,omitempty"`

	Runner ContainerRunner `yaml:"-"` // Custom runner for testing
}

// AppleContainerAgent runs an AX agent or code execution sandbox in an Apple container.
// It implements both agent.Agent and harness.Harness interfaces.
type AppleContainerAgent struct {
	cfg    AppleContainerAgentConfig
	runner ContainerRunner

	mu              sync.Mutex
	activeInstances map[string]*containerInstance
}

type containerInstance struct {
	name     string
	hostPort int
	address  string
	mode     string
}

// NewAppleContainerAgent creates a new AppleContainerAgent.
func NewAppleContainerAgent(cfg AppleContainerAgentConfig) (*AppleContainerAgent, error) {
	if cfg.Image == "" {
		return nil, errors.New("container image cannot be empty")
	}

	mode := strings.ToLower(strings.TrimSpace(cfg.Mode))
	if mode == "" {
		if cfg.Port > 0 {
			mode = "service"
		} else {
			mode = "sandbox"
		}
	}
	cfg.Mode = mode

	if cfg.ReadyTimeout == 0 {
		cfg.ReadyTimeout = 30 * time.Second
	}

	runner := cfg.Runner
	if runner == nil {
		runner = &defaultContainerRunner{binaryPath: cfg.BinaryPath}
	}

	return &AppleContainerAgent{
		cfg:             cfg,
		runner:          runner,
		activeInstances: make(map[string]*containerInstance),
	}, nil
}

// Connect implements agent.Agent.
func (a *AppleContainerAgent) Connect(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, e Executor, o OutputHandler) error {
	switch a.cfg.Mode {
	case "service":
		return a.connectService(ctx, conversationID, execID, start, e, o)
	case "sandbox", "exec":
		return a.connectSandbox(ctx, conversationID, execID, start, o)
	default:
		return fmt.Errorf("unknown apple container mode: %s", a.cfg.Mode)
	}
}

func (a *AppleContainerAgent) connectService(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, e Executor, o OutputHandler) error {
	instanceKey := a.instanceKey(conversationID, execID)

	inst, err := a.getOrCreateInstance(ctx, instanceKey, "service")
	if err != nil {
		return err
	}

	// If not keep-alive, ensure container cleanup after Connect finishes.
	if !a.cfg.KeepAlive {
		defer a.stopInstance(instanceKey)
	}

	// Dial the remote agent running in the container.
	remoteAgent, err := NewRemoteAgent(RemoteAgentConfig{
		Address:    inst.address,
		Reconnect:  true,
		MaxRetries: 3,
		DialOpts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to create remote agent for container %s: %w", inst.name, err)
	}
	defer remoteAgent.Close()

	return remoteAgent.Connect(ctx, conversationID, execID, start, e, o)
}

func (a *AppleContainerAgent) connectSandbox(ctx context.Context, conversationID string, execID string, start *proto.AgentStart, o OutputHandler) error {
	userText := extractLastUserText(start.GetMessages())
	containerName := a.containerName(conversationID, execID)

	if a.cfg.KeepAlive {
		instanceKey := a.instanceKey(conversationID, execID)
		inst, err := a.getOrCreateInstance(ctx, instanceKey, "sandbox")
		if err != nil {
			return err
		}
		containerName = inst.name
		return a.execInRunningContainer(ctx, containerName, userText, o)
	}

	// Ephemeral execution: run single-shot container
	args := []string{"run", "--progress", "none", "--rm", "--name", containerName}
	for k, v := range a.cfg.Env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}
	for _, vol := range a.cfg.Volumes {
		args = append(args, "-v", vol)
	}
	if a.cfg.WorkDir != "" {
		args = append(args, "-w", a.cfg.WorkDir)
	}
	if a.cfg.CPUs > 0 {
		args = append(args, "--cpus", fmt.Sprintf("%d", a.cfg.CPUs))
	}
	if a.cfg.Memory != "" {
		args = append(args, "--memory", a.cfg.Memory)
	}
	args = append(args, a.cfg.Image)
	if len(a.cfg.Command) > 0 {
		args = append(args, a.cfg.Command...)
	}
	if userText != "" {
		args = append(args, userText)
	}

	var stdin io.Reader
	if len(a.cfg.Command) == 0 && userText != "" {
		stdin = strings.NewReader(userText)
	}

	var stdoutBuf, stderrBuf bytes.Buffer
	var stdoutWriter io.Writer = &stdoutBuf
	var wg sync.WaitGroup
	if o != nil {
		// Use a pipe to stream stdout line-by-line
		pr, pw := io.Pipe()
		stdoutWriter = pw
		wg.Add(1)

		go func() {
			defer wg.Done()
			defer pr.Close()
			scanner := bufio.NewScanner(pr)
			for scanner.Scan() {
				line := scanner.Text()
				_ = o(&proto.AgentOutputs{
					Messages: []*proto.Message{{
						Role: "assistant",
						Content: &proto.Content{
							Type: &proto.Content_Text{
								Text: &proto.TextContent{Text: line},
							},
						},
					}},
				})
			}
		}()
	}

	err := a.runner.Exec(ctx, stdin, stdoutWriter, &stderrBuf, args...)
	if closer, ok := stdoutWriter.(io.Closer); ok {
		_ = closer.Close()
	}
	wg.Wait()

	if err != nil {
		return fmt.Errorf("sandbox run failed: %w (stderr: %s)", err, strings.TrimSpace(stderrBuf.String()))
	}
	return nil
}

func (a *AppleContainerAgent) execInRunningContainer(ctx context.Context, containerName string, userText string, o OutputHandler) error {
	args := []string{"exec"}
	for k, v := range a.cfg.Env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", k, v))
	}
	if a.cfg.WorkDir != "" {
		args = append(args, "-w", a.cfg.WorkDir)
	}
	args = append(args, containerName)
	if len(a.cfg.Command) > 0 {
		args = append(args, a.cfg.Command...)
	}
	if userText != "" {
		args = append(args, userText)
	}

	pr, pw := io.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		defer pr.Close()
		scanner := bufio.NewScanner(pr)
		for scanner.Scan() {
			line := scanner.Text()
			if o != nil {
				_ = o(&proto.AgentOutputs{
					Messages: []*proto.Message{{
						Role: "assistant",
						Content: &proto.Content{
							Type: &proto.Content_Text{
								Text: &proto.TextContent{Text: line},
							},
						},
					}},
				})
			}
		}
	}()

	var stderrBuf bytes.Buffer
	err := a.runner.Exec(ctx, nil, pw, &stderrBuf, args...)
	_ = pw.Close()
	wg.Wait()

	if err != nil {
		return fmt.Errorf("container exec failed: %w (stderr: %s)", err, strings.TrimSpace(stderrBuf.String()))
	}
	return nil
}

func (a *AppleContainerAgent) getOrCreateInstance(ctx context.Context, key string, mode string) (*containerInstance, error) {
	a.mu.Lock()
	inst, exists := a.activeInstances[key]
	if exists {
		a.mu.Unlock()
		return inst, nil
	}
	a.mu.Unlock()

	containerName := sanitizeContainerName(fmt.Sprintf("ax-%s-%s", a.cfg.ID, key))

	var hostPort int
	var runArgs []string
	runArgs = append(runArgs, "run", "--progress", "none", "-d", "--name", containerName)

	if mode == "service" {
		hostPort = a.cfg.HostPort
		if hostPort == 0 {
			var err error
			hostPort, err = allocateFreePort()
			if err != nil {
				return nil, fmt.Errorf("failed to allocate free port: %w", err)
			}
		}
		targetPort := a.cfg.Port
		if targetPort == 0 {
			targetPort = 50051
		}
		runArgs = append(runArgs, "-p", fmt.Sprintf("%d:%d", hostPort, targetPort))
	}

	for k, v := range a.cfg.Env {
		runArgs = append(runArgs, "-e", fmt.Sprintf("%s=%s", k, v))
	}
	for _, vol := range a.cfg.Volumes {
		runArgs = append(runArgs, "-v", vol)
	}
	if a.cfg.WorkDir != "" {
		runArgs = append(runArgs, "-w", a.cfg.WorkDir)
	}
	if a.cfg.CPUs > 0 {
		runArgs = append(runArgs, "--cpus", fmt.Sprintf("%d", a.cfg.CPUs))
	}
	if a.cfg.Memory != "" {
		runArgs = append(runArgs, "--memory", a.cfg.Memory)
	}

	runArgs = append(runArgs, a.cfg.Image)
	if len(a.cfg.Command) > 0 {
		runArgs = append(runArgs, a.cfg.Command...)
	} else if mode == "sandbox" {
		// Keep sandbox container alive for interactive exec
		runArgs = append(runArgs, "sleep", "infinity")
	}

	_, err := a.runner.Run(ctx, runArgs...)
	if err != nil {
		return nil, fmt.Errorf("failed to start container %s: %w", containerName, err)
	}

	inst = &containerInstance{
		name:     containerName,
		hostPort: hostPort,
		address:  fmt.Sprintf("127.0.0.1:%d", hostPort),
		mode:     mode,
	}

	if mode == "service" {
		// Wait for service port readiness
		if err := a.waitForPortReady(ctx, inst.address, a.cfg.ReadyTimeout); err != nil {
			_ = a.stopContainer(context.Background(), containerName)
			return nil, fmt.Errorf("container service at %s not ready: %w", inst.address, err)
		}
	}

	a.mu.Lock()
	a.activeInstances[key] = inst
	a.mu.Unlock()

	return inst, nil
}

func (a *AppleContainerAgent) waitForPortReady(ctx context.Context, address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := net.DialTimeout("tcp", address, 500*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for port %s to open after %v", address, timeout)
}

func (a *AppleContainerAgent) stopInstance(key string) {
	a.mu.Lock()
	inst, exists := a.activeInstances[key]
	if exists {
		delete(a.activeInstances, key)
	}
	a.mu.Unlock()

	if exists && inst != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = a.stopContainer(ctx, inst.name)
	}
}

func (a *AppleContainerAgent) stopContainer(ctx context.Context, containerName string) error {
	_, _ = a.runner.Run(ctx, "stop", "--time", "2", containerName)
	_, err := a.runner.Run(ctx, "delete", "--force", containerName)
	return err
}

func (a *AppleContainerAgent) instanceKey(conversationID, execID string) string {
	if a.cfg.KeepAlive && conversationID != "" {
		return sanitizeContainerName(conversationID)
	}
	if execID != "" {
		return sanitizeContainerName(execID)
	}
	return sanitizeContainerName(fmt.Sprintf("%d", time.Now().UnixNano()))
}

func (a *AppleContainerAgent) containerName(conversationID, execID string) string {
	key := a.instanceKey(conversationID, execID)
	return sanitizeContainerName(fmt.Sprintf("ax-%s-%s", a.cfg.ID, key))
}

// Close gracefully stops all active container instances.
func (a *AppleContainerAgent) Close() error {
	a.mu.Lock()
	instances := make([]*containerInstance, 0, len(a.activeInstances))
	for _, inst := range a.activeInstances {
		instances = append(instances, inst)
	}
	a.activeInstances = make(map[string]*containerInstance)
	a.mu.Unlock()

	var firstErr error
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	for _, inst := range instances {
		if err := a.stopContainer(ctx, inst.name); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Start implements harness.Harness.
func (a *AppleContainerAgent) Start(ctx context.Context, conversationID string) (harness.Execution, error) {
	execID := fmt.Sprintf("exec-%d", time.Now().UnixNano())
	return &appleContainerExecution{
		agent:          a,
		conversationID: conversationID,
		execID:         execID,
	}, nil
}

type appleContainerExecution struct {
	agent          *AppleContainerAgent
	conversationID string
	execID         string
	queuedMessages []*proto.Message
	mu             sync.Mutex
}

func (e *appleContainerExecution) ID() string {
	return e.execID
}

func (e *appleContainerExecution) Queue(ctx context.Context, msg ...*proto.Message) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.queuedMessages = append(e.queuedMessages, msg...)
	return nil
}

func (e *appleContainerExecution) Run(ctx context.Context, handler harness.Handler) error {
	e.mu.Lock()
	messages := e.queuedMessages
	e.queuedMessages = nil
	e.mu.Unlock()

	start := &proto.AgentStart{
		AgentId:  e.agent.cfg.ID,
		Messages: messages,
	}

	outputHandler := func(outgoing *proto.AgentOutputs) error {
		if handler != nil && outgoing != nil {
			for _, m := range outgoing.Messages {
				if err := handler.OnMessage(ctx, e.execID, m); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := e.agent.Connect(ctx, e.conversationID, e.execID, start, nil, outputHandler); err != nil {
		return err
	}

	if handler != nil {
		return handler.OnComplete(ctx, e.execID)
	}
	return nil
}

func (e *appleContainerExecution) Close(ctx context.Context) error {
	e.agent.stopInstance(e.agent.instanceKey(e.conversationID, e.execID))
	return nil
}

var nonAlphanumeric = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

func sanitizeContainerName(name string) string {
	s := nonAlphanumeric.ReplaceAllString(name, "-")
	s = strings.Trim(s, "-._")
	if len(s) > 48 {
		s = s[:48]
	}
	if s == "" {
		s = fmt.Sprintf("ax-%d", time.Now().UnixNano())
	}
	return s
}

func extractLastUserText(messages []*proto.Message) string {
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if msg.Role == "user" {
			if txt := msg.GetContent().GetText(); txt != nil {
				return txt.Text
			}
		}
	}
	return ""
}

func allocateFreePort() (int, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port, nil
}
