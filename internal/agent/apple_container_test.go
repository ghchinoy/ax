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
	"context"
	"io"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/proto"
	"google.golang.org/grpc"
)

type mockContainerRunner struct {
	mu          sync.Mutex
	runCalls    [][]string
	execCalls   [][]string
	execOutputs string
	execErr     error
	runOutputs  map[string]string
	runErr      error
}

func (m *mockContainerRunner) Run(ctx context.Context, args ...string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runCalls = append(m.runCalls, args)
	if m.runErr != nil {
		return "", m.runErr
	}
	cmdKey := strings.Join(args, " ")
	if out, ok := m.runOutputs[cmdKey]; ok {
		return out, nil
	}
	return "ok", nil
}

func (m *mockContainerRunner) Exec(ctx context.Context, stdin io.Reader, stdout io.Writer, stderr io.Writer, args ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execCalls = append(m.execCalls, args)
	if m.execOutputs != "" && stdout != nil {
		_, _ = stdout.Write([]byte(m.execOutputs))
	}
	return m.execErr
}

func TestAppleContainerAgent_ConfigDefaults(t *testing.T) {
	_, err := NewAppleContainerAgent(AppleContainerAgentConfig{})
	if err == nil {
		t.Fatal("expected error for empty image")
	}

	agent1, err := NewAppleContainerAgent(AppleContainerAgentConfig{
		Image: "debian:12",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent1.cfg.Mode != "sandbox" {
		t.Errorf("expected default mode 'sandbox', got %s", agent1.cfg.Mode)
	}

	agent2, err := NewAppleContainerAgent(AppleContainerAgentConfig{
		Image: "ax-agent:latest",
		Port:  50051,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if agent2.cfg.Mode != "service" {
		t.Errorf("expected default mode 'service' when port is set, got %s", agent2.cfg.Mode)
	}
}

func TestAppleContainerAgent_SandboxRun(t *testing.T) {
	mockRunner := &mockContainerRunner{
		execOutputs: "Hello from inside container\nLine 2\n",
	}

	agent, err := NewAppleContainerAgent(AppleContainerAgentConfig{
		ID:      "test-sandbox",
		Image:   "debian:12",
		Mode:    "sandbox",
		WorkDir: "/workspace",
		Env: map[string]string{
			"TEST_VAR": "value123",
		},
		CPUs:   2,
		Memory: "512M",
		Runner: mockRunner,
	})
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}

	var outputLines []string
	outputHandler := func(outgoing *proto.AgentOutputs) error {
		for _, m := range outgoing.Messages {
			if txt := m.GetContent().GetText(); txt != nil {
				outputLines = append(outputLines, txt.Text)
			}
		}
		return nil
	}

	start := &proto.AgentStart{
		AgentId: "test-sandbox",
		Messages: []*proto.Message{
			{
				Role: "user",
				Content: &proto.Content{
					Type: &proto.Content_Text{
						Text: &proto.TextContent{Text: "run something"},
					},
				},
			},
		},
	}

	err = agent.Connect(context.Background(), "conv-123", "exec-456", start, nil, outputHandler)
	if err != nil {
		t.Fatalf("Connect failed: %v", err)
	}

	// Verify runner received expected flags
	mockRunner.mu.Lock()
	if len(mockRunner.execCalls) != 1 {
		t.Fatalf("expected 1 exec call, got %d", len(mockRunner.execCalls))
	}
	args := mockRunner.execCalls[0]
	mockRunner.mu.Unlock()

	cmdStr := strings.Join(args, " ")
	if !strings.Contains(cmdStr, "run --progress none --rm") {
		t.Errorf("expected 'run --progress none --rm' in args, got: %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "-e TEST_VAR=value123") {
		t.Errorf("expected env var in args, got: %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "-w /workspace") {
		t.Errorf("expected workdir in args, got: %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "--cpus 2") {
		t.Errorf("expected cpus in args, got: %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "--memory 512M") {
		t.Errorf("expected memory in args, got: %s", cmdStr)
	}
	if !strings.Contains(cmdStr, "debian:12") {
		t.Errorf("expected image in args, got: %s", cmdStr)
	}

	// Verify outputs streamed
	if len(outputLines) != 2 {
		t.Fatalf("expected 2 output lines, got %d: %v", len(outputLines), outputLines)
	}
	if outputLines[0] != "Hello from inside container" || outputLines[1] != "Line 2" {
		t.Errorf("unexpected output lines: %v", outputLines)
	}
}

func TestAppleContainerAgent_ServiceMode(t *testing.T) {
	// Start a mock gRPC server representing the agent running in the container
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	port := lis.Addr().(*net.TCPAddr).Port

	srv := grpc.NewServer()
	defer srv.Stop()

	mockServer := &mockAgentServiceServer{
		response: []*proto.AgentResponse{
			{
				ConversationId: "conv-svc",
				ExecId:         "exec-svc",
				Type: &proto.AgentResponse_Outputs{
					Outputs: &proto.AgentOutputs{
						Messages: []*proto.Message{
							{
								Role: "assistant",
								Content: &proto.Content{
									Type: &proto.Content_Text{
										Text: &proto.TextContent{Text: "Service response from container"},
									},
								},
							},
						},
					},
				},
			},
			{
				ConversationId: "conv-svc",
				ExecId:         "exec-svc",
				Type: &proto.AgentResponse_End{
					End: &proto.AgentEnd{},
				},
			},
		},
	}
	proto.RegisterAgentServiceServer(srv, mockServer)

	go func() {
		_ = srv.Serve(lis)
	}()

	mockRunner := &mockContainerRunner{}

	agent, err := NewAppleContainerAgent(AppleContainerAgentConfig{
		ID:           "test-service-agent",
		Image:        "ax-agent:v1",
		Mode:         "service",
		Port:         50051,
		HostPort:     port, // use the real mock server port so gRPC dial succeeds
		ReadyTimeout: 5 * time.Second,
		Runner:       mockRunner,
	})
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}

	var outputReceived string
	outputHandler := func(outgoing *proto.AgentOutputs) error {
		for _, m := range outgoing.Messages {
			if txt := m.GetContent().GetText(); txt != nil {
				outputReceived = txt.Text
			}
		}
		return nil
	}

	start := &proto.AgentStart{
		AgentId: "test-service-agent",
		Messages: []*proto.Message{
			{
				Role: "user",
				Content: &proto.Content{
					Type: &proto.Content_Text{
						Text: &proto.TextContent{Text: "Hello service"},
					},
				},
			},
		},
	}

	err = agent.Connect(context.Background(), "conv-svc", "exec-svc", start, nil, outputHandler)
	if err != nil {
		t.Fatalf("Connect service mode failed: %v", err)
	}

	if outputReceived != "Service response from container" {
		t.Errorf("expected 'Service response from container', got %q", outputReceived)
	}

	// Verify container start & cleanup occurred
	mockRunner.mu.Lock()
	defer mockRunner.mu.Unlock()

	var started, stopped, deleted bool
	for _, call := range mockRunner.runCalls {
		callStr := strings.Join(call, " ")
		if strings.Contains(callStr, "run --progress none -d") {
			started = true
		}
		if strings.Contains(callStr, "stop --time 2") {
			stopped = true
		}
		if strings.Contains(callStr, "delete --force") {
			deleted = true
		}
	}

	if !started {
		t.Errorf("expected container run call")
	}
	if !stopped {
		t.Errorf("expected container stop call")
	}
	if !deleted {
		t.Errorf("expected container delete call")
	}
}

func TestAppleContainerAgent_HarnessInterface(t *testing.T) {
	mockRunner := &mockContainerRunner{
		execOutputs: "Harness execution output\n",
	}

	agent, err := NewAppleContainerAgent(AppleContainerAgentConfig{
		ID:     "test-harness",
		Image:  "debian:12",
		Mode:   "sandbox",
		Runner: mockRunner,
	})
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}

	execSession, err := agent.Start(context.Background(), "conv-harness")
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if execSession.ID() == "" {
		t.Errorf("expected non-empty execution ID")
	}

	err = execSession.Queue(context.Background(), &proto.Message{
		Role: "user",
		Content: &proto.Content{
			Type: &proto.Content_Text{
				Text: &proto.TextContent{Text: "harness input"},
			},
		},
	})
	if err != nil {
		t.Fatalf("Queue failed: %v", err)
	}

	var handlerMessages []string
	var completed bool
	mockHandler := &testHarnessHandler{
		onMessage: func(msg *proto.Message) {
			if txt := msg.GetContent().GetText(); txt != nil {
				handlerMessages = append(handlerMessages, txt.Text)
			}
		},
		onComplete: func() {
			completed = true
		},
	}

	err = execSession.Run(context.Background(), mockHandler)
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	if !completed {
		t.Errorf("expected handler.OnComplete to be called")
	}
	if len(handlerMessages) != 1 || handlerMessages[0] != "Harness execution output" {
		t.Errorf("unexpected handler messages: %v", handlerMessages)
	}

	_ = execSession.Close(context.Background())
}

type testHarnessHandler struct {
	onMessage  func(msg *proto.Message)
	onComplete func()
}

func (h *testHarnessHandler) OnMessage(ctx context.Context, execID string, msg *proto.Message) error {
	if h.onMessage != nil {
		h.onMessage(msg)
	}
	return nil
}

func (h *testHarnessHandler) OnComplete(ctx context.Context, execID string) error {
	if h.onComplete != nil {
		h.onComplete()
	}
	return nil
}

// Live test: runs only when container CLI is installed and responsive
func TestAppleContainerAgent_LiveCLI(t *testing.T) {
	if _, err := exec.LookPath("container"); err != nil {
		t.Skip("container CLI not available in PATH, skipping live test")
	}

	// Verify container CLI works
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "container", "list")
	if err := cmd.Run(); err != nil {
		t.Skipf("container daemon not responsive: %v, skipping live test", err)
	}

	agent, err := NewAppleContainerAgent(AppleContainerAgentConfig{
		ID:      "live-test-agent",
		Image:   "debian:12",
		Mode:    "sandbox",
		Command: []string{"echo", "live apple container test success"},
	})
	if err != nil {
		t.Fatalf("failed to create agent: %v", err)
	}
	defer agent.Close()

	var output string
	outputHandler := func(outgoing *proto.AgentOutputs) error {
		for _, m := range outgoing.Messages {
			if txt := m.GetContent().GetText(); txt != nil {
				output += txt.Text
			}
		}
		return nil
	}

	start := &proto.AgentStart{
		AgentId: "live-test-agent",
		Messages: []*proto.Message{
			{
				Role: "user",
				Content: &proto.Content{
					Type: &proto.Content_Text{
						Text: &proto.TextContent{Text: ""},
					},
				},
			},
		},
	}

	execCtx, execCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer execCancel()
	err = agent.Connect(execCtx, "live-conv", "live-exec", start, nil, outputHandler)
	if err != nil {
		t.Fatalf("Live container execution failed: %v", err)
	}

	if !strings.Contains(output, "live apple container test success") {
		t.Errorf("expected 'live apple container test success' in output, got: %q", output)
	}
}
