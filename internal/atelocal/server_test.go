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

package atelocal

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type mockRunner struct {
	mu         sync.Mutex
	runCalls   [][]string
	inspectMap map[string]*ContainerInspectResult
	runErr     error
	inspectErr error
	created    map[string]bool
}

func (m *mockRunner) Run(ctx context.Context, args ...string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runCalls = append(m.runCalls, args)
	if len(args) > 0 && args[0] == "run" {
		if m.created == nil {
			m.created = make(map[string]bool)
		}
		for i, a := range args {
			if a == "--name" && i+1 < len(args) {
				m.created[args[i+1]] = true
			}
		}
	}
	if m.runErr != nil {
		return "", m.runErr
	}
	return "ok", nil
}

func (m *mockRunner) Inspect(ctx context.Context, containerName string) (*ContainerInspectResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.inspectErr != nil {
		return nil, m.inspectErr
	}
	if m.created != nil && !m.created[containerName] {
		return nil, status.Errorf(codes.NotFound, "container not found")
	}
	if res, ok := m.inspectMap[containerName]; ok {
		return res, nil
	}
	// Default mock inspect
	res := &ContainerInspectResult{
		ID: containerName,
	}
	res.Status.State = "running"
	res.Status.Networks = []struct {
		Hostname    string `json:"hostname"`
		IPv4Address string `json:"ipv4Address"`
		IPv4Gateway string `json:"ipv4Gateway"`
	}{
		{Hostname: containerName, IPv4Address: "192.168.64.42/24"},
	}
	return res, nil
}

func startTestControlServer(t *testing.T, cfg *Config) (ateapipb.ControlClient, func()) {
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv.Register(grpcServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}

	client := ateapipb.NewControlClient(conn)

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = lis.Close()
		_ = srv.Close(context.Background())
	}

	return client, cleanup
}

func TestControlServer_AtespaceLifecycle(t *testing.T) {
	runner := &mockRunner{created: make(map[string]bool)}
	cfg := DefaultConfig()
	cfg.Runner = runner

	client, cleanup := startTestControlServer(t, cfg)
	defer cleanup()

	ctx := context.Background()

	// 1. Create atespace
	createResp, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{Name: "test-space"})
	if err != nil {
		t.Fatalf("CreateAtespace failed: %v", err)
	}
	if createResp.GetAtespace().GetName() != "test-space" {
		t.Errorf("expected atespace name 'test-space', got %q", createResp.GetAtespace().GetName())
	}

	// 2. Get atespace
	getResp, err := client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Name: "test-space"})
	if err != nil {
		t.Fatalf("GetAtespace failed: %v", err)
	}
	if getResp.GetAtespace().GetName() != "test-space" {
		t.Errorf("expected name 'test-space', got %q", getResp.GetAtespace().GetName())
	}

	// 3. Delete atespace
	_, err = client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{Name: "test-space"})
	if err != nil {
		t.Fatalf("DeleteAtespace failed: %v", err)
	}

	// 4. Get after delete should return NotFound
	_, err = client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{Name: "test-space"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after delete, got %v", err)
	}
}

func TestControlServer_ActorLifecycle(t *testing.T) {
	// Start a mock listener for the port readiness check
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on test port: %v", err)
	}
	defer lis.Close()
	testPort := lis.Addr().(*net.TCPAddr).Port

	runner := &mockRunner{
		created: make(map[string]bool),
		inspectMap: map[string]*ContainerInspectResult{
			"ate-ax-my-actor": {
				ID: "ate-ax-my-actor",
				Status: struct {
					State    string `json:"state"`
					Networks []struct {
						Hostname    string `json:"hostname"`
						IPv4Address string `json:"ipv4Address"`
						IPv4Gateway string `json:"ipv4Gateway"`
					} `json:"networks"`
				}{
					State: "running",
					Networks: []struct {
						Hostname    string `json:"hostname"`
						IPv4Address string `json:"ipv4Address"`
						IPv4Gateway string `json:"ipv4Gateway"`
					}{
						{Hostname: "ate-ax-my-actor", IPv4Address: "127.0.0.1/32"},
					},
				},
			},
		},
	}

	cfg := DefaultConfig()
	cfg.Runner = runner
	cfg.ReadyTimeout = 2 * time.Second
	cfg.Templates["custom-template"] = TemplateConfig{
		Image: "ax-harness-worker:latest",
		Port:  testPort,
		Env:   map[string]string{"ENV_KEY": "ENV_VAL"},
	}

	client, cleanup := startTestControlServer(t, cfg)
	defer cleanup()

	ctx := context.Background()

	// 1. CreateActor
	createResp, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: "ax", Name: "my-actor"},
		ActorTemplateNamespace: "ax",
		ActorTemplateName:      "custom-template",
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}
	if createResp.GetActor().GetActorId() != "my-actor" {
		t.Errorf("expected actor ID 'my-actor', got %q", createResp.GetActor().GetActorId())
	}
	if createResp.GetActor().GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("expected status SUSPENDED on create, got %v", createResp.GetActor().GetStatus())
	}

	// 2. Duplicate CreateActor should fail with AlreadyExists
	_, err = client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: "my-actor"},
	})
	if status.Code(err) != codes.AlreadyExists {
		t.Errorf("expected AlreadyExists on duplicate create, got %v", err)
	}

	// 3. ResumeActor
	resumeResp, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: "my-actor"},
	})
	if err != nil {
		t.Fatalf("ResumeActor failed: %v", err)
	}
	actor := resumeResp.GetActor()
	if actor.GetAteomPodIp() != "127.0.0.1" {
		t.Errorf("expected AteomPodIp '127.0.0.1', got %q", actor.GetAteomPodIp())
	}
	if actor.GetStatus() != ateapipb.Actor_STATUS_RUNNING {
		t.Errorf("expected status RUNNING on resume, got %v", actor.GetStatus())
	}

	// 4. SuspendActor
	suspendResp, err := client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: "my-actor"},
	})
	if err != nil {
		t.Fatalf("SuspendActor failed: %v", err)
	}
	if suspendResp.GetActor().GetStatus() != ateapipb.Actor_STATUS_SUSPENDED {
		t.Errorf("expected status SUSPENDED on suspend, got %v", suspendResp.GetActor().GetStatus())
	}

	// 5. DeleteActor
	_, err = client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: "my-actor"},
	})
	if err != nil {
		t.Fatalf("DeleteActor failed: %v", err)
	}

	// 6. GetActor after delete should fail with NotFound
	_, err = client.GetActor(ctx, &ateapipb.GetActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: "my-actor"},
	})
	if status.Code(err) != codes.NotFound {
		t.Errorf("expected NotFound after delete, got %v", err)
	}

	// Verify runner calls were made
	runner.mu.Lock()
	defer runner.mu.Unlock()

	var hadRun, hadStop, hadDelete bool
	for _, call := range runner.runCalls {
		cmdStr := strings.Join(call, " ")
		if strings.Contains(cmdStr, "run --progress none -d") {
			hadRun = true
		}
		if strings.Contains(cmdStr, "stop --time 2") {
			hadStop = true
		}
		if strings.Contains(cmdStr, "delete --force") {
			hadDelete = true
		}
	}

	if !hadRun {
		t.Errorf("expected container run call")
	}
	if !hadStop {
		t.Errorf("expected container stop call")
	}
	if !hadDelete {
		t.Errorf("expected container delete call")
	}
}

func TestCleanIPv4(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"192.168.64.2/24", "192.168.64.2"},
		{"127.0.0.1", "127.0.0.1"},
		{" 10.0.0.1/16 ", "10.0.0.1"},
		{"", ""},
	}
	for _, tc := range tests {
		got := CleanIPv4(tc.input)
		if got != tc.want {
			t.Errorf("CleanIPv4(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// Live test against real Apple container CLI
func TestControlServer_LiveAppleContainer(t *testing.T) {
	if _, err := exec.LookPath("container"); err != nil {
		t.Skip("container CLI not available, skipping live test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := exec.CommandContext(ctx, "container", "list").Run(); err != nil {
		t.Skipf("container daemon not responsive: %v, skipping live test", err)
	}

	cfg := DefaultConfig()
	cfg.ReadyTimeout = 2 * time.Second
	cfg.Templates["live-template"] = TemplateConfig{
		Image:   "debian:12",
		Command: []string{"sleep", "infinity"},
		Port:    50053,
	}

	client, cleanup := startTestControlServer(t, cfg)
	defer cleanup()

	testActorName := fmt.Sprintf("test-live-%d", time.Now().UnixNano()%100000)
	reqCtx, reqCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer reqCancel()

	// 1. Create Actor
	_, err := client.CreateActor(reqCtx, &ateapipb.CreateActorRequest{
		ActorRef:          &ateapipb.ActorRef{Atespace: "ax", Name: testActorName},
		ActorTemplateName: "live-template",
	})
	if err != nil {
		t.Fatalf("Live CreateActor failed: %v", err)
	}

	// 2. Resume Actor (spawns real Apple container and gets IP)
	resumeResp, err := client.ResumeActor(reqCtx, &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: testActorName},
	})
	if err != nil {
		t.Fatalf("Live ResumeActor failed: %v", err)
	}

	actor := resumeResp.GetActor()
	t.Logf("Live Actor resumed in Apple Container: ID=%s, IP=%s, Status=%v", actor.GetActorId(), actor.GetAteomPodIp(), actor.GetStatus())

	if actor.GetAteomPodIp() == "" {
		t.Errorf("expected non-empty AteomPodIp from real container")
	}

	// 3. Suspend Actor
	_, err = client.SuspendActor(reqCtx, &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: testActorName},
	})
	if err != nil {
		t.Fatalf("Live SuspendActor failed: %v", err)
	}

	// 4. Clean up / Delete Actor
	_, err = client.DeleteActor(reqCtx, &ateapipb.DeleteActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: "ax", Name: testActorName},
	})
	if err != nil {
		t.Fatalf("Live DeleteActor failed: %v", err)
	}
}
