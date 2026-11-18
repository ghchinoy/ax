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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/ax/internal/controller"
	"github.com/google/ax/internal/guest"
	"github.com/google/ax/internal/substrate"
	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestControlServer_AtespaceLifecycle(t *testing.T) {
	runner := &mockRunner{created: make(map[string]bool)}
	cfg := DefaultConfig()
	cfg.Runner = runner

	client, cleanup := startTestControlServer(t, cfg)
	defer cleanup()

	ctx := t.Context()
	spaceName := "test-space"

	// 1. Create atespace
	createResp, err := client.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: spaceName},
		},
	})
	if err != nil {
		t.Fatalf("CreateAtespace(%q) failed: %v", spaceName, err)
	}
	if got := createResp.GetMetadata().GetName(); got != spaceName {
		t.Errorf("CreateAtespace(%q).Metadata.Name = %q, want %q", spaceName, got, spaceName)
	}

	// 2. Get atespace
	getResp, err := client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: spaceName},
	})
	if err != nil {
		t.Fatalf("GetAtespace(%q) failed: %v", spaceName, err)
	}
	if got := getResp.GetMetadata().GetName(); got != spaceName {
		t.Errorf("GetAtespace(%q).Metadata.Name = %q, want %q", spaceName, got, spaceName)
	}

	// 3. Delete atespace
	_, err = client.DeleteAtespace(ctx, &ateapipb.DeleteAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: spaceName},
	})
	if err != nil {
		t.Fatalf("DeleteAtespace(%q) failed: %v", spaceName, err)
	}

	// 4. Get after delete should return NotFound
	_, err = client.GetAtespace(ctx, &ateapipb.GetAtespaceRequest{
		Atespace: &ateapipb.ObjectRef{Name: spaceName},
	})
	if gotCode := status.Code(err); gotCode != codes.NotFound {
		t.Errorf("GetAtespace(%q) after delete error code = %v, want %v", spaceName, gotCode, codes.NotFound)
	}
}

func TestControlServer_ActorLifecycle(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer lis.Close()
	testPort := lis.Addr().(*net.TCPAddr).Port

	actorName := "my-actor"
	containerName := "ate-ax-my-actor"
	runner := &mockRunner{
		created: make(map[string]bool),
		inspectMap: map[string]*ContainerInspectResult{
			containerName: {
				ID: containerName,
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
						{Hostname: containerName, IPv4Address: "127.0.0.1/32"},
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

	ctx := t.Context()

	// 1. CreateActor
	createResp, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "ax",
				Name:     actorName,
			},
			ActorTemplate: &ateapipb.ObjectRef{
				Atespace: "ax",
				Name:     "custom-template",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor(%q) failed: %v", actorName, err)
	}
	if got := createResp.GetMetadata().GetName(); got != actorName {
		t.Errorf("CreateActor(%q).Metadata.Name = %q, want %q", actorName, got, actorName)
	}
	if createResp.GetMetadata().GetCreateTime() == nil || createResp.GetMetadata().GetUpdateTime() == nil {
		t.Errorf("CreateActor(%q).Metadata timestamps missing: create=%v update=%v", actorName, createResp.GetMetadata().GetCreateTime(), createResp.GetMetadata().GetUpdateTime())
	}
	if got := createResp.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("CreateActor(%q).Status.State = %v, want %v", actorName, got, ateapipb.ActorState_ACTOR_STATE_SUSPENDED)
	}

	// 2. Duplicate CreateActor should fail with AlreadyExists
	_, err = client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "ax",
				Name:     actorName,
			},
		},
	})
	if gotCode := status.Code(err); gotCode != codes.AlreadyExists {
		t.Errorf("CreateActor(%q) duplicate error code = %v, want %v", actorName, gotCode, codes.AlreadyExists)
	}

	// 3. ResumeActor
	resumeResp, err := client.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: actorName},
	})
	if err != nil {
		t.Fatalf("ResumeActor(%q) failed: %v", actorName, err)
	}
	actor := resumeResp.GetActor()
	workerIP := ""
	if actor.GetStatus() != nil && actor.GetStatus().GetWorkerAssignment() != nil {
		if ips := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); len(ips) > 0 {
			workerIP = ips[0]
		}
	}
	if workerIP != "127.0.0.1" {
		t.Errorf("ResumeActor(%q).Status.WorkerAssignment.WorkerPodIps[0] = %q, want %q", actorName, workerIP, "127.0.0.1")
	}
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Errorf("ResumeActor(%q).Status.State = %v, want %v", actorName, got, ateapipb.ActorState_ACTOR_STATE_RUNNING)
	}

	// 4. SuspendActor
	_, err = client.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: actorName},
	})
	if err != nil {
		t.Fatalf("SuspendActor(%q) failed: %v", actorName, err)
	}

	// 5. DeleteActor
	_, err = client.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: actorName},
	})
	if err != nil {
		t.Fatalf("DeleteActor(%q) failed: %v", actorName, err)
	}

	// 6. GetActor after delete should fail with NotFound
	_, err = client.GetActor(ctx, &ateapipb.GetActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: actorName},
	})
	if gotCode := status.Code(err); gotCode != codes.NotFound {
		t.Errorf("GetActor(%q) after delete error code = %v, want %v", actorName, gotCode, codes.NotFound)
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
		t.Errorf("runner.runCalls missing 'run' command; calls were %v", runner.runCalls)
	}
	if !hadStop {
		t.Errorf("runner.runCalls missing 'stop' command; calls were %v", runner.runCalls)
	}
	if !hadDelete {
		t.Errorf("runner.runCalls missing 'delete' command; calls were %v", runner.runCalls)
	}
}

func TestControlServer_RevertActor(t *testing.T) {
	runner := &mockRunner{created: make(map[string]bool)}
	cfg := DefaultConfig()
	cfg.Runner = runner

	client, cleanup := startTestControlServer(t, cfg)
	defer cleanup()

	ctx := t.Context()
	actorName := "revert-actor"

	// 1. Create Actor
	_, err := client.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "ax",
				Name:     actorName,
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor failed: %v", err)
	}

	// 2. RevertActor returns state SUSPENDED
	revertResp, err := client.RevertActor(ctx, &ateapipb.RevertActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: actorName},
	})
	if err != nil {
		t.Fatalf("RevertActor failed: %v", err)
	}
	if got := revertResp.GetActor().GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("RevertActor state = %v, want SUSPENDED", got)
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

func TestControlServer_TemplateDeduplicationAndEgressPolicy(t *testing.T) {
	runner := &mockRunner{created: make(map[string]bool)}
	cfg := DefaultConfig()
	cfg.Runner = runner

	client, cleanup := startTestControlServer(t, cfg)
	defer cleanup()

	ctx := t.Context()

	// 1. Create an ActorTemplate in atespace "ax"
	_, err := client.CreateActorTemplate(ctx, &ateapipb.CreateActorTemplateRequest{
		ActorTemplate: &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "ax",
				Name:     "task-tmpl-1234",
			},
			Containers: []*ateapipb.Container{{
				Name:  "guest",
				Image: "debian:12",
			}},
		},
	})
	if err != nil {
		t.Fatalf("CreateActorTemplate failed: %v", err)
	}

	// 2. List templates filtered by "ax" -> should return exactly 1 entry without duplicates
	listResp, err := client.ListActorTemplates(ctx, &ateapipb.ListActorTemplatesRequest{
		Atespace: "ax",
	})
	if err != nil {
		t.Fatalf("ListActorTemplates failed: %v", err)
	}
	if len(listResp.GetActorTemplates()) != 1 {
		t.Fatalf("ListActorTemplates(ax) returned %d templates, want 1: %v", len(listResp.GetActorTemplates()), listResp.GetActorTemplates())
	}
	if got := listResp.GetActorTemplates()[0].GetMetadata().GetName(); got != "task-tmpl-1234" {
		t.Errorf("template name = %q, want %q", got, "task-tmpl-1234")
	}

	// 3. Create, Get, Update, Delete EgressPolicy
	actorRef := &ateapipb.ObjectRef{Atespace: "ax", Name: "policy-actor"}
	policy := &ateapipb.EgressPolicy{
		Metadata: &ateapipb.ResourceMetadata{Atespace: "ax", Name: "default"},
	}
	if _, err := client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: policy,
	}); err != nil {
		t.Fatalf("CreateActorEgressPolicy failed: %v", err)
	}

	// Duplicate create should return AlreadyExists
	if _, err := client.CreateActorEgressPolicy(ctx, &ateapipb.CreateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: policy,
	}); status.Code(err) != codes.AlreadyExists {
		t.Errorf("duplicate CreateActorEgressPolicy code = %v, want AlreadyExists", status.Code(err))
	}

	if _, err := client.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actorRef}); err != nil {
		t.Fatalf("GetActorEgressPolicy failed: %v", err)
	}
	if _, err := client.UpdateActorEgressPolicy(ctx, &ateapipb.UpdateActorEgressPolicyRequest{
		Actor:        actorRef,
		EgressPolicy: policy,
	}); err != nil {
		t.Fatalf("UpdateActorEgressPolicy failed: %v", err)
	}
	if _, err := client.DeleteActorEgressPolicy(ctx, &ateapipb.DeleteActorEgressPolicyRequest{Actor: actorRef}); err != nil {
		t.Fatalf("DeleteActorEgressPolicy failed: %v", err)
	}
}

func TestControlServer_ReconcilerIntegration(t *testing.T) {
	// Start a mock readyz HTTP server representing the task container's metadata server
	readyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}))
	defer readyServer.Close()

	readyAddr := strings.TrimPrefix(readyServer.URL, "http://")

	runner := &mockRunner{
		created: make(map[string]bool),
		inspectMap: map[string]*ContainerInspectResult{
			"ate-default-e2e-task": {
				ID: "ate-default-e2e-task",
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
						{Hostname: "ate-default-e2e-task", IPv4Address: readyAddr + "/32"},
					},
				},
			},
		},
	}

	cfg := DefaultConfig()
	cfg.Runner = runner
	cfg.ReadyTimeout = 500 * time.Millisecond

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	srv.Register(grpcServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer grpcServer.Stop()

	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:    lis.Addr().String(),
		Plaintext: true,
	})
	if err != nil {
		t.Fatalf("substrate.NewClientWithOptions failed: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default", "default", "")
	reconciler.WorkspaceReadyTimeout = 2 * time.Second

	ctx := t.Context()
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{
			Name:     "e2e-task",
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{
			Image:   "debian:12",
			Command: []string{"sleep", "infinity"},
		},
		Status: &v1alpha1.TaskStatus{
			Phase: "Running",
		},
	}

	// 1. Resume active task -> should set WorkspaceReady=True
	if err := reconciler.Resume(ctx, task, nil); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	for _, wantCond := range []string{"WorkspaceReady"} {
		foundTrue := false
		for _, c := range task.GetStatus().GetConditions() {
			if c.GetType() == wantCond && c.GetStatus() == "True" {
				foundTrue = true
			}
		}
		if !foundTrue {
			t.Errorf("expected condition %q=True on resumed task, got conditions: %v", wantCond, task.GetStatus().GetConditions())
		}
	}

	// 2. Suspend -> should suspend actor on ate-local
	if err := reconciler.Suspend(ctx, task, nil); err != nil {
		t.Fatalf("Suspend failed: %v", err)
	}
	actor, err := subClient.GetActor(ctx, "default", "e2e-task")
	if err != nil {
		t.Fatalf("GetActor after suspend failed: %v", err)
	}
	if got := actor.GetStatus().GetState(); got != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		t.Errorf("Actor state after suspend = %v, want SUSPENDED", got)
	}

	// 3. Delete -> should delete both actor and per-task ActorTemplate
	if err := reconciler.Delete(ctx, "default", "e2e-task"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}

// Live test against real Apple container CLI
func TestControlServer_LiveAppleContainer(t *testing.T) {
	if _, err := exec.LookPath("container"); err != nil {
		t.Skip("container CLI not available, skipping live test")
	}

	ctx := t.Context()
	checkCtx, checkCancel := context.WithTimeout(ctx, 10*time.Second)
	defer checkCancel()
	if err := exec.CommandContext(checkCtx, "container", "list").Run(); err != nil {
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
	reqCtx, reqCancel := context.WithTimeout(ctx, 45*time.Second)
	defer reqCancel()

	// 1. Create Actor
	_, err := client.CreateActor(reqCtx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "ax",
				Name:     testActorName,
			},
			ActorTemplate: &ateapipb.ObjectRef{
				Atespace: "ax",
				Name:     "live-template",
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateActor(%q) failed: %v", testActorName, err)
	}

	// 2. Resume Actor (spawns real Apple container and gets IP)
	resumeResp, err := client.ResumeActor(reqCtx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: testActorName},
	})
	if err != nil {
		t.Fatalf("ResumeActor(%q) failed: %v", testActorName, err)
	}

	actor := resumeResp.GetActor()
	workerIP := ""
	if actor.GetStatus() != nil && actor.GetStatus().GetWorkerAssignment() != nil {
		if ips := actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps(); len(ips) > 0 {
			workerIP = ips[0]
		}
	}
	t.Logf("Live Actor resumed in Apple Container: ID=%s, IP=%s, State=%v",
		actor.GetMetadata().GetName(), workerIP, actor.GetStatus().GetState())

	if workerIP == "" {
		t.Errorf("ResumeActor(%q).Status.WorkerAssignment.WorkerPodIps = %v, want non-empty IP", testActorName, actor.GetStatus().GetWorkerAssignment().GetWorkerPodIps())
	}

	// 3. Suspend Actor
	_, err = client.SuspendActor(reqCtx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: testActorName},
	})
	if err != nil {
		t.Fatalf("SuspendActor(%q) failed: %v", testActorName, err)
	}

	// 4. Clean up / Delete Actor
	_, err = client.DeleteActor(reqCtx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "ax", Name: testActorName},
	})
	if err != nil {
		t.Fatalf("DeleteActor(%q) failed: %v", testActorName, err)
	}
}

// Live end-to-end test: TaskReconciler + ate-local + Apple Container + ax-task-runner + guest.Client (ax ssh)
func TestControlServer_LiveReconcilerAndGuestExec(t *testing.T) {
	if _, err := exec.LookPath("container"); err != nil {
		t.Skip("container CLI not available, skipping live reconciler test")
	}

	ctx := t.Context()
	checkCtx, checkCancel := context.WithTimeout(ctx, 10*time.Second)
	defer checkCancel()
	if err := exec.CommandContext(checkCtx, "container", "list").Run(); err != nil {
		t.Skipf("container daemon not responsive: %v, skipping live reconciler test", err)
	}

	// Cross-compile ax-task-runner for linux/arm64 into temp dir
	tmpDir := t.TempDir()
	runnerBin := filepath.Join(tmpDir, "ax-task-runner")
	buildCmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-ldflags=-s -w", "-o", runnerBin, "../../cmd/ax-task-runner")
	buildCmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=arm64", "CGO_ENABLED=0")
	if out, err := buildCmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to cross-compile ax-task-runner: %v (%s)", err, string(out))
	}

	cfg := DefaultConfig()
	cfg.ReadyTimeout = 5 * time.Second
	cfg.Templates["default"] = TemplateConfig{
		Atespace: "default",
		Name:     "default",
		Image:    "debian:12",
		Port:     80,
		Volumes:  []string{fmt.Sprintf("%s:/usr/local/bin/ax-task-runner", runnerBin)},
	}

	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}
	defer lis.Close()

	grpcServer := grpc.NewServer()
	srv.Register(grpcServer)
	go func() { _ = grpcServer.Serve(lis) }()
	defer func() {
		grpcServer.Stop()
		_ = srv.Close(context.Background())
	}()

	subClient, err := substrate.NewClientWithOptions(substrate.ClientOptions{
		Target:    lis.Addr().String(),
		Plaintext: true,
	})
	if err != nil {
		t.Fatalf("substrate.NewClientWithOptions failed: %v", err)
	}
	defer subClient.Close()

	reconciler := controller.NewTaskReconciler(subClient, "default", "default", "")
	reconciler.WorkspaceReadyTimeout = 10 * time.Second

	taskName := fmt.Sprintf("live-task-%d", time.Now().UnixNano()%100000)
	defer func() {
		_ = reconciler.Delete(context.Background(), "default", taskName)
	}()

	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{
			Name:     taskName,
			Atespace: "default",
		},
		Spec: &v1alpha1.TaskSpec{
			Image:   "debian:12",
			Command: []string{"sleep", "infinity"},
			Debug:   true, // enables guest ProcessService (ax ssh)
		},
		Status: &v1alpha1.TaskStatus{
			Phase: "Running",
		},
	}

	if err := reconciler.Resume(ctx, task, nil); err != nil {
		t.Fatalf("Resume(%q) failed: %v", taskName, err)
	}

	actor, err := subClient.GetActor(ctx, "default", taskName)
	if err != nil {
		t.Fatalf("GetActor(%q) failed: %v", taskName, err)
	}
	workerIP := substrate.WorkerIP(actor)
	t.Logf("Live Task resumed: name=%s, workerIP=%s, actorState=%v", taskName, workerIP, actor.GetStatus().GetState())

	if actor.GetStatus().GetState() != ateapipb.ActorState_ACTOR_STATE_RUNNING {
		t.Fatalf("Actor state = %v, want RUNNING", actor.GetStatus().GetState())
	}
	for _, wantCond := range []string{"WorkspaceReady"} {
		foundTrue := false
		for _, c := range task.GetStatus().GetConditions() {
			if c.GetType() == wantCond && c.GetStatus() == "True" {
				foundTrue = true
			}
		}
		if !foundTrue {
			t.Errorf("expected condition %q=True on live task, got: %v", wantCond, task.GetStatus().GetConditions())
		}
	}

	// Dial guest daemon (ax ssh) inside the live Apple container over vmnet IP:80
	guestClient, err := guest.Dial(fmt.Sprintf("%s:80", workerIP))
	if err != nil {
		t.Fatalf("guest.Dial(%s:80) failed: %v", workerIP, err)
	}
	defer guestClient.Close()

	var stdout, stderr bytes.Buffer
	exitCode, err := guestClient.Exec(ctx, guest.ExecOptions{
		Command: []string{"uname", "-sm"},
		Stdout:  &stdout,
		Stderr:  &stderr,
	})
	if err != nil || exitCode != 0 {
		t.Fatalf("guestClient.Exec(uname -sm) exit=%d err=%v stderr=%s", exitCode, err, stderr.String())
	}
	t.Logf("Live ax ssh guest exec inside Apple container (%s:80) output: %s", workerIP, strings.TrimSpace(stdout.String()))
	if !strings.Contains(stdout.String(), "Linux") {
		t.Errorf("unexpected uname output from container: %q", stdout.String())
	}
}

func startTestControlServer(t *testing.T, cfg *Config) (ateapipb.ControlClient, func()) {
	t.Helper()
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen failed: %v", err)
	}

	grpcServer := grpc.NewServer()
	srv.Register(grpcServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient failed: %v", err)
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

type mockRunner struct {
	mu         sync.Mutex
	runCalls   [][]string
	inspectMap map[string]*ContainerInspectResult
	runErr     error
	inspectErr error
	created    map[string]bool
	execOutput string
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
	if len(args) > 0 && args[0] == "exec" && m.execOutput != "" {
		return m.execOutput, nil
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

func TestPodmanRunner_ArgsAndInspect(t *testing.T) {
	for _, tc := range []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "", want: RuntimeApple},
		{in: "apple", want: RuntimeApple},
		{in: "apple-container", want: RuntimeApple},
		{in: "podman", want: RuntimePodman},
		{in: "PODMAN", want: RuntimePodman},
		{in: "unknown", wantErr: true},
	} {
		got, err := NormalizeRuntime(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("NormalizeRuntime(%q) err=%v, wantErr=%v", tc.in, err, tc.wantErr)
		}
		if got != tc.want {
			t.Errorf("NormalizeRuntime(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	gotDel := translatePodmanArgs([]string{"delete", "--force", "ate-default-t1"})
	wantDel := []string{"rm", "--force", "ate-default-t1"}
	if !slices.Equal(gotDel, wantDel) {
		t.Errorf("translatePodmanArgs(delete) = %v, want %v", gotDel, wantDel)
	}

	gotRun := translatePodmanArgs([]string{"run", "--progress", "none", "-d", "--name", "c1", "--init-image", "vminit:0.1", "debian:12", "sleep", "infinity"})
	wantRun := []string{"run", "-p", "127.0.0.1::80", "-d", "--name", "c1", "debian:12", "sleep", "infinity"}
	if !slices.Equal(gotRun, wantRun) {
		t.Errorf("translatePodmanArgs(run) = %v, want %v", gotRun, wantRun)
	}

	gotRunExplicitPort := translatePodmanArgs([]string{"run", "--progress=none", "-d", "-p", "127.0.0.1::8080", "--name", "c2", "debian:12"})
	wantRunExplicitPort := []string{"run", "-d", "-p", "127.0.0.1::8080", "--name", "c2", "debian:12"}
	if !slices.Equal(gotRunExplicitPort, wantRunExplicitPort) {
		t.Errorf("translatePodmanArgs(run with explicit port) = %v, want %v", gotRunExplicitPort, wantRunExplicitPort)
	}

	rawPodmanInspect := `[{
		"Id": "abc123def",
		"State": {"Status": "running", "Running": true},
		"NetworkSettings": {
			"IPAddress": "10.88.0.5",
			"Ports": {
				"80/tcp": [{"HostIp": "127.0.0.1", "HostPort": "54123"}]
			}
		}
	}]`
	insp, err := parsePodmanInspect(rawPodmanInspect, "ate-podman-default-t1")
	if err != nil {
		t.Fatalf("parsePodmanInspect failed: %v", err)
	}
	if insp.Status.State != "running" {
		t.Errorf("insp.Status.State = %q, want running", insp.Status.State)
	}
	if len(insp.Status.Networks) != 1 || insp.Status.Networks[0].IPv4Address != "127.0.0.1:54123" {
		t.Errorf("insp.Status.Networks = %+v, want IPv4Address=127.0.0.1:54123", insp.Status.Networks)
	}
}

func TestCrossClusterTaskMigration_AppleToPodman(t *testing.T) {
	ctx := t.Context()
	snapshotDir := t.TempDir()

	// Cluster A: Apple container runtime
	fakeTarGz := []byte("fake-workspace-tar-gz-payload-from-apple-container")
	appleRunner := &mockRunner{
		created:    make(map[string]bool),
		execOutput: base64.StdEncoding.EncodeToString(fakeTarGz),
	}
	cfgApple := DefaultConfig()
	cfgApple.Runtime = RuntimeApple
	cfgApple.ContainerPrefix = "ate-apple"
	cfgApple.SnapshotDir = snapshotDir
	cfgApple.ReadyTimeout = 100 * time.Millisecond
	cfgApple.Runner = appleRunner

	clientApple, cleanupApple := startTestControlServer(t, cfgApple)
	defer cleanupApple()

	// Cluster B: Podman runtime sharing the same snapshot directory
	podmanPortLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on ephemeral port: %v", err)
	}
	defer podmanPortLis.Close()
	podmanAddr := podmanPortLis.Addr().String()

	podmanRunner := &mockRunner{
		created: make(map[string]bool),
		inspectMap: map[string]*ContainerInspectResult{
			"ate-podman-default-migrated-task": {
				ID: "ate-podman-default-migrated-task",
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
					}{{Hostname: "ate-podman-default-migrated-task", IPv4Address: podmanAddr}},
				},
			},
		},
	}
	cfgPodman := DefaultConfig()
	cfgPodman.Runtime = RuntimePodman
	cfgPodman.ContainerPrefix = "ate-podman"
	cfgPodman.SnapshotDir = snapshotDir
	cfgPodman.ReadyTimeout = 500 * time.Millisecond
	cfgPodman.Runner = podmanRunner

	clientPodman, cleanupPodman := startTestControlServer(t, cfgPodman)
	defer cleanupPodman()

	// 1. Create and Resume task actor on Cluster A (Apple container)
	_, err = clientApple.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "default",
				Name:     "migrated-task",
			},
		},
	})
	if err != nil {
		t.Fatalf("Cluster A CreateActor failed: %v", err)
	}

	resApple, err := clientApple.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "default", Name: "migrated-task"},
	})
	if err != nil {
		t.Fatalf("Cluster A ResumeActor failed: %v", err)
	}
	if gotPod := resApple.GetActor().GetStatus().GetWorkerAssignment().GetWorkerPod(); gotPod != "ate-apple-default-migrated-task" {
		t.Errorf("Cluster A WorkerPod = %q, want ate-apple-default-migrated-task", gotPod)
	}

	// 2. Suspend on Cluster A -> snapshots /workspace to snapshotDir/default/migrated-task.tar.gz
	_, err = clientApple.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "default", Name: "migrated-task"},
	})
	if err != nil {
		t.Fatalf("Cluster A SuspendActor failed: %v", err)
	}

	snapFile := filepath.Join(snapshotDir, "default", "migrated-task.tar.gz")
	savedBytes, err := os.ReadFile(snapFile)
	if err != nil {
		t.Fatalf("expected snapshot file %s to exist after SuspendActor on Cluster A: %v", snapFile, err)
	}
	if !bytes.Equal(savedBytes, fakeTarGz) {
		t.Errorf("saved snapshot bytes = %q, want %q", savedBytes, fakeTarGz)
	}

	// 3. Create and Resume the same task actor on Cluster B (Podman)
	_, err = clientPodman.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: "default",
				Name:     "migrated-task",
			},
		},
	})
	if err != nil {
		t.Fatalf("Cluster B CreateActor failed: %v", err)
	}

	resPodman, err := clientPodman.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: "default", Name: "migrated-task"},
	})
	if err != nil {
		t.Fatalf("Cluster B ResumeActor failed: %v", err)
	}
	if gotPod := resPodman.GetActor().GetStatus().GetWorkerAssignment().GetWorkerPod(); gotPod != "ate-podman-default-migrated-task" {
		t.Errorf("Cluster B WorkerPod = %q, want ate-podman-default-migrated-task", gotPod)
	}
	if gotIP := substrate.WorkerIP(resPodman.GetActor()); gotIP != podmanAddr {
		t.Errorf("Cluster B WorkerIP = %q, want %q", gotIP, podmanAddr)
	}

	// Verify Cluster B restored the snapshot via exec into the Podman container
	podmanRunner.mu.Lock()
	calls := append([][]string(nil), podmanRunner.runCalls...)
	podmanRunner.mu.Unlock()

	foundRestoreExec := false
	wantEncoded := base64.StdEncoding.EncodeToString(fakeTarGz)
	for _, call := range calls {
		if len(call) >= 5 && call[0] == "exec" && call[1] == "ate-podman-default-migrated-task" && strings.Contains(call[4], wantEncoded) {
			foundRestoreExec = true
			break
		}
	}
	if !foundRestoreExec {
		t.Errorf("expected Cluster B to restore snapshot via exec, got calls: %v", calls)
	}
}
