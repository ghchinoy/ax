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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/firebase/genkit/go/ai"
	"github.com/google/ax/internal/workspace"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
	"google.golang.org/protobuf/types/known/structpb"
)

func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	return s
}

func TestParseAgentSpec_Errors(t *testing.T) {
	for name, m := range map[string]map[string]any{
		"empty config":       {},
		"missing model":      {"name": "triage"},
		"empty model":        {"model": ""},
		"non-string model":   {"model": 123},
		"invalid agent name": {"name": "../bad", "model": "gemini-3.8-flash"},
		"bad description":    {"model": "gemini-3.8-flash", "description": 1},
		"bad instruction":    {"model": "gemini-3.8-flash", "instruction": 1},
		"empty promptFile":   {"promptFile": "   "},
		"non-list use":       {"model": "gemini-3.8-flash", "use": "filesystem"},
		"use map no name":    {"model": "gemini-3.8-flash", "use": []any{map[string]any{"config": map[string]any{}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if got, err := parseAgentSpec(mustStruct(t, m), nil, "/workspace"); err == nil {
				t.Errorf("parseAgentSpec(%v) = %+v, want error", m, got)
			}
		})
	}
}

func TestParseAgentSpec_DefaultsAndNormalization(t *testing.T) {
	ws := &v1alpha1.Workspace{
		Spec: &v1alpha1.WorkspaceSpec{
			Skills: &v1alpha1.SkillsConfig{Path: ".agents/skills"},
			Mcp: &v1alpha1.MCPConfig{
				Servers: []*v1alpha1.MCPServer{
					{Name: "docs", Endpoint: "https://example.com/mcp"},
					{Name: "git", Command: "git-mcp", Args: []string{"--repo", "."}},
				},
			},
		},
	}

	spec, err := parseAgentSpec(mustStruct(t, map[string]any{
		"name":        "triage_agent",
		"description": "Triages failing tests.",
		"model":       "gemini-3.8-flash",
		"instruction": "You are a careful engineer.",
		"maxTurns":    15,
		"use": []any{
			map[string]any{
				"name": "filesystem",
				"config": map[string]any{
					"rootDirectory":    "/workspace",
					"allowWriteAccess": true,
				},
			},
		},
	}), ws, "/tmp/custom-ws")
	if err != nil {
		t.Fatalf("parseAgentSpec: %v", err)
	}

	if spec.Name != "triage_agent" {
		t.Errorf("Name = %q, want triage_agent", spec.Name)
	}
	if spec.Model != "googleai/gemini-3.8-flash" {
		t.Errorf("Model = %q, want googleai/gemini-3.8-flash", spec.Model)
	}
	if len(spec.MCPServers) != 2 {
		t.Errorf("MCPServers = %d, want 2", len(spec.MCPServers))
	}

	promptDir := t.TempDir()
	if err := spec.writePromptFile(promptDir, "/tmp/custom-ws", []string{"docs_search"}); err != nil {
		t.Fatalf("writePromptFile: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(promptDir, "triage_agent.prompt"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	got := string(content)
	for _, wantSub := range []string{
		"model: googleai/gemini-3.8-flash",
		"name: genkit-middleware/filesystem",
		"rootDirectory: /tmp/custom-ws",
		"name: genkit-middleware/skills",
		"/tmp/custom-ws/.agents/skills",
		"- docs_search",
		"{{role \"system\"}}\nYou are a careful engineer.\n{{history}}",
	} {
		if !strings.Contains(got, wantSub) {
			t.Errorf("prompt file missing %q:\n%s", wantSub, got)
		}
	}
}

func TestGenkitRunner_EndToEndWithSuspendResumeAndFilesystem(t *testing.T) {
	dir := t.TempDir()
	wsPath := filepath.Join(dir, "workspace")

	origAXDir := workspace.AXDir
	workspace.AXDir = filepath.Join(dir, "ax")
	t.Cleanup(func() { workspace.AXDir = origAXDir })

	ws := &v1alpha1.Workspace{
		Metadata: &v1alpha1.ObjectMeta{Name: "repo-ws", Atespace: "default"},
		Spec: &v1alpha1.WorkspaceSpec{
			Files: []*v1alpha1.File{
				{Path: "README.md", Content: "# Sample Repo\nAll tests pass.\n"},
			},
		},
	}
	task := &v1alpha1.Task{
		Metadata: &v1alpha1.ObjectMeta{Name: "genkit-triage", Atespace: "default"},
		Spec: &v1alpha1.TaskSpec{
			Workspace: &v1alpha1.WorkspaceRef{Name: "repo-ws", Path: wsPath},
			Config: mustStruct(t, map[string]any{
				"name":        "triage_agent",
				"description": "Triages failing tests.",
				"model":       "mock/echo",
				"instruction": "You are a careful engineer.",
				"maxTurns":    10,
				"use": []any{
					map[string]any{
						"name": "genkit-middleware/filesystem",
						"config": map[string]any{
							"allowWriteAccess": true,
						},
					},
				},
			}),
		},
	}

	mockPlugin := &mockModelPlugin{
		provider: "mock",
		model:    "echo",
		fn: func(ctx context.Context, req *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error) {
			var userMsgs []string
			var sysText string
			var toolNames []string
			for _, m := range req.Messages {
				switch m.Role {
				case ai.RoleUser:
					userMsgs = append(userMsgs, m.Text())
				case ai.RoleSystem:
					sysText = m.Text()
				}
			}
			for _, td := range req.Tools {
				toolNames = append(toolNames, td.Name)
			}
			reply := fmt.Sprintf("turn=%d history=%s tools=%d sys=%s",
				len(userMsgs),
				strings.Join(userMsgs, "|"),
				len(toolNames),
				strings.TrimSpace(sysText),
			)
			return &ai.ModelResponse{
				Request: req,
				Message: ai.NewModelTextMessage(reply),
			}, nil
		},
	}

	// --- Boot 1: Run runner.Run with Genkit handler ---
	port1 := freePort(t)
	metaPort1 := freePort(t)
	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)

	dyn1 := &dynamicHandler{}
	go func() {
		done1 <- runner.Run(ctx1, runner.Config{
			Port:         port1,
			MetadataPort: metaPort1,
			Task:         task,
			Workspace:    ws,
			Handler:      dyn1,
			OnWorkspaceReady: func(ctx context.Context, wp string) error {
				h, _, err := buildAgentHandler(ctx, task, ws, wp, mockPlugin)
				if err != nil {
					return err
				}
				dyn1.Set(h)
				return nil
			},
		})
	}()

	waitReady(t, port1)

	// 1. Check /.well-known/agent-card.json
	cardResp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/.well-known/agent-card.json", port1))
	if err != nil {
		t.Fatalf("GET agent-card.json: %v", err)
	}
	cardBody, _ := io.ReadAll(cardResp.Body)
	cardResp.Body.Close()
	if cardResp.StatusCode != http.StatusOK || !strings.Contains(string(cardBody), `"triage_agent"`) {
		t.Fatalf("unexpected agent card (%d): %s", cardResp.StatusCode, cardBody)
	}

	// 2. Send A2A JSON-RPC message (turn 1)
	a2aReq1 := `{
		"jsonrpc": "2.0", "id": 1, "method": "message/send",
		"params": {"message": {"role": "user", "messageId": "m1", "contextId": "ctx-triage",
			"parts": [{"kind": "text", "text": "Triage the failing tests."}]}}
	}`
	resp1, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/", port1), "application/json", bytes.NewBufferString(a2aReq1))
	if err != nil {
		t.Fatalf("A2A turn 1 failed: %v", err)
	}
	raw1, _ := io.ReadAll(resp1.Body)
	resp1.Body.Close()
	if !strings.Contains(string(raw1), "turn=1 history=Triage the failing tests. tools=4") {
		t.Fatalf("unexpected A2A turn 1 response: %s", raw1)
	}

	// Simulate `ax suspend` (stop Boot 1)
	cancel1()
	if err := <-done1; err != nil {
		t.Fatalf("Boot 1 exited with error: %v", err)
	}

	// --- Boot 2: Simulate `ax resume` on a new container against the same wsPath ---
	port2 := freePort(t)
	metaPort2 := freePort(t)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	done2 := make(chan error, 1)

	dyn2 := &dynamicHandler{}
	go func() {
		done2 <- runner.Run(ctx2, runner.Config{
			Port:         port2,
			MetadataPort: metaPort2,
			Task:         task,
			Workspace:    ws,
			Handler:      dyn2,
			OnWorkspaceReady: func(ctx context.Context, wp string) error {
				h, _, err := buildAgentHandler(ctx, task, ws, wp, mockPlugin)
				if err != nil {
					return err
				}
				dyn2.Set(h)
				return nil
			},
		})
	}()

	waitReady(t, port2)

	// 3. Send A2A JSON-RPC message (turn 2) with the same contextId "ctx-triage"
	a2aReq2 := `{
		"jsonrpc": "2.0", "id": 2, "method": "message/send",
		"params": {"message": {"role": "user", "messageId": "m2", "contextId": "ctx-triage",
			"parts": [{"kind": "text", "text": "Write the summary."}]}}
	}`
	resp2, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/", port2), "application/json", bytes.NewBufferString(a2aReq2))
	if err != nil {
		t.Fatalf("A2A turn 2 failed: %v", err)
	}
	raw2, _ := io.ReadAll(resp2.Body)
	resp2.Body.Close()
	if !strings.Contains(string(raw2), "turn=2 history=Triage the failing tests.|Write the summary. tools=4") {
		t.Fatalf("session did not survive suspend/resume: %s", raw2)
	}

	// 4. Send turn 3 via Genkit's native HTTP route (/agents/triage_agent) on the same session!
	genkitReq := `{
		"data": {"message": {"role": "user", "content": [{"text": "Done?"}]}},
		"init": {"sessionId": "ctx-triage"}
	}`
	resp3, err := http.Post(fmt.Sprintf("http://127.0.0.1:%d/agents/triage_agent", port2), "application/json", bytes.NewBufferString(genkitReq))
	if err != nil {
		t.Fatalf("Genkit HTTP turn 3 failed: %v", err)
	}
	var genkitBody map[string]any
	_ = json.NewDecoder(resp3.Body).Decode(&genkitBody)
	resp3.Body.Close()
	raw3, _ := json.Marshal(genkitBody)
	if !strings.Contains(string(raw3), "turn=3 history=Triage the failing tests.|Write the summary.|Done? tools=4") {
		t.Fatalf("unexpected Genkit native HTTP response: %s", raw3)
	}

	cancel2()
	if err := <-done2; err != nil {
		t.Fatalf("Boot 2 exited with error: %v", err)
	}
}

func waitReady(t *testing.T, port int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/readyz", port))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("runner on port %d never became ready", port)
}

func freePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	return lis.Addr().(*net.TCPAddr).Port
}
