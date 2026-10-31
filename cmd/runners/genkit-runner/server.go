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
	"context"
	"fmt"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2acompat/a2av0"
	"github.com/a2aproject/a2a-go/v2/a2asrv"
	"github.com/firebase/genkit/go/ai"
	aix "github.com/firebase/genkit/go/ai/exp"
	"github.com/firebase/genkit/go/ai/exp/localstore"
	"github.com/firebase/genkit/go/core/api"
	"github.com/firebase/genkit/go/genkit"
	genkitx "github.com/firebase/genkit/go/genkit/exp"
	"github.com/firebase/genkit/go/plugins/anthropic"
	"github.com/firebase/genkit/go/plugins/compat_oai/openai"
	"github.com/firebase/genkit/go/plugins/googlegenai"
	"github.com/firebase/genkit/go/plugins/mcp"
	"github.com/firebase/genkit/go/plugins/middleware"
	"github.com/google/ax/pkg/apis/v1alpha1"
)

// dynamicHandler delegates HTTP requests to the inner handler set once
// initialization completes on workspace readiness.
type dynamicHandler struct {
	mu      sync.RWMutex
	handler http.Handler
}

func (d *dynamicHandler) Set(h http.Handler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handler = h
}

func (d *dynamicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.mu.RLock()
	h := d.handler
	d.mu.RUnlock()
	if h == nil {
		http.Error(w, "agent initializing", http.StatusServiceUnavailable)
		return
	}
	h.ServeHTTP(w, r)
}

// buildAgentHandler initializes Genkit, connects any Workspace MCP servers,
// materializes the Dotprompt file from spec, defines the store-backed Genkit
// agent under wsPath/.genkit/snapshots, and mounts both Genkit's native HTTP
// routes (/agents/<name>...) and the A2A JSON-RPC server (/ and
// /.well-known/agent-card.json).
func buildAgentHandler(ctx context.Context, task *v1alpha1.Task, ws *v1alpha1.Workspace, wsPath string, extraPlugins ...api.Plugin) (http.Handler, func(), error) {
	spec, err := parseAgentSpec(task.GetSpec().GetConfig(), ws, wsPath)
	if err != nil {
		return nil, nil, err
	}

	promptDir := filepath.Join(wsPath, ".genkit", "prompts")
	if err := os.MkdirAll(promptDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("creating prompt directory: %w", err)
	}

	// Write the initial prompt file before genkit.Init so the prompt registry
	// discovers it; if MCP servers advertise tools we rewrite and reload before
	// defining the agent.
	if err := spec.writePromptFile(promptDir, wsPath, nil); err != nil {
		return nil, nil, err
	}

	plugins := defaultPlugins(spec.Model)
	plugins = append(plugins, extraPlugins...)

	// Connect MCP servers first (if any) using a bootstrap Genkit instance or
	// after Init. Because DefinePromptAgent renders the prompt at definition
	// time, we initialize Genkit with the prompt directory after writing any
	// discovered MCP tool names into the prompt frontmatter.
	var mcpCleanup func()
	var mcpClients []*mcp.GenkitMCPClient
	for _, srv := range spec.MCPServers {
		client, err := mcp.NewGenkitMCPClient(srv.Config)
		if err != nil {
			slog.Warn("failed to connect to workspace MCP server", "server", srv.Name, "error", err)
			continue
		}
		mcpClients = append(mcpClients, client)
	}
	mcpCleanup = func() {
		for _, c := range mcpClients {
			_ = c.Disconnect()
		}
	}

	g := genkit.Init(ctx,
		genkit.WithExperimental(),
		genkit.WithPromptDir(promptDir),
		genkit.WithPlugins(plugins...),
	)

	// Register active tools from any connected MCP servers on g.
	var mcpToolNames []string
	for _, c := range mcpClients {
		tools, err := c.GetActiveTools(ctx, g)
		if err != nil {
			slog.Warn("failed to fetch tools from MCP server", "error", err)
			continue
		}
		for _, t := range tools {
			mcpToolNames = append(mcpToolNames, t.Name())
		}
	}
	// If MCP servers provided tools and we generated the prompt from
	// spec.config, re-register the prompt with those tools attached.
	if len(mcpToolNames) > 0 && spec.PromptFile == "" {
		if err := spec.writePromptFile(promptDir, wsPath, mcpToolNames); err != nil {
			mcpCleanup()
			return nil, nil, err
		}
		// Re-initialize with the updated prompt file so the prompt includes MCP tools.
		g = genkit.Init(ctx,
			genkit.WithExperimental(),
			genkit.WithPromptDir(promptDir),
			genkit.WithPlugins(plugins...),
		)
		for _, c := range mcpClients {
			_, _ = c.GetActiveTools(ctx, g)
		}
	}

	storeDir := filepath.Join(wsPath, ".genkit", "snapshots")
	store, err := localstore.NewFileSessionStore[any](storeDir)
	if err != nil {
		mcpCleanup()
		return nil, nil, fmt.Errorf("creating session store: %w", err)
	}

	var opts []aix.PromptAgentOption[any]
	opts = append(opts, aix.WithSessionStore(store))
	if spec.Description != "" {
		opts = append(opts, aix.WithDescription[any](spec.Description))
	}
	ag := genkitx.DefinePromptAgent(g, spec.Name, opts...)

	mux := http.NewServeMux()
	for _, route := range genkitx.AllAgentRoutes(g) {
		mux.HandleFunc(route.Pattern(), route.Handler())
	}

	a2aHandler := a2asrv.NewHandler(&genkitA2AExecutor{agent: ag})
	mux.Handle("/", a2av0.NewJSONRPCHandler(a2aHandler))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(&a2a.AgentCard{
		Name:               spec.Name,
		Description:        firstNonEmpty(spec.Description, fmt.Sprintf("Genkit agent %s", spec.Name)),
		Version:            "1.0.0",
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
	}))

	slog.Info("initialized Genkit agent server",
		"agent", spec.Name,
		"model", spec.Model,
		"storeDir", storeDir,
		"mcpTools", len(mcpToolNames),
	)
	return mux, mcpCleanup, nil
}

// defaultPlugins returns the built-in middleware plugin plus the model
// provider plugins whose credentials or model prefixes are active.
func defaultPlugins(modelName string) []api.Plugin {
	plugins := []api.Plugin{&middleware.Middleware{}}

	switch {
	case strings.HasPrefix(modelName, "googleai/"):
		if os.Getenv("GEMINI_API_KEY") != "" || os.Getenv("GOOGLE_API_KEY") != "" {
			plugins = append(plugins, &googlegenai.GoogleAI{})
		}
	case strings.HasPrefix(modelName, "vertexai/"):
		plugins = append(plugins, &googlegenai.VertexAI{
			ProjectID: firstNonEmpty(os.Getenv("GOOGLE_CLOUD_PROJECT"), os.Getenv("GCLOUD_PROJECT")),
			Location:  firstNonEmpty(os.Getenv("GOOGLE_CLOUD_LOCATION"), "global"),
		})
	case strings.HasPrefix(modelName, "anthropic/"):
		if os.Getenv("ANTHROPIC_API_KEY") != "" || os.Getenv("ANTHROPIC_AUTH_TOKEN") != "" {
			plugins = append(plugins, &anthropic.Anthropic{})
		}
	case strings.HasPrefix(modelName, "openai/"):
		if os.Getenv("OPENAI_API_KEY") != "" {
			plugins = append(plugins, &openai.OpenAI{})
		}
	default:
		if os.Getenv("GEMINI_API_KEY") != "" || os.Getenv("GOOGLE_API_KEY") != "" {
			plugins = append(plugins, &googlegenai.GoogleAI{})
		}
	}
	return plugins
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// genkitA2AExecutor adapts a Genkit *aix.Agent[any] to an A2A AgentExecutor.
type genkitA2AExecutor struct {
	agent *aix.Agent[any]
}

func (e *genkitA2AExecutor) Execute(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		var texts []string
		if execCtx.Message != nil {
			for _, p := range execCtx.Message.Parts {
				if t, ok := p.Content.(a2a.Text); ok {
					texts = append(texts, string(t))
				}
			}
		}
		prompt := strings.Join(texts, "\n")

		var opts []aix.InvocationOption[any]
		if execCtx.ContextID != "" {
			opts = append(opts, aix.WithSessionID[any](execCtx.ContextID))
		}

		out, err := e.agent.RunText(ctx, prompt, opts...)
		if err != nil {
			yield(nil, err)
			return
		}
		if out.FinishReason == aix.AgentFinishReasonFailed && out.Error != nil {
			yield(nil, fmt.Errorf("agent turn failed (%s): %s", out.Error.Status, out.Error.Message))
			return
		}

		var replyText string
		if out.Message != nil {
			replyText = out.Message.Text()
		}
		reply := a2a.NewMessageForTask(a2a.MessageRoleAgent, execCtx, a2a.NewTextPart(replyText))
		if execCtx.ContextID != "" {
			reply.ContextID = execCtx.ContextID
		} else {
			reply.ContextID = out.SessionID
		}
		yield(reply, nil)
	}
}

func (e *genkitA2AExecutor) Cancel(ctx context.Context, execCtx *a2asrv.ExecutorContext) iter.Seq2[a2a.Event, error] {
	return func(yield func(a2a.Event, error) bool) {
		if execCtx.ContextID != "" && e.agent.Store() != nil {
			if snap, err := e.agent.GetLatestSnapshot(ctx, execCtx.ContextID, aix.WithMetadataOnly()); err == nil && snap != nil && snap.Status == aix.SnapshotStatusPending {
				_, _ = e.agent.Abort(ctx, snap.SnapshotID)
			}
		}
		yield(a2a.NewStatusUpdateEvent(execCtx, a2a.TaskStateCanceled, nil), nil)
	}
}

// mockModelPlugin is a test helper plugin that registers a deterministic model
// on the Genkit instance during genkit.Init.
type mockModelPlugin struct {
	provider string
	model    string
	fn       func(ctx context.Context, req *ai.ModelRequest, cb ai.ModelStreamCallback) (*ai.ModelResponse, error)
}

func (p *mockModelPlugin) Name() string { return p.provider }

func (p *mockModelPlugin) Init(ctx context.Context) []api.Action {
	m := ai.NewModel(api.NewName(p.provider, p.model), &ai.ModelOptions{
		Supports: &ai.ModelSupports{Multiturn: true, SystemRole: true, Tools: true, Media: true},
	}, p.fn)
	return []api.Action{m.(api.Action)}
}
