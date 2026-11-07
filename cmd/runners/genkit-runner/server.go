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
	"encoding/json"
	"fmt"
	"iter"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2acompat/a2av0"
	a2agrpcv0 "github.com/a2aproject/a2a-go/v2/a2agrpc/v0"
	a2agrpcv1 "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
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
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
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
// agent under wsPath/.genkit/snapshots, and mounts Genkit's native HTTP
// routes (/agents/<name>...), the A2A JSON-RPC server (/ and
// /.well-known/agent-card.json), and the A2A gRPC server
// (lf.a2a.v1.A2AService and a2a.v1.A2AService).
func buildAgentHandler(ctx context.Context, task *v1alpha1.Task, ws *v1alpha1.Workspace, wsPath string, extraPlugins ...api.Plugin) (http.Handler, func(), error) {
	spec, err := parseAgentSpec(task.GetSpec().GetConfig(), ws, wsPath)
	if err != nil {
		return nil, nil, err
	}

	promptDir := filepath.Join(wsPath, ".genkit", "prompts")
	if err := os.MkdirAll(promptDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("creating prompt directory: %w", err)
	}

	tracesPath := filepath.Join(wsPath, ".genkit", "traces.jsonl")
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(&genkitSpanExporter{
			agentName:  spec.Name,
			tracesPath: tracesPath,
		}),
	)
	otel.SetTracerProvider(tp)

	plugins := defaultPlugins(spec.Model)
	plugins = append(plugins, extraPlugins...)

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
		_ = tp.Shutdown(context.Background())
		for _, c := range mcpClients {
			_ = c.Disconnect()
		}
	}

	g := genkit.Init(ctx,
		genkit.WithExperimental(),
		genkit.WithPlugins(plugins...),
	)

	// Register active tools from any connected MCP servers on g before writing
	// and loading the prompt file so discovered MCP tools are included in
	// frontmatter with a single genkit.Init call.
	var mcpToolNames []string
	for _, c := range mcpClients {
		tools, err := c.GetActiveTools(ctx, g)
		if err != nil {
			slog.Warn("failed to fetch tools from MCP server", "error", err)
			continue
		}
		for _, t := range tools {
			if reg, ok := t.(api.Registerable); ok {
				genkit.RegisterAction(g, reg)
			}
			mcpToolNames = append(mcpToolNames, t.Name())
		}
	}

	if err := spec.writePromptFile(promptDir, wsPath, mcpToolNames); err != nil {
		mcpCleanup()
		return nil, nil, err
	}
	genkit.LoadPromptDir(g, promptDir, "")

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

	agentDesc := firstNonEmpty(spec.Description, fmt.Sprintf("Genkit agent %s", spec.Name))
	agentCard := &a2a.AgentCard{
		Name:               spec.Name,
		Description:        agentDesc,
		Version:            "1.0.0",
		DefaultInputModes:  []string{"text/plain"},
		DefaultOutputModes: []string{"text/plain"},
		SupportedInterfaces: []*a2a.AgentInterface{
			a2a.NewAgentInterface("localhost:80", a2a.TransportProtocolGRPC),
			a2a.NewAgentInterface("http://localhost:80/", a2a.TransportProtocolJSONRPC),
		},
		Capabilities: a2a.AgentCapabilities{
			Streaming:         true,
			ExtendedAgentCard: true,
		},
		Skills: []a2a.AgentSkill{{
			ID:          spec.Name,
			Name:        spec.Name,
			Description: agentDesc,
			Tags:        []string{"genkit", "ax"},
		}},
	}

	a2aHandler := a2asrv.NewHandler(&genkitA2AExecutor{agentName: spec.Name, agent: ag}, a2asrv.WithExtendedAgentCard(agentCard))
	mux.Handle("/", a2av0.NewJSONRPCHandler(a2aHandler))
	mux.Handle(a2asrv.WellKnownAgentCardPath, a2asrv.NewStaticAgentCardHandler(agentCard))

	a2aGRPCServer := grpc.NewServer()
	a2agrpcv1.NewHandler(a2aHandler).RegisterWith(a2aGRPCServer)
	a2agrpcv0.NewHandler(a2aHandler).RegisterWith(a2aGRPCServer)

	combined := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			a2aGRPCServer.ServeHTTP(w, r)
			return
		}
		mux.ServeHTTP(w, r)
	})

	slog.Info("initialized Genkit agent server",
		"agent", spec.Name,
		"model", spec.Model,
		"storeDir", storeDir,
		"mcpTools", len(mcpToolNames),
	)
	return combined, mcpCleanup, nil
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

// genkitSpanRecord is persisted to /workspace/.genkit/traces.jsonl and logged
// via slog so Substrate worker logs and AX Central can reconstruct the full
// Genkit OpenTelemetry span tree for every agent turn, model call, and tool call.
type genkitSpanRecord struct {
	Agent         string  `json:"agent"`
	Span          string  `json:"span"`
	Type          string  `json:"type,omitempty"`
	Subtype       string  `json:"subtype,omitempty"`
	Status        string  `json:"status,omitempty"`
	TraceID       string  `json:"trace_id"`
	SpanID        string  `json:"span_id"`
	ParentSpanID  string  `json:"parent_span_id,omitempty"`
	StartUnixNano int64   `json:"start_unix_nano"`
	EndUnixNano   int64   `json:"end_unix_nano"`
	DurationMs    float64 `json:"duration_ms"`
}

type genkitSpanExporter struct {
	mu         sync.Mutex
	agentName  string
	tracesPath string
}

func (e *genkitSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	var f *os.File
	if e.tracesPath != "" {
		f, _ = os.OpenFile(e.tracesPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if f != nil {
			defer f.Close()
		}
	}

	for _, s := range spans {
		var spanType, spanSubtype, spanState string
		for _, attr := range s.Attributes() {
			switch string(attr.Key) {
			case "genkit:type":
				spanType = attr.Value.AsString()
			case "genkit:subtype":
				spanSubtype = attr.Value.AsString()
			case "genkit:state":
				spanState = attr.Value.AsString()
			}
		}
		if spanState == "" {
			spanState = "success"
		}
		traceID := s.SpanContext().TraceID().String()
		spanID := s.SpanContext().SpanID().String()
		parentSpanID := ""
		if s.Parent().IsValid() {
			parentSpanID = s.Parent().SpanID().String()
		}
		startNano := s.StartTime().UnixNano()
		endNano := s.EndTime().UnixNano()
		durMs := float64(endNano-startNano) / 1e6

		rec := genkitSpanRecord{
			Agent:         e.agentName,
			Span:          s.Name(),
			Type:          spanType,
			Subtype:       spanSubtype,
			Status:        spanState,
			TraceID:       traceID,
			SpanID:        spanID,
			ParentSpanID:  parentSpanID,
			StartUnixNano: startNano,
			EndUnixNano:   endNano,
			DurationMs:    durMs,
		}
		if f != nil {
			if data, err := json.Marshal(rec); err == nil {
				_, _ = f.Write(append(data, '\n'))
			}
		}
		slog.Info("genkit otel span",
			"agent", e.agentName,
			"span", s.Name(),
			"type", spanType,
			"subtype", spanSubtype,
			"status", spanState,
			"duration_ms", fmt.Sprintf("%.2f", durMs),
			"start_unix_nano", startNano,
			"end_unix_nano", endNano,
			"trace_id", traceID,
			"span_id", spanID,
			"parent_span_id", parentSpanID,
		)
	}
	return nil
}

func (e *genkitSpanExporter) Shutdown(_ context.Context) error { return nil }

// genkitA2AExecutor adapts a Genkit *aix.Agent[any] to an A2A AgentExecutor.
type genkitA2AExecutor struct {
	agentName string
	agent     *aix.Agent[any]
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

		started := time.Now()
		slog.Info("A2A turn started",
			"agent", e.agentName,
			"context_id", execCtx.ContextID,
			"prompt_chars", len(prompt),
		)

		out, err := e.agent.RunText(ctx, prompt, opts...)
		if err != nil {
			slog.Error("A2A turn failed",
				"agent", e.agentName,
				"context_id", execCtx.ContextID,
				"error", err,
			)
			yield(nil, err)
			return
		}
		if out.FinishReason == aix.AgentFinishReasonFailed && out.Error != nil {
			err := fmt.Errorf("agent turn failed (%s): %s", out.Error.Status, out.Error.Message)
			slog.Error("A2A turn failed",
				"agent", e.agentName,
				"context_id", execCtx.ContextID,
				"error", err,
			)
			yield(nil, err)
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
		slog.Info("A2A turn completed",
			"agent", e.agentName,
			"context_id", reply.ContextID,
			"finish_reason", string(out.FinishReason),
			"duration_ms", fmt.Sprintf("%.2f", float64(time.Since(started).Nanoseconds())/1e6),
			"reply_chars", len(replyText),
		)
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
