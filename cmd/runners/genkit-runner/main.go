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

// Command genkit-runner is a task runner that serves a Genkit Go agent
// configured from the Task's spec.config (Dotprompt frontmatter + instruction)
// or a .prompt file in the workspace. It is installed as
// /usr/local/bin/ax-task-runner in the image built from Dockerfile.genkit.
//
// When the task leaves spec.command empty and sets spec.config, the runner
// serves the configured Genkit agent in-process on port 80 alongside /healthz,
// /readyz, and the optional guest gRPC services:
//   - A2A JSON-RPC at / and /.well-known/agent-card.json
//   - Genkit native agent HTTP routes at /agents/<name>, /getSnapshot,
//     /waitForSnapshot, and /abort
//
// Sessions are persisted in /workspace/.genkit/snapshots so multi-turn
// conversations survive `ax suspend` and `ax resume`. A task that sets
// spec.command runs that command unchanged, just like ax-task-runner.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/ax/internal/workspace"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/ax/runner"
)

func main() {
	var (
		cfg      runner.Config
		taskFile string
		wsFile   string
	)
	flag.IntVar(&cfg.Port, "port", runner.DefaultPort, "Port for the probe, agent, and guest server")
	flag.IntVar(&cfg.MetadataPort, "metadata-port", runner.DefaultMetadataPort, "Loopback-only port for the metadata endpoints")
	flag.StringVar(&taskFile, "task-file", "", "Read the Task YAML from this file instead of AX_TASK_YAML")
	flag.StringVar(&wsFile, "workspace-file", "", "Read the Workspace YAML from this file instead of AX_WORKSPACE_YAML")
	flag.StringVar(&workspace.AXDir, "ax-dir", workspace.DefaultAXDir, "Directory for AX runner state and initialization markers")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	var ws v1alpha1.Workspace
	if err := runner.LoadSpec(wsFile, "AX_WORKSPACE_YAML", &ws); err != nil {
		fatal(err)
	} else if ws.GetMetadata() != nil {
		cfg.Workspace = &ws
	}

	var task v1alpha1.Task
	if err := runner.LoadSpec(taskFile, "AX_TASK_YAML", &task); err != nil {
		fatal(err)
	} else if task.GetMetadata() != nil {
		cfg.Task = &task
	}

	var cleanup func()
	defer func() {
		if cleanup != nil {
			cleanup()
		}
	}()

	// When spec.command is empty and spec.config is set, serve the Genkit
	// agent in-process on the probe listener once the workspace is ready.
	if spec := cfg.Task.GetSpec(); len(spec.GetCommand()) == 0 && spec.GetConfig() != nil {
		// Validate spec.config up front before starting the runner.
		if _, err := parseAgentSpec(spec.GetConfig(), cfg.Workspace, spec.WorkspacePath()); err != nil {
			fatal(err)
		}
		dyn := &dynamicHandler{}
		cfg.Handler = dyn
		cfg.OnWorkspaceReady = func(ctx context.Context, wsPath string) error {
			h, c, err := buildAgentHandler(ctx, cfg.Task, cfg.Workspace, wsPath)
			if err != nil {
				return err
			}
			cleanup = c
			dyn.Set(h)
			return nil
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := runner.Run(ctx, cfg); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	slog.Error("genkit runner failed", "error", err)
	os.Exit(1)
}
