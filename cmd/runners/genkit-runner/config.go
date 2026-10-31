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
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/firebase/genkit/go/plugins/mcp"
	"github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

const (
	// defaultAgentName is used when spec.config omits name.
	defaultAgentName = "agent"
	// defaultGoogleProvider is prepended to a bare Gemini model ID (such as
	// "gemini-3.8-flash") when no provider prefix is present.
	defaultGoogleProvider = "googleai"
)

// validAgentName matches a safe Dotprompt / URL path identifier.
var validAgentName = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

// agentSpec holds the parsed agent configuration derived from Task.spec.config
// and the bound Workspace.
type agentSpec struct {
	Name        string
	Description string
	Model       string
	PromptFile  string
	Instruction string
	Frontmatter map[string]any
	MCPServers  []mcp.MCPServerConfig
}

// parseAgentSpec extracts the agent definition and Dotprompt frontmatter from
// Task.spec.config and merges any Workspace skills and MCP servers.
func parseAgentSpec(config *structpb.Struct, ws *v1alpha1.Workspace, wsPath string) (*agentSpec, error) {
	fields := config.GetFields()
	if len(fields) == 0 {
		return nil, errors.New("spec.config: must not be empty")
	}

	raw := config.AsMap()
	spec := &agentSpec{
		Name:        defaultAgentName,
		Frontmatter: make(map[string]any),
	}

	if v, ok := raw["name"]; ok {
		s, ok := v.(string)
		if !ok || !validAgentName.MatchString(s) {
			return nil, fmt.Errorf("spec.config.name: %v must be an identifier matching %s", v, validAgentName)
		}
		spec.Name = s
		delete(raw, "name")
	}

	if v, ok := raw["description"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("spec.config.description: must be a string")
		}
		spec.Description = s
		delete(raw, "description")
	}

	if v, ok := raw["promptFile"]; ok {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return nil, errors.New("spec.config.promptFile: must be a non-empty string")
		}
		spec.PromptFile = s
		delete(raw, "promptFile")
	}

	if v, ok := raw["instruction"]; ok {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("spec.config.instruction: must be a string")
		}
		spec.Instruction = s
		delete(raw, "instruction")
	}

	if v, ok := raw["model"]; ok {
		s, ok := v.(string)
		if !ok || strings.TrimSpace(s) == "" {
			return nil, errors.New("spec.config.model: must be a non-empty string")
		}
		spec.Model = normalizeModelName(s)
		raw["model"] = spec.Model
	} else if spec.PromptFile == "" {
		return nil, errors.New("spec.config: model is required when promptFile is not set")
	}

	// Normalize middleware entries under "use", resolving relative filesystem
	// rootDirectory paths against wsPath and wiring Workspace.spec.skills.path.
	useList, err := normalizeUse(raw["use"], ws, wsPath)
	if err != nil {
		return nil, err
	}
	if len(useList) > 0 {
		raw["use"] = useList
	}

	spec.Frontmatter = raw
	spec.MCPServers = workspaceMCPServers(ws.GetSpec().GetMcp())
	return spec, nil
}

// normalizeModelName ensures a model name carries a Genkit provider prefix.
// Bare Gemini model names (e.g. "gemini-3.8-flash") default to "googleai/".
func normalizeModelName(model string) string {
	if strings.Contains(model, "/") {
		return model
	}
	return defaultGoogleProvider + "/" + model
}

// normalizeUse normalizes the Dotprompt `use` middleware list and injects
// genkit-middleware/skills when the bound Workspace declares a skills path.
func normalizeUse(rawUse any, ws *v1alpha1.Workspace, wsPath string) ([]any, error) {
	var out []any
	hasSkills := false

	if rawUse != nil {
		items, ok := rawUse.([]any)
		if !ok {
			return nil, errors.New("spec.config.use: must be a list")
		}
		for i, item := range items {
			switch v := item.(type) {
			case string:
				name := normalizeMiddlewareName(v)
				if name == "genkit-middleware/skills" {
					hasSkills = true
				}
				if name == "genkit-middleware/filesystem" && wsPath != "" {
					out = append(out, map[string]any{
						"name":   name,
						"config": map[string]any{"rootDirectory": wsPath},
					})
				} else {
					out = append(out, name)
				}
			case map[string]any:
				rawName, ok := v["name"].(string)
				if !ok || rawName == "" {
					return nil, fmt.Errorf("spec.config.use[%d]: name must be a non-empty string", i)
				}
				name := normalizeMiddlewareName(rawName)
				entry := make(map[string]any, len(v))
				for k, val := range v {
					entry[k] = val
				}
				entry["name"] = name
				if name == "genkit-middleware/skills" {
					hasSkills = true
					if cfg, ok := entry["config"].(map[string]any); ok {
						entry["config"] = resolveSkillPaths(cfg, ws, wsPath)
					} else if sp := workspaceSkillsPath(ws, wsPath); sp != "" {
						entry["config"] = map[string]any{"skillPaths": []any{sp}}
					}
				}
				if name == "genkit-middleware/filesystem" && wsPath != "" {
					cfg, _ := entry["config"].(map[string]any)
					if cfg == nil {
						cfg = make(map[string]any)
					} else {
						cloned := make(map[string]any, len(cfg))
						for k, val := range cfg {
							cloned[k] = val
						}
						cfg = cloned
					}
					rootDir, _ := cfg["rootDirectory"].(string)
					switch {
					case rootDir == "" || rootDir == "." || rootDir == "/workspace":
						cfg["rootDirectory"] = wsPath
					case !filepath.IsAbs(rootDir):
						cfg["rootDirectory"] = filepath.Join(wsPath, rootDir)
					}
					entry["config"] = cfg
				}
				out = append(out, entry)
			default:
				return nil, fmt.Errorf("spec.config.use[%d]: must be a string or object", i)
			}
		}
	}

	if !hasSkills {
		if sp := workspaceSkillsPath(ws, wsPath); sp != "" {
			out = append(out, map[string]any{
				"name":   "genkit-middleware/skills",
				"config": map[string]any{"skillPaths": []any{sp}},
			})
		}
	}

	return out, nil
}

// normalizeMiddlewareName adds the "genkit-middleware/" prefix to bare built-in
// middleware names (such as "filesystem", "skills", "retry", "fallback",
// "toolApproval") when no provider prefix is present.
func normalizeMiddlewareName(name string) string {
	if strings.Contains(name, "/") {
		return name
	}
	return "genkit-middleware/" + name
}

func resolveSkillPaths(cfg map[string]any, ws *v1alpha1.Workspace, wsPath string) map[string]any {
	cloned := make(map[string]any, len(cfg))
	for k, v := range cfg {
		cloned[k] = v
	}
	if _, ok := cloned["skillPaths"]; !ok {
		if sp := workspaceSkillsPath(ws, wsPath); sp != "" {
			cloned["skillPaths"] = []any{sp}
		}
	}
	return cloned
}

func workspaceSkillsPath(ws *v1alpha1.Workspace, wsPath string) string {
	sp := ws.GetSpec().GetSkills().GetPath()
	if sp == "" {
		return ""
	}
	if filepath.IsAbs(sp) {
		return sp
	}
	if wsPath != "" {
		return filepath.Join(wsPath, sp)
	}
	return sp
}

// writePromptFile materializes the agentSpec's Dotprompt file into promptDir
// (or copies an existing promptFile from wsPath) so Genkit's prompt registry
// loads it under spec.Name.
func (s *agentSpec) writePromptFile(promptDir, wsPath string, extraTools []string) error {
	if err := os.MkdirAll(promptDir, 0o755); err != nil {
		return fmt.Errorf("creating prompt dir: %w", err)
	}
	dest := filepath.Join(promptDir, s.Name+".prompt")

	if s.PromptFile != "" {
		src := s.PromptFile
		if !filepath.IsAbs(src) && wsPath != "" {
			src = filepath.Join(wsPath, src)
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("reading promptFile %s: %w", src, err)
		}
		return os.WriteFile(dest, data, 0o644)
	}

	fm := make(map[string]any, len(s.Frontmatter)+1)
	for k, v := range s.Frontmatter {
		fm[k] = v
	}
	if len(extraTools) > 0 {
		var tools []string
		if existing, ok := fm["tools"].([]any); ok {
			for _, t := range existing {
				if ts, ok := t.(string); ok {
					tools = append(tools, ts)
				}
			}
		}
		for _, t := range extraTools {
			if !slices.Contains(tools, t) {
				tools = append(tools, t)
			}
		}
		fm["tools"] = tools
	}

	fmBytes, err := yaml.Marshal(fm)
	if err != nil {
		return fmt.Errorf("encoding dotprompt frontmatter: %w", err)
	}

	var b strings.Builder
	b.WriteString("---\n")
	b.Write(fmBytes)
	b.WriteString("---\n")
	if s.Instruction != "" {
		b.WriteString("{{role \"system\"}}\n")
		b.WriteString(strings.TrimRight(s.Instruction, "\n"))
		b.WriteString("\n{{history}}\n")
	}

	return os.WriteFile(dest, []byte(b.String()), 0o644)
}

// workspaceMCPServers converts the Workspace's MCP servers, both static and
// registry-listed, into Genkit MCPServerConfig entries. Static servers win on
// duplicate names.
func workspaceMCPServers(mcpCfg *v1alpha1.MCPConfig) []mcp.MCPServerConfig {
	all := slices.Clone(mcpCfg.GetServers())
	for _, r := range mcpCfg.GetRegistries() {
		all = append(all, r.GetServers()...)
	}
	seen := make(map[string]bool)
	var out []mcp.MCPServerConfig
	for _, srv := range all {
		name := srv.GetName()
		if name == "" || seen[name] {
			slog.Warn("skipping MCP server with missing or duplicate name", "name", name)
			continue
		}
		switch {
		case srv.GetEndpoint() != "":
			seen[name] = true
			out = append(out, mcp.MCPServerConfig{
				Name: name,
				Config: mcp.MCPClientOptions{
					Name: name,
					StreamableHTTP: &mcp.StreamableHTTPConfig{
						BaseURL: srv.GetEndpoint(),
					},
				},
			})
		case srv.GetCommand() != "":
			seen[name] = true
			out = append(out, mcp.MCPServerConfig{
				Name: name,
				Config: mcp.MCPClientOptions{
					Name: name,
					Stdio: &mcp.StdioConfig{
						Command: srv.GetCommand(),
						Args:    srv.GetArgs(),
					},
				},
			})
		default:
			slog.Warn("skipping MCP server with neither an endpoint nor a command", "name", name)
		}
	}
	return out
}
