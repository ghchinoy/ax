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

// Package config provides configuration structures for AX server.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/google/ax/internal/agent"
	"github.com/google/ax/internal/auth"
	"gopkg.in/yaml.v3"
)

// Config represents the main configuration for AX server.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	EventLog EventLogConfig `yaml:"eventlog"`
	Planner  PlannerConfig  `yaml:"planner,omitempty"`
	Registry RegistryConfig `yaml:"registry,omitempty"`
	ATE      ATEConfig      `yaml:"ate,omitempty"`
}

// RegistryConfig allows registring agents.
type RegistryConfig struct {
	RemoteAgents         []RemoteAgentConfig         `yaml:"remote_agents,omitempty"`
	ColabAgents          []ColabAgentConfig          `yaml:"colab_agents,omitempty"`
	SubstrateAgents      []SubstrateAgentConfig      `yaml:"substrate_agents,omitempty"`
	AppleContainerAgents []AppleContainerAgentConfig `yaml:"apple_container_agents,omitempty"`
}

// ATEConfig configures the ATE integration.
type ATEConfig struct {
	Endpoint string `yaml:"endpoint"`
}

// ServerConfig configures the gRPC server.
type ServerConfig struct {
	Address string `yaml:"address"` // Server address to listen on (e.g., ":8494")
}

type SQLiteConfig struct {
	Filename string `yaml:"filename"` // SQLite file for event log storage
}

// EventLogConfig configures the event log storage.
type EventLogConfig struct {
	SQLiteConfig SQLiteConfig `yaml:"sqlite"`
}

// PlannerConfig configures the planner.
type PlannerConfig struct {
	Type   string              `yaml:"type"` // "gemini"
	Gemini GeminiPlannerConfig `yaml:"gemini,omitempty"`
}

// GeminiPlannerConfig configures the Gemini-based planner.
// Note: API key is not configurable here for security reasons.
// Set GEMINI_API_KEY environment variable instead.
type GeminiPlannerConfig struct {
	Model        string  `yaml:"model,omitempty"` // Model name
	Temperature  float32 `yaml:"temperature,omitempty"`
	MaxTokens    int32   `yaml:"max_tokens,omitempty"`
	Timeout      string  `yaml:"timeout,omitempty"`
	SystemPrompt string  `yaml:"system_prompt,omitempty"`
	SkillsDir    string  `yaml:"skills_dir,omitempty"` // Directory to discover skills from. If omitted, falls back to SKILLS_DIR env var or ~/.agents/skills.
}

func (c *GeminiPlannerConfig) setDefaults() {
	if c.Model == "" {
		c.Model = "gemini-3.5-flash"
	}
	if c.Timeout == "" {
		c.Timeout = "30s"
	}
}

// GeminiConfig is the configuration for a Gemini agent execution.
type GeminiConfig struct {
	Model        string        `json:"model,omitempty" yaml:"model,omitempty"`
	SystemPrompt string        `json:"system_prompt,omitempty" yaml:"system_prompt,omitempty"`
	MaxTokens    int32         `json:"max_tokens,omitempty" yaml:"max_tokens,omitempty"`
	Temperature  float32       `json:"temperature,omitempty" yaml:"temperature,omitempty"` // 0 means use model default
	Timeout      time.Duration `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Tools        []string      `json:"tools,omitempty" yaml:"tools,omitempty"`
}

// SubstrateAgentConfig allows registering a new agent with an ATE actor template.
type SubstrateAgentConfig struct {
	ID          string            `yaml:"id"`                 // Unique agent identifier
	Name        string            `yaml:"name"`               // Human-readable name
	Description string            `yaml:"description"`        // Description of agent capabilities
	Port        int               `yaml:"port"`               // Port the agent is listening on
	Namespace   string            `yaml:"namespace"`          // Namespace for actor
	Template    string            `yaml:"template"`           // Template for actor
	Protocol    string            `yaml:"protocol,omitempty"` // "axp" (default) or "a2a"
	Auth        auth.Auth         `yaml:"auth,omitempty"`     // Optional auth
	Headers     auth.Headers      `yaml:"headers,omitempty"`  // Optional headers
	Metadata    map[string]string `yaml:"metadata,omitempty"` // Optional metadata
	// TODO(jbd): Rename this struct before releasing.
}

// RemoteAgentConfig configures a remote agent to register on startup.
type RemoteAgentConfig struct {
	ID          string            `yaml:"id"`                 // Unique agent identifier
	Name        string            `yaml:"name"`               // Human-readable name
	Description string            `yaml:"description"`        // Description
	Address     string            `yaml:"address"`            // Remote agent address
	Protocol    string            `yaml:"protocol,omitempty"` // "axp" (default) or "a2a"
	Auth        auth.Auth         `yaml:"auth,omitempty"`     // Optional auth (cross-protocol; today honored only for a2a)
	Headers     auth.Headers      `yaml:"headers,omitempty"`  // Optional headers (cross-protocol; today honored only for a2a)
	A2A         A2AConfig         `yaml:"a2a,omitempty"`      // A2A-protocol-specific options
	Metadata    map[string]string `yaml:"metadata,omitempty"` // Optional metadata
}

// A2AConfig holds A2A-protocol-specific options. Honored only when
// RemoteAgentConfig.Protocol is "a2a".
type A2AConfig struct {
	Stateless bool `yaml:"stateless,omitempty"` // Send full history each turn (default: stateful)
}

// ColabAgentConfig configures a Colab agent to register on startup.
type ColabAgentConfig struct {
	ID              string            `yaml:"id"`                          // Unique agent identifier
	Name            string            `yaml:"name"`                        // Human-readable name
	Description     string            `yaml:"description"`                 // Description of agent capabilities
	LocalFile       string            `yaml:"local_file,omitempty"`        // Path to local .py or .ipynb file (uploaded to VM)
	DriveFile       string            `yaml:"drive_file,omitempty"`        // Path to .ipynb file in Google Drive (e.g. MyDrive/notebooks/nb.ipynb)
	Accelerator     string            `yaml:"accelerator,omitempty"`       // Accelerator type (optional), e.g. "tpu-v5e1", "gpu-A100"
	DriveMountPath  string            `yaml:"drive_mount_path,omitempty"`  // Path to mount Google Drive (optional), default: "/content/drive"
	Requirements    string            `yaml:"requirements,omitempty"`      // Path to requirements.txt (optional)
	InputFlag       string            `yaml:"input_flag,omitempty"`        // Input parameter name (optional). For .py, passed as --<name>. For .ipynb, set as a variable before %run
	OutputImage     string            `yaml:"output_image,omitempty"`      // Local path to download the output image to
	OutputDrivePath string            `yaml:"output_drive_path,omitempty"` // Google Drive path to save converted .ipynb (e.g. MyDrive/notebooks/out.ipynb)
	Metadata        map[string]string `yaml:"metadata,omitempty"`          // Optional metadata
}

// AppleContainerAgentConfig configures an Apple Container Sandbox Agent.
type AppleContainerAgentConfig struct {
	ID          string            `yaml:"id"`                    // Unique agent identifier
	Name        string            `yaml:"name"`                  // Human-readable name
	Description string            `yaml:"description"`           // Description of agent capabilities
	Image       string            `yaml:"image"`                 // Container image name (e.g. debian:12, ax-agent:v1)
	Mode        string            `yaml:"mode,omitempty"`        // "service" or "sandbox" / "exec"
	Port        int               `yaml:"port,omitempty"`        // Container gRPC port for service mode
	HostPort    int               `yaml:"host_port,omitempty"`   // Host port to map (0 for auto-allocated)
	Command     []string          `yaml:"command,omitempty"`     // Command or entrypoint arguments
	WorkDir     string            `yaml:"work_dir,omitempty"`    // Working directory inside container
	Env         map[string]string `yaml:"env,omitempty"`         // Environment variables
	Volumes     []string          `yaml:"volumes,omitempty"`     // Volumes (e.g. "hostPath:containerPath")
	CPUs        int               `yaml:"cpus,omitempty"`        // CPUs allocation
	Memory      string            `yaml:"memory,omitempty"`      // Memory limit (e.g. "1024M", "2G")
	KeepAlive   bool              `yaml:"keep_alive,omitempty"`  // Preserve container across turns in a conversation
	BinaryPath  string            `yaml:"binary_path,omitempty"` // Path to container CLI binary
	Metadata    map[string]string `yaml:"metadata,omitempty"`    // Optional metadata
}

type LocalAgentConfig struct {
	ID          string            `yaml:"id"`                 // Unique agent identifier
	Name        string            `yaml:"name"`               // Human-readable name
	Description string            `yaml:"description"`        // Description of agent capabilities
	Metadata    map[string]string `yaml:"metadata,omitempty"` // Optional metadata
	Agent       agent.Agent       // In-process agent instance (not in YAML)
}

// LoadFromFile loads configuration from a YAML file.
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set defaults
	cfg.setDefaults()

	return &cfg, nil
}

// DefaultConfig returns a configuration with default values set.
func DefaultConfig() *Config {
	var cfg Config
	cfg.setDefaults()
	return &cfg
}

// setDefaults sets default values for optional fields.
func (c *Config) setDefaults() {
	// Server defaults
	if c.Server.Address == "" {
		c.Server.Address = ":8494"
	}

	if c.EventLog.SQLiteConfig.Filename == "" {
		c.EventLog.SQLiteConfig.Filename = "eventlog/log.sqlite"
	}

	// Planner defaults
	if c.Planner.Type == "" {
		c.Planner.Type = "gemini"
	}
	if c.Planner.Type == "gemini" {
		c.Planner.Gemini.setDefaults()
	}

}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if c.Server.Address == "" {
		return fmt.Errorf("server.address is required")
	}
	if c.EventLog.SQLiteConfig.Filename == "" {
		return fmt.Errorf("eventlog.sqlite.filename is required")
	}

	return nil
}
