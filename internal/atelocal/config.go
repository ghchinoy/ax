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
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// TemplateConfig defines container parameters for an ActorTemplate.
type TemplateConfig struct {
	Image   string            `yaml:"image"`
	Port    int               `yaml:"port,omitempty"` // HarnessService port in container (default 50053)
	Command []string          `yaml:"command,omitempty"`
	WorkDir string            `yaml:"work_dir,omitempty"`
	Env     map[string]string `yaml:"env,omitempty"`
	Volumes []string          `yaml:"volumes,omitempty"`
	CPUs    int               `yaml:"cpus,omitempty"`
	Memory  string            `yaml:"memory,omitempty"`
}

// Config configures the local Substrate Control server.
type Config struct {
	Address         string                    `yaml:"address"` // gRPC listen address (default ":50051")
	DefaultTemplate string                    `yaml:"default_template,omitempty"`
	Templates       map[string]TemplateConfig `yaml:"templates,omitempty"`
	ReadyTimeout    time.Duration             `yaml:"ready_timeout,omitempty"`
	BinaryPath      string                    `yaml:"binary_path,omitempty"`

	Runner ContainerRunner `yaml:"-"` // Custom runner for unit testing
}

// LoadConfig loads configuration from a YAML file or returns defaults.
func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	cfg.setDefaults()
	return cfg, nil
}

// DefaultConfig returns default configuration.
func DefaultConfig() *Config {
	cfg := &Config{
		Address:      ":50051",
		ReadyTimeout: 20 * time.Second,
		Templates:    make(map[string]TemplateConfig),
	}
	cfg.setDefaults()
	return cfg
}

func (c *Config) setDefaults() {
	if c.Address == "" {
		c.Address = ":50051"
	}
	if c.ReadyTimeout == 0 {
		c.ReadyTimeout = 20 * time.Second
	}
	if c.Templates == nil {
		c.Templates = make(map[string]TemplateConfig)
	}

	// Default template if none provided
	if _, ok := c.Templates["default"]; !ok {
		c.Templates["default"] = TemplateConfig{
			Image: "debian:12",
			Port:  50053,
		}
	}
}
