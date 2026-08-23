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

// Package atelocal provides a local Substrate Control gRPC server backed by Apple Container.
package atelocal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// ContainerInspectResult models the JSON structure returned by `container inspect <id>`.
type ContainerInspectResult struct {
	ID            string `json:"id"`
	Configuration struct {
		ID             string `json:"id"`
		PublishedPorts []struct {
			HostAddress   string `json:"hostAddress"`
			HostPort      int    `json:"hostPort"`
			ContainerPort int    `json:"containerPort"`
			Proto         string `json:"proto"`
		} `json:"publishedPorts"`
	} `json:"configuration"`
	Status struct {
		State    string `json:"state"`
		Networks []struct {
			Hostname    string `json:"hostname"`
			IPv4Address string `json:"ipv4Address"`
			IPv4Gateway string `json:"ipv4Gateway"`
		} `json:"networks"`
	} `json:"status"`
}

// ContainerRunner abstracts command execution of the Apple container CLI.
type ContainerRunner interface {
	Run(ctx context.Context, args ...string) (string, error)
	Inspect(ctx context.Context, containerName string) (*ContainerInspectResult, error)
}

type defaultContainerRunner struct {
	binaryPath string
}

// NewDefaultContainerRunner creates a ContainerRunner that calls the Apple `container` binary.
func NewDefaultContainerRunner(binPath string) ContainerRunner {
	if binPath == "" {
		if p, err := exec.LookPath("container"); err == nil {
			binPath = p
		} else {
			binPath = "/usr/local/bin/container"
		}
	}
	return &defaultContainerRunner{binaryPath: binPath}
}

func (r *defaultContainerRunner) Run(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, r.binaryPath, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("container %s failed: %w (stderr: %s)", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func (r *defaultContainerRunner) Inspect(ctx context.Context, containerName string) (*ContainerInspectResult, error) {
	out, err := r.Run(ctx, "inspect", containerName)
	if err != nil {
		return nil, err
	}

	var results []ContainerInspectResult
	if err := json.Unmarshal([]byte(out), &results); err != nil {
		return nil, fmt.Errorf("failed to parse container inspect json: %w", err)
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("container %s not found in inspect output", containerName)
	}
	return &results[0], nil
}

// CleanIPv4 extracts the raw IPv4 address from CIDR notation (e.g. "192.168.64.2/24" -> "192.168.64.2").
func CleanIPv4(cidrOrIP string) string {
	parts := strings.Split(cidrOrIP, "/")
	return strings.TrimSpace(parts[0])
}
