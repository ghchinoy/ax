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
	"sort"
	"strconv"
	"strings"
)

const (
	// RuntimeApple selects the native macOS Apple `container` CLI runtime.
	RuntimeApple = "apple"
	// RuntimePodman selects the Podman CLI runtime.
	RuntimePodman = "podman"
)

// NormalizeRuntime canonicalizes a runtime name into RuntimeApple or RuntimePodman.
func NormalizeRuntime(runtime string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(runtime)) {
	case "", "apple", "apple-container", "container":
		return RuntimeApple, nil
	case "podman":
		return RuntimePodman, nil
	default:
		return "", fmt.Errorf("unsupported container runtime %q (want %q or %q)", runtime, RuntimeApple, RuntimePodman)
	}
}

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

// CleanIPv4 extracts the raw IPv4 address from CIDR notation (e.g. "192.168.64.2/24" -> "192.168.64.2").
func CleanIPv4(cidrOrIP string) string {
	parts := strings.Split(cidrOrIP, "/")
	return strings.TrimSpace(parts[0])
}

// NewContainerRunner returns a ContainerRunner for the specified runtime ("apple" or "podman").
func NewContainerRunner(runtime, binPath string) (ContainerRunner, error) {
	norm, err := NormalizeRuntime(runtime)
	if err != nil {
		return nil, err
	}
	if norm == RuntimePodman {
		return NewPodmanContainerRunner(binPath), nil
	}
	return NewDefaultContainerRunner(binPath), nil
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

// NewPodmanContainerRunner creates a ContainerRunner backed by the `podman` CLI.
func NewPodmanContainerRunner(binPath string) ContainerRunner {
	if binPath == "" {
		if p, err := exec.LookPath("podman"); err == nil {
			binPath = p
		} else {
			binPath = "podman"
		}
	}
	return &podmanContainerRunner{binaryPath: binPath}
}

type defaultContainerRunner struct {
	binaryPath       string
	detectedInitOnce bool
	detectedInit     string
}

func (r *defaultContainerRunner) Run(ctx context.Context, args ...string) (string, error) {
	out, err := r.execCmd(ctx, args...)
	if err != nil && len(args) > 0 && args[0] == "run" && !containsArg(args, "--init-image") &&
		strings.Contains(err.Error(), "image snapshot for ghcr.io/apple/containerization/vminit") {
		if initImg := r.detectLocalVminitImage(ctx); initImg != "" {
			retryArgs := make([]string, 0, len(args)+2)
			retryArgs = append(retryArgs, "run", "--init-image", initImg)
			retryArgs = append(retryArgs, args[1:]...)
			return r.execCmd(ctx, retryArgs...)
		}
	}
	return out, err
}

func containsArg(args []string, flag string) bool {
	for _, a := range args {
		if a == flag || strings.HasPrefix(a, flag+"=") {
			return true
		}
	}
	return false
}

func (r *defaultContainerRunner) detectLocalVminitImage(ctx context.Context) string {
	if r.detectedInitOnce {
		return r.detectedInit
	}
	r.detectedInitOnce = true
	out, err := r.execCmd(ctx, "image", "list")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "ghcr.io/apple/containerization/vminit" && fields[1] != "TAG" {
			r.detectedInit = fmt.Sprintf("%s:%s", fields[0], fields[1])
			return r.detectedInit
		}
	}
	return ""
}

func (r *defaultContainerRunner) execCmd(ctx context.Context, args ...string) (string, error) {
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
		return nil, fmt.Errorf("container %q not found in inspect output", containerName)
	}
	return &results[0], nil
}

type podmanContainerRunner struct {
	binaryPath string
}

// translatePodmanArgs converts Apple `container` CLI arguments to `podman` CLI arguments.
func translatePodmanArgs(args []string) []string {
	if len(args) == 0 {
		return nil
	}
	switch args[0] {
	case "delete":
		out := make([]string, len(args))
		copy(out, args)
		out[0] = "rm"
		return out
	case "run":
		out := make([]string, 0, len(args)+2)
		out = append(out, "run")
		hasPublish := false
		for i := 1; i < len(args); i++ {
			a := args[i]
			if a == "--progress" && i+1 < len(args) {
				i++
				continue
			}
			if strings.HasPrefix(a, "--progress=") {
				continue
			}
			if a == "--init-image" && i+1 < len(args) {
				i++
				continue
			}
			if strings.HasPrefix(a, "--init-image=") {
				continue
			}
			if a == "-p" || a == "--publish" || strings.HasPrefix(a, "-p=") || strings.HasPrefix(a, "--publish=") {
				hasPublish = true
			}
			out = append(out, a)
		}
		if !hasPublish {
			withPort := make([]string, 0, len(out)+2)
			withPort = append(withPort, "run", "-p", "127.0.0.1::80")
			withPort = append(withPort, out[1:]...)
			out = withPort
		}
		return out
	default:
		out := make([]string, len(args))
		copy(out, args)
		return out
	}
}

func (r *podmanContainerRunner) Run(ctx context.Context, args ...string) (string, error) {
	podmanArgs := translatePodmanArgs(args)
	cmd := exec.CommandContext(ctx, r.binaryPath, podmanArgs...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("podman %s failed: %w (stderr: %s)", strings.Join(podmanArgs, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

type podmanInspectEntry struct {
	ID    string `json:"Id"`
	State struct {
		Status  string `json:"Status"`
		Running bool   `json:"Running"`
	} `json:"State"`
	NetworkSettings struct {
		IPAddress string `json:"IPAddress"`
		Ports     map[string][]struct {
			HostIP   string `json:"HostIp"`
			HostPort string `json:"HostPort"`
		} `json:"Ports"`
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
			Gateway   string `json:"Gateway"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

func parsePodmanInspect(raw string, containerName string) (*ContainerInspectResult, error) {
	var entries []podmanInspectEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		return nil, fmt.Errorf("failed to parse podman inspect json: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("container %q not found in podman inspect output", containerName)
	}
	e := entries[0]
	res := &ContainerInspectResult{
		ID: e.ID,
	}
	res.Configuration.ID = e.ID
	state := strings.ToLower(strings.TrimSpace(e.State.Status))
	if state == "" && e.State.Running {
		state = "running"
	}
	res.Status.State = state

	// Prefer published host port so macOS callers can reach into the Podman Machine VM via 127.0.0.1:<hostPort>.
	portKeys := make([]string, 0, len(e.NetworkSettings.Ports))
	for k := range e.NetworkSettings.Ports {
		portKeys = append(portKeys, k)
	}
	sort.Slice(portKeys, func(i, j int) bool {
		if portKeys[i] == "80/tcp" {
			return true
		}
		if portKeys[j] == "80/tcp" {
			return false
		}
		return portKeys[i] < portKeys[j]
	})

	var routedAddr string
	for _, key := range portKeys {
		bindings := e.NetworkSettings.Ports[key]
		parts := strings.SplitN(key, "/", 2)
		cPort, _ := strconv.Atoi(parts[0])
		proto := "tcp"
		if len(parts) == 2 && parts[1] != "" {
			proto = parts[1]
		}
		for _, b := range bindings {
			hPort, _ := strconv.Atoi(b.HostPort)
			if hPort <= 0 {
				continue
			}
			hostIP := strings.TrimSpace(b.HostIP)
			if hostIP == "" || hostIP == "0.0.0.0" || hostIP == "::" {
				hostIP = "127.0.0.1"
			}
			res.Configuration.PublishedPorts = append(res.Configuration.PublishedPorts, struct {
				HostAddress   string `json:"hostAddress"`
				HostPort      int    `json:"hostPort"`
				ContainerPort int    `json:"containerPort"`
				Proto         string `json:"proto"`
			}{
				HostAddress:   hostIP,
				HostPort:      hPort,
				ContainerPort: cPort,
				Proto:         proto,
			})
			if routedAddr == "" {
				routedAddr = fmt.Sprintf("%s:%d", hostIP, hPort)
			}
		}
	}

	if routedAddr == "" {
		routedAddr = CleanIPv4(e.NetworkSettings.IPAddress)
	}
	if routedAddr == "" {
		netNames := make([]string, 0, len(e.NetworkSettings.Networks))
		for n := range e.NetworkSettings.Networks {
			netNames = append(netNames, n)
		}
		sort.Strings(netNames)
		for _, n := range netNames {
			if ip := CleanIPv4(e.NetworkSettings.Networks[n].IPAddress); ip != "" {
				routedAddr = ip
				break
			}
		}
	}
	if routedAddr != "" {
		res.Status.Networks = append(res.Status.Networks, struct {
			Hostname    string `json:"hostname"`
			IPv4Address string `json:"ipv4Address"`
			IPv4Gateway string `json:"ipv4Gateway"`
		}{
			Hostname:    containerName,
			IPv4Address: routedAddr,
		})
	}
	return res, nil
}

func (r *podmanContainerRunner) Inspect(ctx context.Context, containerName string) (*ContainerInspectResult, error) {
	out, err := r.Run(ctx, "inspect", containerName)
	if err != nil {
		return nil, err
	}
	return parsePodmanInspect(out, containerName)
}
