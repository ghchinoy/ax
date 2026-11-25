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

// Package local defines the Kubernetes-agnostic local environment lifecycle
// interface for `ax local` (above internal/install/cluster.Provider) and
// provides both the Kind backend and the zero-Kubernetes ate-local backend.
package local

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/ax/internal/install/cluster"
)

const (
	// BackendKind provisions a local Kind cluster + Agent Substrate + AX.
	BackendKind = "kind"
	// BackendAteLocal provisions Postgres + ate-local + ax-server natively
	// without Kubernetes (Apple container or Podman).
	BackendAteLocal = "ate-local"
)

// NormalizeBackend canonicalizes a --backend flag value (defaulting empty to kind).
func NormalizeBackend(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", BackendKind:
		return BackendKind, nil
	case BackendAteLocal, "atelocal", "apple-container":
		return BackendAteLocal, nil
	default:
		return "", fmt.Errorf("unsupported --backend %q (expected: %q or %q)", raw, BackendKind, BackendAteLocal)
	}
}

// Endpoint describes a running local AX environment returned by Backend.Start.
type Endpoint struct {
	Name        string
	Backend     string
	ServerURL   string // Non-empty for direct non-K8s backends (e.g. http://127.0.0.1:8090)
	RouterAddr  string // Non-empty for direct atenet-router backends (e.g. 127.0.0.1:50051)
	KubeContext string // Active context name (kubeconfig context or local endpoint name)
}

// StartOptions configures Backend.Start.
type StartOptions struct {
	Name              string
	Workers           int
	AXNamespace       string
	ServerImage       string
	RunnerVolumeImage string

	// Optional overrides for the ate-local backend.
	Runtime       string // "apple" (default on darwin) or "podman"
	PostgresPort  int
	SubstratePort int
	ServerPort    int
}

// Backend manages a complete single-user AX environment for `ax local`.
type Backend interface {
	Name() string
	Start(ctx context.Context, opts StartOptions) (Endpoint, error)
	Stop(ctx context.Context, name string) error
	Status(ctx context.Context, name string) (cluster.Status, error)
	Delete(ctx context.Context, name string) error
}
