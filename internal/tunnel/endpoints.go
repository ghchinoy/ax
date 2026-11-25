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

package tunnel

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// LocalEndpoint records a non-Kubernetes local AX environment (such as
// `ax local start --backend=ate-local`) under $AX_HOME/endpoints/<name>.json
// so the ax CLI, ax ctx, ax ssh, and pkg/client can target it without
// requiring $AX_SERVER, $ATENET_ROUTER_ADDR, or a kubeconfig entry.
type LocalEndpoint struct {
	Name       string `json:"name"`
	Backend    string `json:"backend"`
	ServerURL  string `json:"serverUrl"`
	RouterAddr string `json:"routerAddr,omitempty"`
}

// AXHomeDir returns the base AX state directory ($AX_HOME or ~/.ax).
func AXHomeDir() (string, error) {
	if dir := os.Getenv("AX_HOME"); dir != "" {
		return dir, os.MkdirAll(dir, 0o755)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	p := filepath.Join(home, ".ax")
	return p, os.MkdirAll(p, 0o755)
}

// EndpointsDir returns the directory storing local non-K8s endpoint records.
func EndpointsDir() (string, error) {
	base, err := AXHomeDir()
	if err != nil {
		return "", err
	}
	p := filepath.Join(base, "endpoints")
	return p, os.MkdirAll(p, 0o755)
}

func endpointPath(name string) (string, error) {
	dir, err := EndpointsDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, SanitizeContext(name)+".json"), nil
}

func currentLocalContextPath() (string, error) {
	base, err := AXHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "current-context"), nil
}

// SaveLocalEndpoint persists ep under $AX_HOME/endpoints/<name>.json.
func SaveLocalEndpoint(ep LocalEndpoint) error {
	ep.Name = strings.TrimSpace(ep.Name)
	if ep.Name == "" {
		return fmt.Errorf("local endpoint name must not be empty")
	}
	if strings.TrimSpace(ep.ServerURL) == "" {
		return fmt.Errorf("local endpoint serverUrl must not be empty")
	}
	path, err := endpointPath(ep.Name)
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(ep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

// GetLocalEndpoint loads a saved local endpoint by context name. Returns (nil, nil)
// when no local endpoint exists for name.
func GetLocalEndpoint(name string) (*LocalEndpoint, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, nil
	}
	path, err := endpointPath(name)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var ep LocalEndpoint
	if err := json.Unmarshal(data, &ep); err != nil {
		return nil, fmt.Errorf("parsing local endpoint %q: %w", name, err)
	}
	if ep.Name != name {
		return nil, nil
	}
	return &ep, nil
}

// ListLocalEndpoints returns all saved local endpoints sorted by name.
func ListLocalEndpoints() ([]LocalEndpoint, error) {
	dir, err := EndpointsDir()
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []LocalEndpoint
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		var ep LocalEndpoint
		if err := json.Unmarshal(data, &ep); err != nil || ep.Name == "" {
			continue
		}
		out = append(out, ep)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// DeleteLocalEndpoint removes the saved endpoint record for name and clears the
// active local context marker if it pointed to name.
func DeleteLocalEndpoint(name string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil
	}
	path, err := endpointPath(name)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if ActiveLocalContext() == name {
		_ = ClearActiveLocalContext()
	}
	return nil
}

// SetActiveLocalContext selects a saved local endpoint as the active AX context.
func SetActiveLocalContext(name string) error {
	ep, err := GetLocalEndpoint(name)
	if err != nil {
		return err
	}
	if ep == nil {
		return fmt.Errorf("no local endpoint exists with name %q", name)
	}
	path, err := currentLocalContextPath()
	if err != nil {
		return err
	}
	return os.WriteFile(path, []byte(ep.Name+"\n"), 0o600)
}

// ClearActiveLocalContext clears the active non-K8s local context selection.
func ClearActiveLocalContext() error {
	path, err := currentLocalContextPath()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// ActiveLocalContext returns the active local endpoint name if set and still
// backed by a saved endpoint JSON file, or "" otherwise.
func ActiveLocalContext() string {
	path, err := currentLocalContextPath()
	if err != nil {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	name := strings.TrimSpace(string(data))
	if name == "" {
		return ""
	}
	ep, err := GetLocalEndpoint(name)
	if err != nil || ep == nil {
		return ""
	}
	return ep.Name
}

// LocalEndpointRouter returns the atenet-router address for the selected
// context (override, $KUBECONTEXT, or active local context) when it refers to a
// saved non-K8s local endpoint, or "" otherwise.
func LocalEndpointRouter(override string) string {
	ctxName, err := CurrentContext(override)
	if err != nil || ctxName == "" {
		return ""
	}
	ep, err := GetLocalEndpoint(ctxName)
	if err != nil || ep == nil {
		return ""
	}
	return strings.TrimSpace(ep.RouterAddr)
}
