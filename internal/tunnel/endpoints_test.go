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
	"context"
	"path/filepath"
	"testing"
)

func TestLocalEndpointLifecycleAndResolution(t *testing.T) {
	axHome := t.TempDir()
	t.Setenv("AX_HOME", axHome)
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "nonexistent"))
	t.Setenv("KUBECONTEXT", "")
	t.Setenv("AX_SERVER", "")

	ep := LocalEndpoint{
		Name:       "ax-dev",
		Backend:    "ate-local",
		ServerURL:  "http://127.0.0.1:8096",
		RouterAddr: "127.0.0.1:50061",
	}
	if err := SaveLocalEndpoint(ep); err != nil {
		t.Fatalf("SaveLocalEndpoint: %v", err)
	}
	if err := SetActiveLocalContext("ax-dev"); err != nil {
		t.Fatalf("SetActiveLocalContext: %v", err)
	}

	gotCtx, err := CurrentContext("")
	if err != nil || gotCtx != "ax-dev" {
		t.Fatalf("CurrentContext() = (%q, %v), want (ax-dev, nil)", gotCtx, err)
	}

	gotURL, err := EnsureServerURL(context.Background(), Options{FromEnv: true})
	if err != nil || gotURL != ep.ServerURL {
		t.Fatalf("EnsureServerURL() = (%q, %v), want (%q, nil)", gotURL, err, ep.ServerURL)
	}

	if gotRouter := LocalEndpointRouter(""); gotRouter != ep.RouterAddr {
		t.Fatalf("LocalEndpointRouter() = %q, want %q", gotRouter, ep.RouterAddr)
	}

	entries, err := ListContexts("")
	if err != nil {
		t.Fatalf("ListContexts: %v", err)
	}
	if len(entries) != 1 || entries[0].Name != "ax-dev" || !entries[0].Current {
		t.Fatalf("ListContexts() = %+v, want single active ax-dev entry", entries)
	}

	if err := SetCurrentContext("ax-dev"); err != nil {
		t.Fatalf("SetCurrentContext(ax-dev): %v", err)
	}

	if err := DeleteLocalEndpoint("ax-dev"); err != nil {
		t.Fatalf("DeleteLocalEndpoint: %v", err)
	}
	if got := ActiveLocalContext(); got != "" {
		t.Fatalf("ActiveLocalContext() after delete = %q, want empty", got)
	}
	if got, err := GetLocalEndpoint("ax-dev"); err != nil || got != nil {
		t.Fatalf("GetLocalEndpoint after delete = (%+v, %v), want (nil, nil)", got, err)
	}
}
