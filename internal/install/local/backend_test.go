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

package local

import (
	"context"
	"strings"
	"testing"

	"github.com/google/ax/internal/install/cluster"
)

func TestNormalizeBackend(t *testing.T) {
	for input, want := range map[string]string{
		"":                BackendKind,
		"kind":            BackendKind,
		"KIND":            BackendKind,
		"ate-local":       BackendAteLocal,
		"atelocal":        BackendAteLocal,
		"apple-container": BackendAteLocal,
	} {
		got, err := NormalizeBackend(input)
		if err != nil || got != want {
			t.Errorf("NormalizeBackend(%q) = (%q, %v), want (%q, nil)", input, got, err, want)
		}
	}
	if _, err := NormalizeBackend("bogus"); err == nil {
		t.Error("NormalizeBackend(bogus) succeeded, want error")
	}
}

func TestAteLocalConfigAndStatusLifecycle(t *testing.T) {
	t.Setenv("AX_HOME", t.TempDir())

	b := NewAteLocalBackend(nil)
	st, err := b.Status(context.Background(), "bench-1rx6")
	if err != nil {
		t.Fatalf("Status(absent): %v", err)
	}
	if st.Backend != BackendAteLocal || st.State != cluster.StateAbsent {
		t.Fatalf("Status(absent) = %+v, want ate-local / NotFound", st)
	}
	if HasAteLocalState("bench-1rx6") {
		t.Fatal("HasAteLocalState should be false before state is written")
	}

	dir, err := AteLocalStateDir("bench-1rx6")
	if err != nil {
		t.Fatal(err)
	}
	state := AteLocalState{
		Name:            "bench-1rx6",
		Runtime:         "apple",
		PostgresName:    "ax-bench-1rx6-postgres",
		PostgresImage:   defaultPostgresImage,
		ContainerPrefix: "ate-bench-1rx6",
		PostgresPort:    5446,
		SubstratePort:   50076,
		ServerPort:      8099,
	}
	if err := saveAteLocalState(dir, state); err != nil {
		t.Fatalf("saveAteLocalState: %v", err)
	}
	if !HasAteLocalState("bench-1rx6") {
		t.Fatal("HasAteLocalState should be true after state is saved")
	}

	cfg := renderAteLocalConfig(state, "/tmp/snaps", "/tmp/bin/ax-runner")
	for _, want := range []string{
		`address: "127.0.0.1:50076"`,
		`container_prefix: "ate-bench-1rx6"`,
		`/tmp/bin/ax-runner:/usr/local/bin/ax-runner`,
	} {
		if !strings.Contains(cfg, want) {
			t.Errorf("renderAteLocalConfig missing %q:\n%s", want, cfg)
		}
	}

	st, err = b.Status(context.Background(), "bench-1rx6")
	if err != nil {
		t.Fatalf("Status(stopped): %v", err)
	}
	if st.State != cluster.StateStopped {
		t.Fatalf("Status(stopped) = %+v, want Stopped", st)
	}

	raw := []byte(`[{"configuration":{"id":"pg1"},"status":{"state":"running"}},{"configuration":{"id":"pg2"},"status":"stopped"}]`)
	if got := containerStateFromJSON(raw, "pg1"); got != "running" {
		t.Errorf("containerStateFromJSON(pg1) = %q, want running", got)
	}
	if got := containerStateFromJSON(raw, "pg2"); got != "stopped" {
		t.Errorf("containerStateFromJSON(pg2) = %q, want stopped", got)
	}
	if got := containerStateFromJSON(raw, "missing"); got != "" {
		t.Errorf("containerStateFromJSON(missing) = %q, want empty", got)
	}
}
