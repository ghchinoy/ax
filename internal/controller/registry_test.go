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

package controller

// TODO(lhuan): Setup a better automated testing framework

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc"

	"github.com/google/ax/internal/config"
	"github.com/google/ax/proto"
)

type mockAgentServer struct {
	proto.UnimplementedAgentServiceServer
	healthy bool
}

func startMockGRPCServer(t *testing.T, healthy bool) (string, func()) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterAgentServiceServer(s, &mockAgentServer{healthy: healthy})
	go func() {
		if err := s.Serve(lis); err != nil {
			// server might be closed
		}
	}()
	return lis.Addr().String(), func() {
		s.Stop()
		lis.Close()
	}
}

func TestRegistry_GracefulShutdown(t *testing.T) {
	r := NewRegistry()

	address, cleanup := startMockGRPCServer(t, true)
	defer cleanup()

	// Register multiple agents to create workload
	for i := range 50 {
		err := r.RegisterRemote(context.Background(), config.RemoteAgentConfig{
			ID:      fmt.Sprintf("remote-shutdown-test-%d", i),
			Name:    "Shutdown Test Remote",
			Address: address,
		})
		if err != nil {
			t.Fatalf("Failed to register remote agent for shutdown test: %v", err)
		}
	}

	// Close should return specific errors for failed agents, but NOT panic or deadlock
	// We are testing for absence of panic/deadlock here.
	_ = r.Close()
}

func TestRegistry_RegisterAppleContainer(t *testing.T) {
	r := NewRegistry()

	err := r.RegisterAppleContainer(config.AppleContainerAgentConfig{
		ID:          "apple-sandbox",
		Name:        "Apple Sandbox",
		Description: "Sandbox running in Apple container",
		Image:       "debian:12",
		Mode:        "sandbox",
	})
	if err != nil {
		t.Fatalf("failed to register apple container agent: %v", err)
	}

	ag, err := r.Get("apple-sandbox")
	if err != nil {
		t.Fatalf("failed to get registered agent: %v", err)
	}
	if ag == nil {
		t.Fatal("agent is nil")
	}

	info, err := r.GetInfo("apple-sandbox")
	if err != nil {
		t.Fatalf("failed to get agent info: %v", err)
	}
	if info.Name != "Apple Sandbox" {
		t.Errorf("expected name 'Apple Sandbox', got %q", info.Name)
	}

	// Duplicate registration should fail
	err = r.RegisterAppleContainer(config.AppleContainerAgentConfig{
		ID:    "apple-sandbox",
		Image: "debian:12",
	})
	if err == nil {
		t.Error("expected error on duplicate ID registration")
	}
}
