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
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ActorRecord holds state for a managed actor in the local Control plane.
type ActorRecord struct {
	Atespace          string
	Name              string
	TemplateNamespace string
	TemplateName      string
	ContainerName     string
	ContainerIP       string
	Port              int
	Status            ateapipb.Actor_Status
	CreatedAt         time.Time
}

func (r *ActorRecord) toProto() *ateapipb.Actor {
	return &ateapipb.Actor{
		ActorId:                r.Name,
		Atespace:               r.Atespace,
		ActorTemplateNamespace: r.TemplateNamespace,
		ActorTemplateName:      r.TemplateName,
		AteomPodName:           r.ContainerName,
		AteomPodIp:             r.ContainerIP,
		Status:                 r.Status,
	}
}

// Server implements ateapipb.ControlServer for local macOS environments backed by Apple container.
type Server struct {
	ateapipb.UnimplementedControlServer

	cfg    *Config
	runner ContainerRunner

	mu        sync.RWMutex
	atespaces map[string]bool
	actors    map[string]*ActorRecord // key: atespace/name
}

// NewServer creates a new local Substrate Control server instance.
func NewServer(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	runner := cfg.Runner
	if runner == nil {
		runner = NewDefaultContainerRunner(cfg.BinaryPath)
	}

	s := &Server{
		cfg:       cfg,
		runner:    runner,
		atespaces: make(map[string]bool),
		actors:    make(map[string]*ActorRecord),
	}

	// Default atespace
	s.atespaces["default"] = true
	s.atespaces["ax"] = true

	return s, nil
}

func actorKey(atespace, name string) string {
	if atespace == "" {
		atespace = "default"
	}
	return fmt.Sprintf("%s/%s", atespace, name)
}

var nonAlphanumeric = regexp.MustCompile(`[^a-zA-Z0-9_.-]`)

func sanitizeName(name string) string {
	s := nonAlphanumeric.ReplaceAllString(name, "-")
	return strings.Trim(s, "-._")
}

func (s *Server) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.CreateAtespaceResponse, error) {
	if req.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "atespace name cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.atespaces[req.GetName()] = true
	return &ateapipb.CreateAtespaceResponse{
		Atespace: &ateapipb.Atespace{
			Name: req.GetName(),
		},
	}, nil
}

func (s *Server) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.GetAtespaceResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.atespaces[req.GetName()] {
		return nil, status.Errorf(codes.NotFound, "atespace %s not found", req.GetName())
	}

	return &ateapipb.GetAtespaceResponse{
		Atespace: &ateapipb.Atespace{
			Name: req.GetName(),
		},
	}, nil
}

func (s *Server) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var spaces []*ateapipb.Atespace
	for name := range s.atespaces {
		spaces = append(spaces, &ateapipb.Atespace{Name: name})
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces: spaces,
	}, nil
}

func (s *Server) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.DeleteAtespaceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, actor := range s.actors {
		if actor.Atespace == req.GetName() {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot delete atespace %s: actors still exist", req.GetName())
		}
	}

	delete(s.atespaces, req.GetName())
	return &ateapipb.DeleteAtespaceResponse{}, nil
}

func (s *Server) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.CreateActorResponse, error) {
	ref := req.GetActorRef()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_ref with valid name is required")
	}

	atespace := ref.GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	key := actorKey(atespace, ref.GetName())

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.actors[key]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "actor %s already exists", ref.GetName())
	}

	s.atespaces[atespace] = true

	templateName := req.GetActorTemplateName()
	if templateName == "" {
		templateName = "default"
	}

	tmpl, ok := s.cfg.Templates[templateName]
	if !ok {
		tmpl = s.cfg.Templates["default"]
	}

	port := tmpl.Port
	if port == 0 {
		port = 50053
	}

	containerName := fmt.Sprintf("ate-%s-%s", sanitizeName(atespace), sanitizeName(ref.GetName()))
	if len(containerName) > 48 {
		containerName = containerName[:48]
	}

	record := &ActorRecord{
		Atespace:          atespace,
		Name:              ref.GetName(),
		TemplateNamespace: req.GetActorTemplateNamespace(),
		TemplateName:      templateName,
		ContainerName:     containerName,
		Port:              port,
		Status:            ateapipb.Actor_STATUS_SUSPENDED,
		CreatedAt:         time.Now(),
	}

	s.actors[key] = record

	return &ateapipb.CreateActorResponse{
		Actor: record.toProto(),
	}, nil
}

func (s *Server) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	ref := req.GetActorRef()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_ref is required")
	}

	key := actorKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", key)
	}

	tmpl, ok := s.cfg.Templates[actor.TemplateName]
	if !ok {
		tmpl = s.cfg.Templates["default"]
	}

	// Inspect container if it exists
	insp, err := s.runner.Inspect(ctx, actor.ContainerName)
	if err == nil && insp != nil {
		if insp.Status.State == "running" && actor.ContainerIP != "" {
			return &ateapipb.ResumeActorResponse{
				Actor: actor.toProto(),
			}, nil
		}
		// Container exists but stopped: start it
		if _, err := s.runner.Run(ctx, "start", actor.ContainerName); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to start container %s: %v", actor.ContainerName, err)
		}
	} else {
		// Container does not exist yet: run fresh container
		runArgs := []string{"run", "--progress", "none", "-d", "--name", actor.ContainerName}
		for k, v := range tmpl.Env {
			runArgs = append(runArgs, "-e", fmt.Sprintf("%s=%s", k, v))
		}
		for _, vol := range tmpl.Volumes {
			runArgs = append(runArgs, "-v", vol)
		}
		if tmpl.WorkDir != "" {
			runArgs = append(runArgs, "-w", tmpl.WorkDir)
		}
		if tmpl.CPUs > 0 {
			runArgs = append(runArgs, "--cpus", fmt.Sprintf("%d", tmpl.CPUs))
		}
		if tmpl.Memory != "" {
			runArgs = append(runArgs, "--memory", tmpl.Memory)
		}

		runArgs = append(runArgs, tmpl.Image)
		if len(tmpl.Command) > 0 {
			runArgs = append(runArgs, tmpl.Command...)
		}

		if _, err := s.runner.Run(ctx, runArgs...); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to run container for actor %s: %v", key, err)
		}
	}

	// Re-inspect to get running network info
	insp, err = s.runner.Inspect(ctx, actor.ContainerName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to inspect container %s: %v", actor.ContainerName, err)
	}

	var ip string
	if len(insp.Status.Networks) > 0 {
		ip = CleanIPv4(insp.Status.Networks[0].IPv4Address)
	}
	if ip == "" {
		ip = "127.0.0.1"
	}

	// Wait for port readiness
	targetAddr := fmt.Sprintf("%s:%d", ip, actor.Port)
	if err := s.waitForPort(ctx, targetAddr, s.cfg.ReadyTimeout); err != nil {
		log.Printf("Warning: port %s not immediately reachable: %v", targetAddr, err)
	}

	s.mu.Lock()
	actor.Status = ateapipb.Actor_STATUS_RUNNING
	actor.ContainerIP = ip
	s.mu.Unlock()

	return &ateapipb.ResumeActorResponse{
		Actor: actor.toProto(),
	}, nil
}

func (s *Server) waitForPort(ctx context.Context, address string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", address)
}

func (s *Server) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	ref := req.GetActorRef()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_ref is required")
	}

	key := actorKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", key)
	}

	// Stop the container
	_, _ = s.runner.Run(ctx, "stop", "--time", "2", actor.ContainerName)

	s.mu.Lock()
	actor.Status = ateapipb.Actor_STATUS_SUSPENDED
	actor.ContainerIP = ""
	s.mu.Unlock()

	return &ateapipb.SuspendActorResponse{
		Actor: actor.toProto(),
	}, nil
}

func (s *Server) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	ref := req.GetActorRef()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_ref is required")
	}

	key := actorKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", key)
	}

	_, _ = s.runner.Run(ctx, "stop", "--time", "2", actor.ContainerName)

	s.mu.Lock()
	actor.Status = ateapipb.Actor_STATUS_PAUSED
	actor.ContainerIP = ""
	s.mu.Unlock()

	return &ateapipb.PauseActorResponse{
		Actor: actor.toProto(),
	}, nil
}

func (s *Server) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.GetActorResponse, error) {
	ref := req.GetActorRef()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_ref is required")
	}

	key := actorKey(ref.GetAtespace(), ref.GetName())

	s.mu.RLock()
	actor, ok := s.actors[key]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", key)
	}

	return &ateapipb.GetActorResponse{
		Actor: actor.toProto(),
	}, nil
}

func (s *Server) ListActors(ctx context.Context, req *ateapipb.ListActorsRequest) (*ateapipb.ListActorsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var actors []*ateapipb.Actor
	for _, a := range s.actors {
		if req.GetAtespace() == "" || a.Atespace == req.GetAtespace() {
			actors = append(actors, a.toProto())
		}
	}
	return &ateapipb.ListActorsResponse{
		Actors: actors,
	}, nil
}

func (s *Server) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.DeleteActorResponse, error) {
	ref := req.GetActorRef()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_ref is required")
	}

	key := actorKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	if ok {
		delete(s.actors, key)
	}
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %s not found", key)
	}

	_, _ = s.runner.Run(ctx, "delete", "--force", actor.ContainerName)

	return &ateapipb.DeleteActorResponse{}, nil
}

func (s *Server) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workers []*ateapipb.Worker
	for _, a := range s.actors {
		if a.Status == ateapipb.Actor_STATUS_RUNNING {
			workers = append(workers, &ateapipb.Worker{
				WorkerPod: a.ContainerName,
				Ip:        a.ContainerIP,
			})
		}
	}
	return &ateapipb.ListWorkersResponse{
		Workers: workers,
	}, nil
}

func (s *Server) DebugClear(ctx context.Context, req *ateapipb.DebugClearRequest) (*ateapipb.DebugClearResponse, error) {
	s.mu.Lock()
	actors := make([]*ActorRecord, 0, len(s.actors))
	for _, a := range s.actors {
		actors = append(actors, a)
	}
	s.actors = make(map[string]*ActorRecord)
	s.atespaces = map[string]bool{"default": true, "ax": true}
	s.mu.Unlock()

	for _, a := range actors {
		_, _ = s.runner.Run(ctx, "delete", "--force", a.ContainerName)
	}

	return &ateapipb.DebugClearResponse{}, nil
}

// Close gracefully stops all active containers.
func (s *Server) Close(ctx context.Context) error {
	s.mu.Lock()
	actors := make([]*ActorRecord, 0, len(s.actors))
	for _, a := range s.actors {
		actors = append(actors, a)
	}
	s.mu.Unlock()

	for _, a := range actors {
		_, _ = s.runner.Run(ctx, "stop", "--time", "2", a.ContainerName)
	}
	return nil
}

// Register registers the server with a gRPC server.
func (s *Server) Register(grpcServer *grpc.Server) {
	ateapipb.RegisterControlServer(grpcServer, s)
}
