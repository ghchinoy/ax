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
	"log/slog"
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
	State             ateapipb.ActorState
	CreatedAt         time.Time
}

func (r *ActorRecord) toProto() *ateapipb.Actor {
	var assignment *ateapipb.WorkerAssignment
	if r.ContainerIP != "" {
		assignment = &ateapipb.WorkerAssignment{
			Worker: &ateapipb.ObjectRef{
				Name: r.ContainerName,
			},
			WorkerPod:   r.ContainerName,
			WorkerPodIp: r.ContainerIP,
		}
	}
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: r.Atespace,
			Name:     r.Name,
		},
		ActorTemplate: &ateapipb.ObjectRef{
			Atespace: r.TemplateNamespace,
			Name:     r.TemplateName,
		},
		Status: &ateapipb.ActorStatus{
			State:            r.State,
			WorkerAssignment: assignment,
		},
	}
}

// Server implements ateapipb.ControlServer for local macOS environments backed by Apple container.
type Server struct {
	ateapipb.UnimplementedControlServer

	cfg    *Config
	runner ContainerRunner

	mu        sync.RWMutex
	atespaces map[string]bool
	templates map[string]TemplateConfig // key: atespace/name or name
	actors    map[string]*ActorRecord   // key: atespace/name
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
		templates: make(map[string]TemplateConfig),
		actors:    make(map[string]*ActorRecord),
	}

	for k, v := range cfg.Templates {
		s.templates[k] = v
	}

	// Default atespaces
	s.atespaces["default"] = true
	s.atespaces["ax"] = true

	return s, nil
}

func resourceKey(atespace, name string) string {
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

// CreateAtespace creates or registers a new Atespace isolation boundary.
func (s *Server) CreateAtespace(ctx context.Context, req *ateapipb.CreateAtespaceRequest) (*ateapipb.Atespace, error) {
	name := ""
	if req.GetAtespace() != nil && req.GetAtespace().GetMetadata() != nil {
		name = req.GetAtespace().GetMetadata().GetName()
	}
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "atespace name cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.atespaces[name] = true
	return &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{
			Name: name,
		},
	}, nil
}

// GetAtespace retrieves an Atespace by name.
func (s *Server) GetAtespace(ctx context.Context, req *ateapipb.GetAtespaceRequest) (*ateapipb.Atespace, error) {
	name := req.GetAtespace().GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "atespace name cannot be empty")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.atespaces[name] {
		return nil, status.Errorf(codes.NotFound, "atespace %q not found", name)
	}

	return &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{
			Name: name,
		},
	}, nil
}

// ListAtespaces returns all registered Atespaces.
func (s *Server) ListAtespaces(ctx context.Context, req *ateapipb.ListAtespacesRequest) (*ateapipb.ListAtespacesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var spaces []*ateapipb.Atespace
	for name := range s.atespaces {
		spaces = append(spaces, &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: name},
		})
	}
	return &ateapipb.ListAtespacesResponse{
		Atespaces: spaces,
	}, nil
}

// DeleteAtespace removes an empty Atespace.
func (s *Server) DeleteAtespace(ctx context.Context, req *ateapipb.DeleteAtespaceRequest) (*ateapipb.Atespace, error) {
	name := req.GetAtespace().GetName()
	if name == "" {
		return nil, status.Errorf(codes.InvalidArgument, "atespace name cannot be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, actor := range s.actors {
		if actor.Atespace == name {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot delete atespace %q: actors still exist", name)
		}
	}

	delete(s.atespaces, name)
	return &ateapipb.Atespace{
		Metadata: &ateapipb.ResourceMetadata{Name: name},
	}, nil
}

// CreateActorTemplate registers an ActorTemplate configuration for container spawning.
func (s *Server) CreateActorTemplate(ctx context.Context, req *ateapipb.CreateActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	tmpl := req.GetActorTemplate()
	if tmpl == nil || tmpl.GetMetadata() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "actor_template with metadata is required")
	}

	name := tmpl.GetMetadata().GetName()
	atespace := tmpl.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}

	key := resourceKey(atespace, name)

	var tc TemplateConfig
	if len(tmpl.GetContainers()) > 0 {
		c := tmpl.GetContainers()[0]
		tc.Image = c.GetImage()
		tc.Command = c.GetCommand()
		if len(c.GetEnv()) > 0 {
			tc.Env = make(map[string]string)
			for _, env := range c.GetEnv() {
				tc.Env[env.GetName()] = env.GetValue()
			}
		}
	}
	if tc.Image == "" {
		tc.Image = "debian:12"
	}
	tc.Port = 80 // Default HTTP/readyz port in AX task runner

	s.mu.Lock()
	s.templates[key] = tc
	s.templates[name] = tc
	s.mu.Unlock()

	return tmpl, nil
}

// GetActorTemplate retrieves an ActorTemplate by atespace and name.
func (s *Server) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	ref := req.GetActorTemplate()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_template ref is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	key := resourceKey(ref.GetAtespace(), ref.GetName())
	tc, ok := s.templates[key]
	if !ok {
		tc, ok = s.templates[ref.GetName()]
	}
	if !ok {
		tc, ok = s.cfg.Templates[ref.GetName()]
	}
	if !ok {
		return nil, status.Errorf(codes.NotFound, "template %q not found", ref.GetName())
	}

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: ref.GetAtespace(),
			Name:     ref.GetName(),
		},
		Containers: []*ateapipb.Container{{
			Name:    "guest",
			Image:   tc.Image,
			Command: tc.Command,
		}},
	}, nil
}

// ListActorTemplates returns all known ActorTemplates.
func (s *Server) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []*ateapipb.ActorTemplate
	for name, tc := range s.templates {
		list = append(list, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{
				Name: name,
			},
			Containers: []*ateapipb.Container{{
				Name:    "guest",
				Image:   tc.Image,
				Command: tc.Command,
			}},
		})
	}
	return &ateapipb.ListActorTemplatesResponse{
		ActorTemplates: list,
	}, nil
}

// DeleteActorTemplate removes an ActorTemplate.
func (s *Server) DeleteActorTemplate(ctx context.Context, req *ateapipb.DeleteActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	ref := req.GetActorTemplate()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_template ref is required")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	key := resourceKey(ref.GetAtespace(), ref.GetName())
	delete(s.templates, key)
	delete(s.templates, ref.GetName())

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: ref.GetAtespace(),
			Name:     ref.GetName(),
		},
	}, nil
}

// CreateActor registers a new Actor instance bound to an ActorTemplate.
func (s *Server) CreateActor(ctx context.Context, req *ateapipb.CreateActorRequest) (*ateapipb.Actor, error) {
	actor := req.GetActor()
	if actor == nil || actor.GetMetadata() == nil || actor.GetMetadata().GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor with metadata.name is required")
	}

	name := actor.GetMetadata().GetName()
	atespace := actor.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	key := resourceKey(atespace, name)

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.actors[key]; ok {
		return nil, status.Errorf(codes.AlreadyExists, "actor %q already exists", name)
	}

	s.atespaces[atespace] = true

	templateName := ""
	templateNamespace := ""
	if tmplRef := actor.GetActorTemplate(); tmplRef != nil {
		templateName = tmplRef.GetName()
		templateNamespace = tmplRef.GetAtespace()
	}
	if templateName == "" {
		templateName = "default"
	}

	tc := s.resolveTemplate(atespace, templateName)
	port := tc.Port
	if port == 0 {
		port = 50053
	}

	containerName := fmt.Sprintf("ate-%s-%s", sanitizeName(atespace), sanitizeName(name))
	if len(containerName) > 48 {
		containerName = containerName[:48]
	}

	record := &ActorRecord{
		Atespace:          atespace,
		Name:              name,
		TemplateNamespace: templateNamespace,
		TemplateName:      templateName,
		ContainerName:     containerName,
		Port:              port,
		State:             ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		CreatedAt:         time.Now(),
	}

	s.actors[key] = record

	return record.toProto(), nil
}

func (s *Server) resolveTemplate(atespace, name string) TemplateConfig {
	if tc, ok := s.templates[resourceKey(atespace, name)]; ok {
		return tc
	}
	if tc, ok := s.templates[name]; ok {
		return tc
	}
	if tc, ok := s.cfg.Templates[name]; ok {
		return tc
	}
	if tc, ok := s.cfg.Templates["default"]; ok {
		return tc
	}
	return TemplateConfig{Image: "debian:12", Port: 50053}
}

// ResumeActor provisions and starts an Apple container for the Actor, returning its routable IP.
func (s *Server) ResumeActor(ctx context.Context, req *ateapipb.ResumeActorRequest) (*ateapipb.ResumeActorResponse, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.RLock()
	actor, ok := s.actors[key]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}

	tmpl := s.resolveTemplate(actor.Atespace, actor.TemplateName)

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
			return nil, status.Errorf(codes.Internal, "failed to start container %q: %v", actor.ContainerName, err)
		}
	} else {
		// Run fresh container
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
			return nil, status.Errorf(codes.Internal, "failed to run container for actor %q: %v", key, err)
		}
	}

	// Re-inspect to get running network info
	insp, err = s.runner.Inspect(ctx, actor.ContainerName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to inspect container %q: %v", actor.ContainerName, err)
	}

	var ip string
	if len(insp.Status.Networks) > 0 {
		ip = CleanIPv4(insp.Status.Networks[0].IPv4Address)
	}
	if ip == "" {
		ip = "127.0.0.1"
	}

	targetAddr := fmt.Sprintf("%s:%d", ip, actor.Port)
	if err := s.waitForPort(ctx, targetAddr, s.cfg.ReadyTimeout); err != nil {
		slog.Warn("port not immediately reachable", "address", targetAddr, "error", err)
	}

	s.mu.Lock()
	actor.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
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
	return fmt.Errorf("timeout waiting for %q", address)
}

// SuspendActor stops the Actor's container, releasing host compute resources.
func (s *Server) SuspendActor(ctx context.Context, req *ateapipb.SuspendActorRequest) (*ateapipb.SuspendActorResponse, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}

	_, _ = s.runner.Run(ctx, "stop", "--time", "2", actor.ContainerName)

	s.mu.Lock()
	actor.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	actor.ContainerIP = ""
	s.mu.Unlock()

	return &ateapipb.SuspendActorResponse{}, nil
}

// PauseActor pauses the Actor's container.
func (s *Server) PauseActor(ctx context.Context, req *ateapipb.PauseActorRequest) (*ateapipb.PauseActorResponse, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}

	_, _ = s.runner.Run(ctx, "stop", "--time", "2", actor.ContainerName)

	s.mu.Lock()
	actor.State = ateapipb.ActorState_ACTOR_STATE_PAUSED
	actor.ContainerIP = ""
	s.mu.Unlock()

	return &ateapipb.PauseActorResponse{}, nil
}

// GetActor retrieves an Actor by atespace and name.
func (s *Server) GetActor(ctx context.Context, req *ateapipb.GetActorRequest) (*ateapipb.Actor, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.RLock()
	actor, ok := s.actors[key]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}

	return actor.toProto(), nil
}

// ListActors lists all managed Actors, optionally filtered by atespace.
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

// DeleteActor terminates and deletes the container associated with the Actor.
func (s *Server) DeleteActor(ctx context.Context, req *ateapipb.DeleteActorRequest) (*ateapipb.Actor, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	actor, ok := s.actors[key]
	if ok {
		delete(s.actors, key)
	}
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}

	_, _ = s.runner.Run(ctx, "delete", "--force", actor.ContainerName)

	return actor.toProto(), nil
}

// ListWorkers returns active running containers hosting actors.
func (s *Server) ListWorkers(ctx context.Context, req *ateapipb.ListWorkersRequest) (*ateapipb.ListWorkersResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var workers []*ateapipb.Worker
	for _, a := range s.actors {
		if a.State == ateapipb.ActorState_ACTOR_STATE_RUNNING {
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
