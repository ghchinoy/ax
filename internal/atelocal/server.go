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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ActorRecord holds state for a managed actor in the local Control plane.
type ActorRecord struct {
	Atespace          string
	Name              string
	UID               string
	TemplateNamespace string
	TemplateName      string
	ContainerName     string
	ContainerIP       string
	Port              int
	State             ateapipb.ActorState
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func (r *ActorRecord) toProto() *ateapipb.Actor {
	var assignment *ateapipb.WorkerAssignment
	if r.ContainerIP != "" {
		assignment = &ateapipb.WorkerAssignment{
			Worker: &ateapipb.ObjectRef{
				Name: r.ContainerName,
			},
			WorkerPod:    r.ContainerName,
			WorkerPodIps: []string{r.ContainerIP},
		}
	}
	var createTime, updateTime *timestamppb.Timestamp
	if !r.CreatedAt.IsZero() {
		createTime = timestamppb.New(r.CreatedAt)
	}
	if !r.UpdatedAt.IsZero() {
		updateTime = timestamppb.New(r.UpdatedAt)
	} else if createTime != nil {
		updateTime = createTime
	}
	return &ateapipb.Actor{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   r.Atespace,
			Name:       r.Name,
			Uid:        r.UID,
			CreateTime: createTime,
			UpdateTime: updateTime,
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

	mu             sync.RWMutex
	atespaces      map[string]bool
	templates      map[string]TemplateConfig         // key: atespace/name
	actors         map[string]*ActorRecord           // key: atespace/name
	egressPolicies map[string]*ateapipb.EgressPolicy // key: atespace/actorName
}

// NewServer creates a new local Substrate Control server instance.
func NewServer(cfg *Config) (*Server, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	normRuntime, err := NormalizeRuntime(cfg.Runtime)
	if err != nil {
		return nil, err
	}
	cfg.Runtime = normRuntime
	if cfg.ContainerPrefix == "" {
		cfg.ContainerPrefix = "ate"
	}

	runner := cfg.Runner
	if runner == nil {
		runner, err = NewContainerRunner(cfg.Runtime, cfg.BinaryPath)
		if err != nil {
			return nil, err
		}
	}

	s := &Server{
		cfg:            cfg,
		runner:         runner,
		atespaces:      make(map[string]bool),
		templates:      make(map[string]TemplateConfig),
		actors:         make(map[string]*ActorRecord),
		egressPolicies: make(map[string]*ateapipb.EgressPolicy),
	}

	for k, v := range cfg.Templates {
		atespace := v.Atespace
		name := v.Name
		if parts := strings.SplitN(k, "/", 2); len(parts) == 2 {
			atespace = parts[0]
			name = parts[1]
		} else if name == "" {
			name = k
		}
		if atespace == "" {
			atespace = "default"
		}
		v.Atespace = atespace
		v.Name = name
		s.templates[resourceKey(atespace, name)] = v
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

func resolveVolumeSpec(vol string) string {
	parts := strings.SplitN(vol, ":", 2)
	if len(parts) == 2 && (strings.HasPrefix(parts[0], "./") || strings.HasPrefix(parts[0], "../")) {
		if abs, err := filepath.Abs(parts[0]); err == nil {
			return fmt.Sprintf("%s:%s", abs, parts[1])
		}
	}
	return vol
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

	defaultTC := s.resolveTemplate("default", "default")
	tc := TemplateConfig{
		Atespace:  atespace,
		Name:      name,
		InitImage: defaultTC.InitImage,
		Port:      80, // Default HTTP/readyz port in AX task runner
		Volumes:   append([]string(nil), defaultTC.Volumes...),
		CPUs:      defaultTC.CPUs,
		Memory:    defaultTC.Memory,
	}
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
		if c.GetWakeupProbe() != nil && c.GetWakeupProbe().GetHttpGet() != nil && c.GetWakeupProbe().GetHttpGet().GetPort() > 0 {
			tc.Port = int(c.GetWakeupProbe().GetHttpGet().GetPort())
		}
	}
	if limits := tmpl.GetResources().GetLimits(); limits != nil {
		if cpuMillis := limits.GetCpuMillicores(); cpuMillis > 0 {
			cpus := int((cpuMillis + 999) / 1000)
			if cpus < 1 {
				cpus = 1
			}
			tc.CPUs = cpus
		}
		if memBytes := limits.GetMemoryBytes(); memBytes > 0 {
			memMB := memBytes / (1024 * 1024)
			if memMB < 1 {
				memMB = 1
			}
			tc.Memory = fmt.Sprintf("%dM", memMB)
		}
	}
	if tc.Image == "" {
		tc.Image = "debian:12"
	}
	// Images in ghcr.io/rakyll/ax-images/* already ship a multi-arch linux/arm64
	// /usr/local/bin/ax-task-runner (linked to /usr/local/bin/ax-agent). Skip
	// bind-mounting the host's default ax-task-runner over it.
	if strings.HasPrefix(tc.Image, "ghcr.io/rakyll/ax-images/") {
		var filtered []string
		for _, v := range tc.Volumes {
			if !strings.HasSuffix(v, ":/usr/local/bin/ax-task-runner") {
				filtered = append(filtered, v)
			}
		}
		tc.Volumes = filtered
	}

	s.mu.Lock()
	s.templates[key] = tc
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

	tc, ok := s.lookupTemplateLocked(ref.GetAtespace(), ref.GetName())
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

// ListActorTemplates returns all known ActorTemplates, optionally filtered by atespace.
func (s *Server) ListActorTemplates(ctx context.Context, req *ateapipb.ListActorTemplatesRequest) (*ateapipb.ListActorTemplatesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	filterAtespace := req.GetAtespace()
	var list []*ateapipb.ActorTemplate
	for _, tc := range s.templates {
		if filterAtespace != "" && tc.Atespace != filterAtespace {
			continue
		}
		list = append(list, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: tc.Atespace,
				Name:     tc.Name,
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

	tc := s.resolveTemplateLocked(atespace, templateName)
	port := tc.Port
	if port == 0 {
		port = 80
	}

	prefix := s.cfg.ContainerPrefix
	if prefix == "" {
		prefix = "ate"
	}
	containerName := fmt.Sprintf("%s-%s-%s", sanitizeName(prefix), sanitizeName(atespace), sanitizeName(name))
	if len(containerName) > 48 {
		sum := sha256.Sum256([]byte(containerName))
		containerName = fmt.Sprintf("%s-%x", strings.TrimRight(containerName[:41], "-._"), sum[:3])
	}

	now := time.Now()
	uidSum := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%d", atespace, name, now.UnixNano())))
	actorUID := fmt.Sprintf("%x-%x-%x-%x-%x", uidSum[0:4], uidSum[4:6], uidSum[6:8], uidSum[8:10], uidSum[10:16])
	record := &ActorRecord{
		Atespace:          atespace,
		Name:              name,
		UID:               actorUID,
		TemplateNamespace: templateNamespace,
		TemplateName:      templateName,
		ContainerName:     containerName,
		Port:              port,
		State:             ateapipb.ActorState_ACTOR_STATE_SUSPENDED,
		CreatedAt:         now,
		UpdatedAt:         now,
	}

	s.actors[key] = record

	return record.toProto(), nil
}

func (s *Server) lookupTemplateLocked(atespace, name string) (TemplateConfig, bool) {
	if tc, ok := s.templates[resourceKey(atespace, name)]; ok {
		return tc, true
	}
	if tc, ok := s.templates[resourceKey("default", name)]; ok {
		return tc, true
	}
	if tc, ok := s.cfg.Templates[name]; ok {
		return tc, true
	}
	return TemplateConfig{}, false
}

func (s *Server) resolveTemplate(atespace, name string) TemplateConfig {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.resolveTemplateLocked(atespace, name)
}

func (s *Server) resolveTemplateLocked(atespace, name string) TemplateConfig {
	if tc, ok := s.lookupTemplateLocked(atespace, name); ok {
		return tc
	}
	if tc, ok := s.lookupTemplateLocked("default", "default"); ok {
		return tc
	}
	return TemplateConfig{Atespace: "default", Name: "default", Image: "debian:12", Port: 80}
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
	freshContainer := false
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
		freshContainer = true
		// Run fresh container
		runArgs := []string{"run", "--progress", "none", "-d", "--name", actor.ContainerName}
		if s.cfg.Runtime == RuntimePodman && actor.Port > 0 {
			runArgs = append(runArgs, "-p", fmt.Sprintf("127.0.0.1::%d", actor.Port))
		}
		initImg := tmpl.InitImage
		if initImg == "" {
			initImg = s.cfg.InitImage
		}
		if initImg != "" {
			runArgs = append(runArgs, "--init-image", initImg)
		}
		for k, v := range tmpl.Env {
			runArgs = append(runArgs, "-e", fmt.Sprintf("%s=%s", k, v))
		}
		for _, vol := range tmpl.Volumes {
			runArgs = append(runArgs, "-v", resolveVolumeSpec(vol))
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

	isNonServerCommand := len(tmpl.Command) > 0 && tmpl.Command[0] == "sleep"
	if actor.Port > 0 && !isNonServerCommand {
		targetAddr := fmt.Sprintf("%s:%d", ip, actor.Port)
		if _, _, err := net.SplitHostPort(ip); err == nil {
			targetAddr = ip
		}
		if err := s.waitForPort(ctx, targetAddr, s.cfg.ReadyTimeout); err != nil {
			slog.Warn("port not immediately reachable", "address", targetAddr, "error", err)
		}
	}

	if freshContainer && s.cfg.SnapshotDir != "" {
		if err := s.restoreWorkspaceSnapshot(ctx, actor); err != nil {
			slog.Warn("failed to restore workspace snapshot", "actor", key, "error", err)
		}
	}

	s.mu.Lock()
	actor.State = ateapipb.ActorState_ACTOR_STATE_RUNNING
	actor.ContainerIP = ip
	actor.UpdatedAt = time.Now()
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

	if s.cfg.SnapshotDir != "" {
		if err := s.saveWorkspaceSnapshot(ctx, actor); err != nil {
			slog.Warn("failed to save workspace snapshot", "actor", key, "error", err)
		}
	}

	_, _ = s.runner.Run(ctx, "stop", "--time", "2", actor.ContainerName)

	s.mu.Lock()
	actor.State = ateapipb.ActorState_ACTOR_STATE_SUSPENDED
	actor.ContainerIP = ""
	actor.UpdatedAt = time.Now()
	actorProto := actor.toProto()
	s.mu.Unlock()

	return &ateapipb.SuspendActorResponse{
		Actor: actorProto,
	}, nil
}

func (s *Server) snapshotPath(atespace, actorName string) string {
	if atespace == "" {
		atespace = "default"
	}
	return filepath.Join(s.cfg.SnapshotDir, sanitizeName(atespace), sanitizeName(actorName)+".tar.gz")
}

func (s *Server) saveWorkspaceSnapshot(ctx context.Context, actor *ActorRecord) error {
	out, err := s.runner.Run(ctx, "exec", actor.ContainerName, "sh", "-c", "test -d /workspace && tar -czf - -C /workspace . | base64 | tr -d '\\n\\r'")
	if err != nil {
		return err
	}
	encoded := strings.TrimSpace(out)
	if encoded == "" {
		return nil
	}
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decoding workspace snapshot base64: %w", err)
	}
	path := s.snapshotPath(actor.Atespace, actor.Name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating snapshot directory: %w", err)
	}
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("writing snapshot file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("committing snapshot file: %w", err)
	}
	slog.Info("saved workspace snapshot", "actor", resourceKey(actor.Atespace, actor.Name), "path", path, "bytes", len(data))
	return nil
}

func (s *Server) restoreWorkspaceSnapshot(ctx context.Context, actor *ActorRecord) error {
	path := s.snapshotPath(actor.Atespace, actor.Name)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("reading snapshot file: %w", err)
	}
	if len(data) == 0 {
		return nil
	}
	encoded := base64.StdEncoding.EncodeToString(data)
	script := fmt.Sprintf("mkdir -p /workspace && printf '%%s' '%s' | base64 -d | tar -xzf - -C /workspace", encoded)
	if _, err := s.runner.Run(ctx, "exec", actor.ContainerName, "sh", "-c", script); err != nil {
		return err
	}
	slog.Info("restored workspace snapshot", "actor", resourceKey(actor.Atespace, actor.Name), "path", path, "bytes", len(data))
	return nil
}

// RevertActor reverts a crashed or running actor to SUSPENDED state.
func (s *Server) RevertActor(ctx context.Context, req *ateapipb.RevertActorRequest) (*ateapipb.RevertActorResponse, error) {
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
	actor.UpdatedAt = time.Now()
	s.mu.Unlock()

	return &ateapipb.RevertActorResponse{
		Actor: actor.toProto(),
	}, nil
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
		delete(s.egressPolicies, key)
	}
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "actor %q not found", key)
	}

	_, _ = s.runner.Run(ctx, "delete", "--force", actor.ContainerName)

	return actor.toProto(), nil
}

// CreateActorEgressPolicy records an egress policy for the specified Actor.
func (s *Server) CreateActorEgressPolicy(ctx context.Context, req *ateapipb.CreateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}
	policy := req.GetEgressPolicy()
	if policy == nil {
		return nil, status.Errorf(codes.InvalidArgument, "egress_policy is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.egressPolicies[key]; exists {
		return nil, status.Errorf(codes.AlreadyExists, "egress policy for actor %q already exists", key)
	}
	s.egressPolicies[key] = policy
	return policy, nil
}

// GetActorEgressPolicy retrieves the egress policy for the specified Actor.
func (s *Server) GetActorEgressPolicy(ctx context.Context, req *ateapipb.GetActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.RLock()
	policy, ok := s.egressPolicies[key]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "egress policy for actor %q not found", key)
	}
	return policy, nil
}

// UpdateActorEgressPolicy updates the egress policy for the specified Actor.
func (s *Server) UpdateActorEgressPolicy(ctx context.Context, req *ateapipb.UpdateActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}
	policy := req.GetEgressPolicy()
	if policy == nil {
		return nil, status.Errorf(codes.InvalidArgument, "egress_policy is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	s.egressPolicies[key] = policy
	s.mu.Unlock()

	return policy, nil
}

// DeleteActorEgressPolicy deletes the egress policy for the specified Actor.
func (s *Server) DeleteActorEgressPolicy(ctx context.Context, req *ateapipb.DeleteActorEgressPolicyRequest) (*ateapipb.EgressPolicy, error) {
	ref := req.GetActor()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor ref is required")
	}

	key := resourceKey(ref.GetAtespace(), ref.GetName())

	s.mu.Lock()
	policy, ok := s.egressPolicies[key]
	delete(s.egressPolicies, key)
	s.mu.Unlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "egress policy for actor %q not found", key)
	}
	return policy, nil
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
