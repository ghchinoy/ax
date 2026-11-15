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
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/ax/pkg/apis/v1alpha1"
	ateapipb "go.ate.dev/substrate/api/ateapi/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gopkg.in/yaml.v3"
)

func deterministicUUID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[0:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

// ActorRecord holds state for a managed actor in the local Control plane.
type ActorRecord struct {
	Atespace           string
	Name               string
	UID                string
	TemplateNamespace  string
	TemplateName       string
	TemplateUID        string
	SourceTag          string
	ContainerName      string
	ContainerIP        string
	WorkspaceHostDir   string
	Port               int
	State              ateapipb.ActorState
	SnapshotGeneration int32
	CreatedAt          time.Time
	UpdatedAt          time.Time
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
	var sourceTag *ateapipb.ObjectRef
	if r.SourceTag != "" {
		sourceTag = &ateapipb.ObjectRef{
			Atespace: r.Atespace,
			Name:     r.SourceTag,
		}
	}
	var snapshots []*ateapipb.Snapshot
	if r.SnapshotGeneration > 0 {
		snapshots = []*ateapipb.Snapshot{{
			Uuid:             fmt.Sprintf("snap-%s-%d", sanitizeName(r.Name), r.SnapshotGeneration),
			Generation:       r.SnapshotGeneration,
			Owner:            ateapipb.SnapshotOwner_SNAPSHOT_OWNER_ACTOR,
			Fidelity:         ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			ActorTemplateUid: r.TemplateUID,
			DurableSnapshot: &ateapipb.SnapshotStorage{
				State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
			},
		}}
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
		SourceTag: sourceTag,
		Status: &ateapipb.ActorStatus{
			State:                  r.State,
			WorkerAssignment:       assignment,
			Snapshots:              snapshots,
			LastAssignedGeneration: r.SnapshotGeneration,
		},
	}
}

// Server implements ateapipb.ControlServer and a local atenet-router proxy
// backed by Apple container or Podman.
type Server struct {
	ateapipb.UnimplementedControlServer

	cfg        *Config
	runner     ContainerRunner
	http1Trans *http.Transport
	h2cTrans   *http.Transport

	mu             sync.RWMutex
	atespaces      map[string]bool
	templates      map[string]TemplateConfig         // key: atespace/name
	actors         map[string]*ActorRecord           // key: atespace/name
	egressPolicies map[string]*ateapipb.EgressPolicy // key: atespace/actorName
	tags           map[string]*ateapipb.Tag          // key: atespace/tagName
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

	h1Trans := http.DefaultTransport.(*http.Transport).Clone()
	h2Trans := http.DefaultTransport.(*http.Transport).Clone()
	h2Trans.Protocols = new(http.Protocols)
	h2Trans.Protocols.SetUnencryptedHTTP2(true)

	s := &Server{
		cfg:            cfg,
		runner:         runner,
		http1Trans:     h1Trans,
		h2cTrans:       h2Trans,
		atespaces:      make(map[string]bool),
		templates:      make(map[string]TemplateConfig),
		actors:         make(map[string]*ActorRecord),
		egressPolicies: make(map[string]*ateapipb.EgressPolicy),
		tags:           make(map[string]*ateapipb.Tag),
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
		if v.UID == "" {
			v.UID = deterministicUUID("template/" + resourceKey(atespace, name))
		}
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
	for _, tag := range s.tags {
		if tag.GetMetadata().GetAtespace() == name {
			return nil, status.Errorf(codes.FailedPrecondition, "cannot delete atespace %q: tags still exist", name)
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
	// Images in */ax-images/* (such as ghcr.io/ghchinoy/ax-images/* and
	// ghcr.io/rakyll/ax-images/*) already ship a multi-arch linux/arm64
	// /usr/local/bin/ax-task-runner (linked to /usr/local/bin/ax-agent). Skip
	// bind-mounting the host's default ax-task-runner over it.
	if strings.Contains(tc.Image, "/ax-images/") {
		var filtered []string
		for _, v := range tc.Volumes {
			if !strings.HasSuffix(v, ":/usr/local/bin/ax-task-runner") {
				filtered = append(filtered, v)
			}
		}
		tc.Volumes = filtered
	}

	tmplUID := tmpl.GetMetadata().GetUid()
	if tmplUID == "" {
		tmplUID = deterministicUUID("template/" + key)
	}
	tc.UID = tmplUID

	s.mu.Lock()
	s.templates[key] = tc
	s.mu.Unlock()

	out := proto.Clone(tmpl).(*ateapipb.ActorTemplate)
	if out.Metadata == nil {
		out.Metadata = &ateapipb.ResourceMetadata{}
	}
	out.Metadata.Atespace = atespace
	out.Metadata.Name = name
	out.Metadata.Uid = tmplUID
	return out, nil
}

// GetActorTemplate retrieves an ActorTemplate by atespace and name.
func (s *Server) GetActorTemplate(ctx context.Context, req *ateapipb.GetActorTemplateRequest) (*ateapipb.ActorTemplate, error) {
	ref := req.GetActorTemplate()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "actor_template ref is required")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	atespace := ref.GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	tc, ok := s.lookupTemplateLocked(atespace, ref.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "template %q not found", ref.GetName())
	}
	uid := tc.UID
	if uid == "" {
		uid = deterministicUUID("template/" + resourceKey(atespace, ref.GetName()))
	}

	return &ateapipb.ActorTemplate{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace: ref.GetAtespace(),
			Name:     ref.GetName(),
			Uid:      uid,
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
		uid := tc.UID
		if uid == "" {
			uid = deterministicUUID("template/" + resourceKey(tc.Atespace, tc.Name))
		}
		list = append(list, &ateapipb.ActorTemplate{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: tc.Atespace,
				Name:     tc.Name,
				Uid:      uid,
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
	tmplAtespace := templateNamespace
	if tmplAtespace == "" {
		tmplAtespace = atespace
	}
	templateUID := tc.UID
	if templateUID == "" {
		templateUID = deterministicUUID("template/" + resourceKey(tmplAtespace, templateName))
	}

	sourceTagName := ""
	if tagRef := actor.GetSourceTag(); tagRef != nil && tagRef.GetName() != "" {
		tagAtespace := tagRef.GetAtespace()
		if tagAtespace == "" {
			tagAtespace = atespace
		}
		tag, ok := s.tags[resourceKey(tagAtespace, tagRef.GetName())]
		if !ok {
			return nil, status.Errorf(codes.NotFound, "source tag %s/%s not found", tagAtespace, tagRef.GetName())
		}
		tagUID := tag.GetStatus().GetActorTemplateUid()
		if tagUID != "" && templateUID != "" && tagUID != templateUID {
			return nil, status.Errorf(codes.InvalidArgument, "source tag template uid %q does not match actor template uid %q", tagUID, templateUID)
		}
		sourceTagName = tagRef.GetName()
	}

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
	actorUID := deterministicUUID(fmt.Sprintf("%s/%s/%d", atespace, name, now.UnixNano()))
	record := &ActorRecord{
		Atespace:          atespace,
		Name:              name,
		UID:               actorUID,
		TemplateNamespace: templateNamespace,
		TemplateName:      templateName,
		TemplateUID:       templateUID,
		SourceTag:         sourceTagName,
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
	preMountedWorkspace := false
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
		if s.cfg.SnapshotDir != "" {
			if wsHostDir, ok := s.stagePreBootWorkspace(ctx, actor, tmpl); ok {
				runArgs = append(runArgs, "-v", fmt.Sprintf("%s:/workspace", wsHostDir))
				s.mu.Lock()
				actor.WorkspaceHostDir = wsHostDir
				s.mu.Unlock()
				preMountedWorkspace = true
			}
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

	if freshContainer && s.cfg.SnapshotDir != "" && !preMountedWorkspace {
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
	actor.SnapshotGeneration++
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

func (s *Server) tagSnapshotPath(atespace, tagName string) string {
	if atespace == "" {
		atespace = "default"
	}
	return filepath.Join(s.cfg.SnapshotDir, sanitizeName(atespace), "tags", sanitizeName(tagName)+".tar.gz")
}

func (s *Server) actorWorkdirPath(atespace, actorName string) string {
	if atespace == "" {
		atespace = "default"
	}
	return filepath.Join(s.cfg.SnapshotDir, "workdirs", sanitizeName(s.cfg.ContainerPrefix), sanitizeName(atespace), sanitizeName(actorName))
}

func archiveDirTarGz(srcDir, dstPath string) (int64, error) {
	info, err := os.Stat(srcDir)
	if err != nil || !info.IsDir() {
		return 0, fmt.Errorf("workspace host dir %q not available", srcDir)
	}
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return 0, fmt.Errorf("creating snapshot directory: %w", err)
	}
	tmpPath := dstPath + ".tmp"
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, fmt.Errorf("creating temp snapshot file: %w", err)
	}
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	walkErr := filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil || rel == "." {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		var linkTarget string
		if fi.Mode()&os.ModeSymlink != 0 {
			linkTarget, _ = os.Readlink(path)
		}
		hdr, err := tar.FileInfoHeader(fi, linkTarget)
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if d.IsDir() && !strings.HasSuffix(hdr.Name, "/") {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if fi.Mode().IsRegular() {
			rf, err := os.Open(path)
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(tw, rf)
			_ = rf.Close()
			if copyErr != nil {
				return copyErr
			}
		}
		return nil
	})
	closeTwErr := tw.Close()
	closeGzErr := gz.Close()
	closeFileErr := f.Close()
	if walkErr != nil {
		_ = os.Remove(tmpPath)
		return 0, walkErr
	}
	for _, cerr := range []error{closeTwErr, closeGzErr, closeFileErr} {
		if cerr != nil {
			_ = os.Remove(tmpPath)
			return 0, cerr
		}
	}
	st, err := os.Stat(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)
		return 0, err
	}
	if err := os.Rename(tmpPath, dstPath); err != nil {
		_ = os.Remove(tmpPath)
		return 0, err
	}
	return st.Size(), nil
}

func (s *Server) saveWorkspaceSnapshot(ctx context.Context, actor *ActorRecord) error {
	path := s.snapshotPath(actor.Atespace, actor.Name)
	s.mu.RLock()
	wsHostDir := actor.WorkspaceHostDir
	s.mu.RUnlock()

	out, execErr := s.runner.Run(ctx, "exec", actor.ContainerName, "sh", "-c", "test -d /workspace && tar -czf - -C /workspace . | base64 | tr -d '\\n\\r'")
	encoded := strings.TrimSpace(out)
	if execErr != nil || encoded == "" {
		if wsHostDir != "" {
			if n, hostErr := archiveDirTarGz(wsHostDir, path); hostErr == nil {
				slog.Info("saved workspace snapshot from staged host directory", "actor", resourceKey(actor.Atespace, actor.Name), "path", path, "bytes", n)
				return nil
			}
		}
		return execErr
	}

	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return fmt.Errorf("decoding workspace snapshot base64: %w", err)
	}
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

var wsNameFromYAMLRe = regexp.MustCompile(`(?m)^\s*name:\s*["']?([a-zA-Z0-9._-]+)["']?\s*$`)

// stagePreBootWorkspace extracts an actor's snapshot (or warm workspace tag
// snapshot) into a host directory before `container run` / `podman run` so
// /workspace is populated at T=0 before PID 1 (`ax-task-runner`) starts.
// Returns ("", false) when no valid tar.gz archive exists (falling back to
// post-boot restoreWorkspaceSnapshot).
func (s *Server) stagePreBootWorkspace(ctx context.Context, actor *ActorRecord, tmpl TemplateConfig) (string, bool) {
	candidates := []string{s.snapshotPath(actor.Atespace, actor.Name)}
	if actor.SourceTag != "" {
		candidates = append(candidates, s.tagSnapshotPath(actor.Atespace, actor.SourceTag))
	}
	wsYAML := tmpl.Env["AX_WORKSPACE_YAML"]
	if wsYAML != "" {
		if m := wsNameFromYAMLRe.FindStringSubmatch(wsYAML); len(m) == 2 {
			candidates = append(candidates, s.tagSnapshotPath(actor.Atespace, m[1]+"-warm"))
		}
	}

	var snapPath string
	for _, c := range candidates {
		hdr, err := os.ReadFile(c)
		// Check gzip magic header (0x1f, 0x8b) so unit tests using synthetic
		// non-gzip byte slices keep exercising post-boot restoreWorkspaceSnapshot.
		if err == nil && len(hdr) >= 2 && hdr[0] == 0x1f && hdr[1] == 0x8b {
			snapPath = c
			break
		}
	}
	if snapPath == "" && wsYAML != "" {
		if prewarmed, ok := s.ensureWarmWorkspaceTag(ctx, actor, wsYAML); ok {
			snapPath = prewarmed
		}
	}
	if snapPath == "" {
		return "", false
	}

	hostDir := s.actorWorkdirPath(actor.Atespace, actor.Name)
	_ = os.RemoveAll(hostDir)
	if err := os.MkdirAll(hostDir, 0o755); err != nil {
		return "", false
	}
	if err := exec.Command("tar", "-xzf", snapPath, "-C", hostDir).Run(); err != nil {
		_ = os.RemoveAll(hostDir)
		return "", false
	}
	slog.Info("pre-staged workspace snapshot before container boot", "actor", resourceKey(actor.Atespace, actor.Name), "snapshot", snapPath, "hostDir", hostDir)
	return hostDir, true
}

func workspaceNeedsSetup(ws *v1alpha1.Workspace) bool {
	setup := ws.GetSpec().GetSetup()
	if setup == nil {
		return false
	}
	return len(setup.GetGit()) > 0 || len(setup.GetFiles()) > 0 || setup.GetSkills() != nil || len(setup.GetCommands()) > 0
}

func (s *Server) findRunnerHostBinary(tmpl TemplateConfig) string {
	for _, tc := range []TemplateConfig{tmpl, s.resolveTemplate("default", "default")} {
		for _, vol := range tc.Volumes {
			resolved := resolveVolumeSpec(vol)
			parts := strings.SplitN(resolved, ":", 2)
			if len(parts) == 2 && parts[1] == "/usr/local/bin/ax-task-runner" {
				if st, err := os.Stat(parts[0]); err == nil && !st.IsDir() {
					return parts[0]
				}
			}
		}
	}
	return ""
}

// ensureWarmWorkspaceTag runs a short-lived setup container (using SetupImage,
// e.g. alpine/git) when a task binds a Workspace with non-empty spec.setup and
// no warm <ws>-warm Tag snapshot exists yet.
func (s *Server) ensureWarmWorkspaceTag(ctx context.Context, actor *ActorRecord, wsYAML string) (string, bool) {
	if s.cfg.SetupImage == "" || s.cfg.SetupImage == "none" {
		return "", false
	}
	var ws v1alpha1.Workspace
	if err := yaml.Unmarshal([]byte(wsYAML), &ws); err != nil || !workspaceNeedsSetup(&ws) {
		return "", false
	}
	wsName := ws.GetMetadata().GetName()
	if wsName == "" {
		return "", false
	}
	atespace := actor.Atespace
	if atespace == "" {
		atespace = "default"
	}
	runnerBin := s.findRunnerHostBinary(s.resolveTemplate(atespace, actor.TemplateName))
	if runnerBin == "" {
		return "", false
	}

	tagName := wsName + "-warm"
	tagSnap := s.tagSnapshotPath(atespace, tagName)

	setupImage := ws.GetSpec().GetProfile().GetImage()
	if setupImage == "" {
		setupImage = s.cfg.SetupImage
	}

	stageDir, err := os.MkdirTemp(s.cfg.SnapshotDir, "ws-prewarm-*")
	if err != nil {
		return "", false
	}
	defer func() { _ = os.RemoveAll(stageDir) }()

	setupName := fmt.Sprintf("%s-setup-%s-%d", sanitizeName(s.cfg.ContainerPrefix), sanitizeName(wsName), time.Now().UnixNano()%100000)
	if len(setupName) > 48 {
		setupName = setupName[:48]
	}
	setupTaskYAML := fmt.Sprintf("apiVersion: ax.io/v1alpha1\nkind: Task\nmetadata:\n  name: %s\n  atespace: %s\nspec:\n  workspace:\n    name: %s\n  commands:\n    - \"true\"\n", setupName, atespace, wsName)

	runArgs := []string{
		"run", "--progress", "none", "-d",
		"--name", setupName,
		"--entrypoint", "/usr/local/bin/ax-task-runner",
		"-v", runnerBin + ":/usr/local/bin/ax-task-runner",
		"-v", stageDir + ":/workspace",
		"-e", "AX_WORKSPACE_YAML=" + wsYAML,
		"-e", "AX_TASK_YAML=" + setupTaskYAML,
	}
	if s.cfg.InitImage != "" {
		runArgs = append(runArgs, "--init-image", s.cfg.InitImage)
	}
	runArgs = append(runArgs, setupImage)

	slog.Info("auto-prewarming workspace snapshot tag", "workspace", resourceKey(atespace, wsName), "tag", tagName, "setupImage", setupImage)
	if _, err := s.runner.Run(ctx, runArgs...); err != nil {
		slog.Warn("workspace auto-prewarm container failed to launch", "workspace", wsName, "error", err)
		return "", false
	}
	defer func() {
		_, _ = s.runner.Run(context.Background(), "delete", "--force", setupName)
	}()

	deadline := time.Now().Add(s.cfg.ReadyTimeout)
	initialized := false
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(stageDir, ".ax", "initialized")); err == nil {
			initialized = true
			break
		}
		if _, err := os.Stat(filepath.Join(stageDir, ".git", ".ax", "initialized")); err == nil {
			initialized = true
			break
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(150 * time.Millisecond):
		}
	}
	_, _ = s.runner.Run(ctx, "stop", "--time", "1", setupName)
	if !initialized {
		slog.Warn("workspace auto-prewarm timed out waiting for .ax/initialized", "workspace", wsName)
		return "", false
	}

	if _, err := archiveDirTarGz(stageDir, tagSnap); err != nil {
		slog.Warn("archiving auto-prewarmed workspace failed", "workspace", wsName, "error", err)
		return "", false
	}

	now := timestamppb.Now()
	tagKey := resourceKey(atespace, tagName)
	tmplUID := actor.TemplateUID
	tag := &ateapipb.Tag{
		Metadata: &ateapipb.ResourceMetadata{
			Atespace:   atespace,
			Name:       tagName,
			Uid:        deterministicUUID("tag/" + tagKey),
			CreateTime: now,
			UpdateTime: now,
		},
		Scope: ateapipb.TagScope_TAG_SCOPE_ATESPACE,
		Status: &ateapipb.TagStatus{
			ActorTemplateUid: tmplUID,
			Snapshot: &ateapipb.Snapshot{
				Uuid:             "snap-" + sanitizeName(tagName),
				Generation:       1,
				Owner:            ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
				Fidelity:         ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
				ActorTemplateUid: tmplUID,
				DurableSnapshot: &ateapipb.SnapshotStorage{
					State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
				},
			},
		},
	}
	s.mu.Lock()
	s.tags[tagKey] = tag
	s.mu.Unlock()

	slog.Info("minted warm workspace snapshot tag", "workspace", wsName, "tag", tagKey, "path", tagSnap)
	return tagSnap, true
}

func (s *Server) restoreWorkspaceSnapshot(ctx context.Context, actor *ActorRecord) error {
	path := s.snapshotPath(actor.Atespace, actor.Name)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) && actor.SourceTag != "" {
			path = s.tagSnapshotPath(actor.Atespace, actor.SourceTag)
			data, err = os.ReadFile(path)
		}
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return fmt.Errorf("reading snapshot file: %w", err)
		}
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
	if s.cfg.SnapshotDir != "" {
		_ = os.RemoveAll(s.actorWorkdirPath(actor.Atespace, actor.Name))
		_ = os.Remove(s.snapshotPath(actor.Atespace, actor.Name))
	}

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
	if req.GetName() != "" && req.GetName() != "default" {
		return nil, status.Errorf(codes.InvalidArgument, "name: Invalid value: %q: must be \"default\"", req.GetName())
	}
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

// CreateTag captures an immutable snapshot Tag from a suspended source Actor.
func (s *Server) CreateTag(ctx context.Context, req *ateapipb.CreateTagRequest) (*ateapipb.Tag, error) {
	in := req.GetTag()
	if in == nil || in.GetMetadata() == nil || in.GetMetadata().GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tag with metadata.name is required")
	}
	atespace := in.GetMetadata().GetAtespace()
	if atespace == "" {
		atespace = "default"
	}
	tagName := in.GetMetadata().GetName()
	tagKey := resourceKey(atespace, tagName)

	srcRef := in.GetSourceActor()
	if srcRef == nil || srcRef.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "source_actor is required")
	}
	srcAtespace := srcRef.GetAtespace()
	if srcAtespace == "" {
		srcAtespace = atespace
	}
	srcKey := resourceKey(srcAtespace, srcRef.GetName())

	s.mu.Lock()
	if _, exists := s.tags[tagKey]; exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "tag %q already exists", tagKey)
	}
	srcActor, ok := s.actors[srcKey]
	if !ok {
		s.mu.Unlock()
		return nil, status.Errorf(codes.NotFound, "source actor %q not found", srcKey)
	}
	if srcActor.State != ateapipb.ActorState_ACTOR_STATE_SUSPENDED {
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "source actor %q must be suspended", srcKey)
	}
	tmplUID := srcActor.TemplateUID
	s.mu.Unlock()

	if s.cfg.SnapshotDir != "" {
		srcSnap := s.snapshotPath(srcAtespace, srcRef.GetName())
		if data, err := os.ReadFile(srcSnap); err == nil {
			dstSnap := s.tagSnapshotPath(atespace, tagName)
			if err := os.MkdirAll(filepath.Dir(dstSnap), 0o755); err != nil {
				return nil, status.Errorf(codes.Internal, "creating tag snapshot dir: %v", err)
			}
			if err := os.WriteFile(dstSnap, data, 0o644); err != nil {
				return nil, status.Errorf(codes.Internal, "writing tag snapshot: %v", err)
			}
		}
	}

	now := timestamppb.Now()
	tagUID := deterministicUUID(fmt.Sprintf("tag/%s/%d", tagKey, now.GetNanos()))
	tag := proto.Clone(in).(*ateapipb.Tag)
	tag.Metadata = &ateapipb.ResourceMetadata{
		Atespace:   atespace,
		Name:       tagName,
		Uid:        tagUID,
		CreateTime: now,
		UpdateTime: now,
	}
	tag.Status = &ateapipb.TagStatus{
		ActorTemplateUid: tmplUID,
		Snapshot: &ateapipb.Snapshot{
			Uuid:             "snap-" + sanitizeName(tagName),
			Generation:       1,
			Owner:            ateapipb.SnapshotOwner_SNAPSHOT_OWNER_TAG,
			Fidelity:         ateapipb.SnapshotFidelity_SNAPSHOT_FIDELITY_VOLUMES,
			ActorTemplateUid: tmplUID,
			DurableSnapshot: &ateapipb.SnapshotStorage{
				State: ateapipb.SnapshotStorageState_SNAPSHOT_STORAGE_STATE_COMPLETED,
			},
		},
	}

	s.mu.Lock()
	if _, exists := s.tags[tagKey]; exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "tag %q already exists", tagKey)
	}
	s.tags[tagKey] = tag
	s.mu.Unlock()

	return proto.Clone(tag).(*ateapipb.Tag), nil
}

// GetTag retrieves a Tag by atespace and name.
func (s *Server) GetTag(ctx context.Context, req *ateapipb.GetTagRequest) (*ateapipb.Tag, error) {
	ref := req.GetTag()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tag ref is required")
	}
	s.mu.RLock()
	tag, ok := s.tags[resourceKey(ref.GetAtespace(), ref.GetName())]
	s.mu.RUnlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tag %q not found", resourceKey(ref.GetAtespace(), ref.GetName()))
	}
	return proto.Clone(tag).(*ateapipb.Tag), nil
}

// ListTags lists Tags, optionally filtered by atespace.
func (s *Server) ListTags(ctx context.Context, req *ateapipb.ListTagsRequest) (*ateapipb.ListTagsResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	resp := &ateapipb.ListTagsResponse{}
	for _, tag := range s.tags {
		if req.GetAtespace() == "" || tag.GetMetadata().GetAtespace() == req.GetAtespace() {
			resp.Tags = append(resp.Tags, proto.Clone(tag).(*ateapipb.Tag))
		}
	}
	return resp, nil
}

// DeleteTag removes a Tag and its snapshot copy.
func (s *Server) DeleteTag(ctx context.Context, req *ateapipb.DeleteTagRequest) (*ateapipb.Tag, error) {
	ref := req.GetTag()
	if ref == nil || ref.GetName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "tag ref is required")
	}
	k := resourceKey(ref.GetAtespace(), ref.GetName())
	s.mu.Lock()
	tag, ok := s.tags[k]
	if ok {
		delete(s.tags, k)
	}
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "tag %q not found", k)
	}
	if s.cfg.SnapshotDir != "" {
		_ = os.Remove(s.tagSnapshotPath(ref.GetAtespace(), ref.GetName()))
	}
	return proto.Clone(tag).(*ateapipb.Tag), nil
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
	if s.http1Trans != nil {
		s.http1Trans.CloseIdleConnections()
	}
	if s.h2cTrans != nil {
		s.h2cTrans.CloseIdleConnections()
	}
	return nil
}

// Register registers the server with a gRPC server.
func (s *Server) Register(grpcServer *grpc.Server) {
	ateapipb.RegisterControlServer(grpcServer, s)
}

// Handler returns an HTTP/1.1 + unencrypted HTTP/2 (h2c) handler that multiplexes:
//   - requests carrying the "ate-target-actor" header (the local atenet-router
//     data plane used by `ax ssh` and ATENET_ROUTER_ADDR readiness/HTTP probes,
//     resuming suspended actors on demand),
//   - GET /healthz health checks, and
//   - the Agent Substrate Control gRPC service on grpcServer.
func (s *Server) Handler(grpcServer *grpc.Server) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if target := strings.TrimSpace(r.Header.Get("ate-target-actor")); target != "" {
			s.serveActorProxy(w, r, target)
			return
		}
		if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
			grpcServer.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok\n"))
			return
		}
		http.NotFound(w, r)
	})
}

// NewHTTPServer builds an *http.Server configured for both HTTP/1.1 and
// unencrypted HTTP/2 (h2c) serving s.Handler(grpcServer).
func (s *Server) NewHTTPServer(addr string, grpcServer *grpc.Server) *http.Server {
	srv := &http.Server{
		Addr:    addr,
		Handler: s.Handler(grpcServer),
	}
	srv.Protocols = new(http.Protocols)
	srv.Protocols.SetHTTP1(true)
	srv.Protocols.SetUnencryptedHTTP2(true)
	return srv
}

func parseTargetActor(target string) (atespace, name string) {
	if parts := strings.SplitN(target, "/", 2); len(parts) == 2 {
		if parts[0] == "" {
			return "default", parts[1]
		}
		return parts[0], parts[1]
	}
	return "default", target
}

func (s *Server) resolveActorEndpoint(ctx context.Context, target string) (string, error) {
	atespace, name := parseTargetActor(target)
	key := resourceKey(atespace, name)

	s.mu.RLock()
	actor, ok := s.actors[key]
	var state ateapipb.ActorState
	var ip string
	var port int
	if ok {
		state = actor.State
		ip = actor.ContainerIP
		port = actor.Port
	}
	s.mu.RUnlock()

	if !ok {
		return "", status.Errorf(codes.NotFound, "actor %q not found", key)
	}
	if state != ateapipb.ActorState_ACTOR_STATE_RUNNING || ip == "" {
		res, err := s.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: name},
		})
		if err != nil {
			return "", err
		}
		ips := res.GetActor().GetStatus().GetWorkerAssignment().GetWorkerPodIps()
		if len(ips) > 0 {
			ip = ips[0]
		}
	}
	if port <= 0 {
		port = 80
	}
	if _, _, err := net.SplitHostPort(ip); err == nil {
		return ip, nil
	}
	return net.JoinHostPort(ip, fmt.Sprintf("%d", port)), nil
}

func (s *Server) serveActorProxy(w http.ResponseWriter, r *http.Request, target string) {
	endpoint, err := s.resolveActorEndpoint(r.Context(), target)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	upstreamURL := &url.URL{
		Scheme: "http",
		Host:   endpoint,
	}
	transport := s.http1Trans
	if r.ProtoMajor == 2 {
		transport = s.h2cTrans
	}
	proxy := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(upstreamURL)
			pr.Out.Host = endpoint
			pr.Out.Header.Del("ate-target-actor")
		},
		Transport:     transport,
		FlushInterval: -1,
	}
	proxy.ServeHTTP(w, r)
}
