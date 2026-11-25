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
	"io"

	"github.com/google/ax/internal/install/axdeploy"
	"github.com/google/ax/internal/install/cluster"
	installsubstrate "github.com/google/ax/internal/install/substrate"
	"github.com/google/ax/internal/tunnel"
)

// KindBackend runs the Kubernetes-based `ax local` flow (Kind + Agent Substrate + AX).
type KindBackend struct {
	Provider *cluster.KindProvider
	Out      io.Writer
}

// NewKindBackend constructs a KindBackend writing progress to out.
func NewKindBackend(axNamespace string, out io.Writer) *KindBackend {
	p := cluster.NewKindProvider(nil, out)
	p.AXNamespace = axNamespace
	return &KindBackend{Provider: p, Out: out}
}

func (b *KindBackend) Name() string { return BackendKind }

func (b *KindBackend) Start(ctx context.Context, opts StartOptions) (Endpoint, error) {
	srvImg, runnerImg, err := axdeploy.ResolveImageInputs(opts.ServerImage, opts.RunnerVolumeImage)
	if err != nil {
		return Endpoint{}, err
	}
	localRegistry, err := installsubstrate.LocalRegistryEndpoint()
	if err != nil {
		return Endpoint{}, err
	}
	info, err := b.Provider.Up(ctx, cluster.UpOptions{
		Name:          opts.Name,
		Images:        []string{srvImg, runnerImg},
		LocalRegistry: localRegistry,
	})
	if err != nil {
		return Endpoint{}, err
	}
	if err := installsubstrate.Bootstrap(ctx, installsubstrate.BootstrapOptions{
		KubeContext:    info.KubeContext,
		AXNamespace:    opts.AXNamespace,
		WorkerReplicas: opts.Workers,
		Out:            b.Out,
	}); err != nil {
		return Endpoint{}, err
	}
	if err := axdeploy.Install(ctx, axdeploy.InstallOptions{
		KubeContext:       info.KubeContext,
		Namespace:         opts.AXNamespace,
		ServerImage:       srvImg,
		RunnerVolumeImage: runnerImg,
		Out:               b.Out,
	}); err != nil {
		return Endpoint{}, err
	}
	// Clear any active non-K8s local context so kubeconfig's context takes effect.
	_ = tunnel.ClearActiveLocalContext()
	return Endpoint{
		Name:        info.Name,
		Backend:     BackendKind,
		KubeContext: info.KubeContext,
	}, nil
}

func (b *KindBackend) Stop(ctx context.Context, name string) error {
	return b.Provider.Stop(ctx, name)
}

func (b *KindBackend) Status(ctx context.Context, name string) (cluster.Status, error) {
	return b.Provider.Status(ctx, name)
}

func (b *KindBackend) Delete(ctx context.Context, name string) error {
	return b.Provider.Delete(ctx, name)
}
