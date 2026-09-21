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

// Command ate-local is a local Agent Substrate Control plane daemon backed by Apple Container.
// It provides a gRPC ControlServer interface (CreateActor, ResumeActor, SuspendActor) that
// upstream AX can point to for running harnesses inside isolated Apple containers on macOS.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/ax/internal/atelocal"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	configFile := flag.String("config", "", "Path to ate-local.yaml configuration file (optional)")
	addr := flag.String("addr", ":50051", "gRPC server listen address")
	containerBin := flag.String("container-bin", "", "Path to Apple container CLI binary (optional)")
	initImage := flag.String("init-image", "", "Custom Apple container vminit image (optional, auto-detected on version mismatch)")
	flag.Parse()

	cfg, err := atelocal.LoadConfig(*configFile)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	if *addr != "" && *addr != ":50051" {
		cfg.Address = *addr
	}
	if *containerBin != "" {
		cfg.BinaryPath = *containerBin
	}
	if *initImage != "" {
		cfg.InitImage = *initImage
	}

	server, err := atelocal.NewServer(cfg)
	if err != nil {
		slog.Error("failed to create local substrate server", "error", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		slog.Error("failed to listen", "address", cfg.Address, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	server.Register(grpcServer)
	reflection.Register(grpcServer)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		<-ctx.Done()
		slog.Info("shutting down ate-local daemon")
		grpcServer.GracefulStop()

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Close(shutdownCtx)
	}()

	slog.Info("ate-local Substrate Control server listening", "address", lis.Addr().String(), "runtime", "apple-container")
	if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
