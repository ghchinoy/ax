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
	"fmt"
	"log"
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
	configFile := flag.String("config", "", "Path to ate-local.yaml configuration file (optional)")
	addr := flag.String("addr", ":50051", "gRPC server listen address")
	containerBin := flag.String("container-bin", "", "Path to Apple container CLI binary (optional)")
	flag.Parse()

	cfg, err := atelocal.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if *addr != "" && *addr != ":50051" {
		cfg.Address = *addr
	}
	if *containerBin != "" {
		cfg.BinaryPath = *containerBin
	}

	server, err := atelocal.NewServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create local substrate server: %v", err)
	}

	lis, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", cfg.Address, err)
	}

	grpcServer := grpc.NewServer()
	server.Register(grpcServer)
	reflection.Register(grpcServer)

	// Graceful shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\nShutting down ate-local daemon...")
		grpcServer.GracefulStop()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Close(ctx)
		os.Exit(0)
	}()

	fmt.Printf("ate-local Substrate Control server listening on %s (backed by Apple container)\n", lis.Addr().String())
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
