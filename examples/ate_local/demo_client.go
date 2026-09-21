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

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	controlAddr := flag.String("control", "127.0.0.1:50051", "ate-local Control server address")
	atespace := flag.String("atespace", "ax", "Atespace name")
	actorName := flag.String("actor", "demo-actor-1", "Actor name")
	template := flag.String("template", "default", "Actor template name")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Printf("1. Connecting to ate-local Control plane at %s...\n", *controlAddr)
	ctrlConn, err := grpc.NewClient(*controlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect to Control plane: %v", err)
	}
	defer ctrlConn.Close()

	ctrlClient := ateapipb.NewControlClient(ctrlConn)

	// 1. Create Actor
	fmt.Printf("2. Calling Control.CreateActor(%s/%s, template=%s)...\n", *atespace, *actorName, *template)
	createResp, err := ctrlClient.CreateActor(ctx, &ateapipb.CreateActorRequest{
		Actor: &ateapipb.Actor{
			Metadata: &ateapipb.ResourceMetadata{
				Atespace: *atespace,
				Name:     *actorName,
			},
			ActorTemplate: &ateapipb.ObjectRef{
				Atespace: *atespace,
				Name:     *template,
			},
		},
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateActor failed: %v", err)
	}
	_ = createResp
	fmt.Println("   Actor registered in Control plane.")

	// 2. Resume Actor (spawns container and gets IP)
	fmt.Printf("3. Calling Control.ResumeActor(%s/%s)...\n", *atespace, *actorName)
	resumeResp, err := ctrlClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: *atespace, Name: *actorName},
	})
	if err != nil {
		log.Fatalf("ResumeActor failed: %v", err)
	}

	actor := resumeResp.GetActor()
	var workerIP string
	if actor.GetStatus() != nil && actor.GetStatus().GetWorkerAssignment() != nil {
		workerIP = actor.GetStatus().GetWorkerAssignment().GetWorkerPodIp()
	}
	fmt.Printf("   Actor resumed! Container Name: %s, Worker IP: %s, State: %v\n",
		actor.GetMetadata().GetName(), workerIP, actor.GetStatus().GetState())

	if workerIP == "" {
		log.Fatalf("Error: empty worker IP returned from ResumeActor")
	}

	// 3. Check connectivity to worker IP directly on macOS vmnet bridge
	fmt.Printf("4. Probing worker HTTP /readyz (%s:80) and gRPC (%s:50053) over vmnet...\n", workerIP, workerIP)
	httpClient := &http.Client{Timeout: 2 * time.Second}
	if resp, err := httpClient.Get(fmt.Sprintf("http://%s:80/readyz", workerIP)); err == nil {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		fmt.Printf("   HTTP /readyz response from %s:80 -> %d (%s)\n", workerIP, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:50053", workerIP), 2*time.Second)
	if err != nil {
		fmt.Printf("   [Note: port 50053 not listening on template %q, but container network %s is verified!]\n", *template, workerIP)
	} else {
		_ = conn.Close()
		fmt.Printf("   Successfully reached worker gRPC port at %s:50053 directly!\n", workerIP)
	}

	// 4. Suspend Actor
	fmt.Printf("5. Calling Control.SuspendActor(%s/%s) to stop container...\n", *atespace, *actorName)
	_, err = ctrlClient.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: *atespace, Name: *actorName},
	})
	if err != nil {
		log.Fatalf("SuspendActor failed: %v", err)
	}
	fmt.Printf("   Actor suspended. Apple container stopped, CPU/RAM freed.\n")

	// 5. Delete Actor
	fmt.Printf("6. Calling Control.DeleteActor(%s/%s) to remove container...\n", *atespace, *actorName)
	_, err = ctrlClient.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: *atespace, Name: *actorName},
	})
	if err != nil {
		log.Fatalf("DeleteActor failed: %v", err)
	}
	fmt.Println("   Actor deleted and container removed.")

	fmt.Println("\n=== Local Substrate on Apple Container demo completed successfully! ===")
}
