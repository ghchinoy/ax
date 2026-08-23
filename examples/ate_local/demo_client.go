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
	"log"
	"time"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	"github.com/google/ax/proto"
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
	inputMsg := flag.String("input", "Hello from AX client driving local Substrate on Apple Container!", "Message to send to actor")
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
	_, err = ctrlClient.CreateActor(ctx, &ateapipb.CreateActorRequest{
		ActorRef:               &ateapipb.ActorRef{Atespace: *atespace, Name: *actorName},
		ActorTemplateNamespace: *atespace,
		ActorTemplateName:      *template,
	})
	if err != nil && status.Code(err) != codes.AlreadyExists {
		log.Fatalf("CreateActor failed: %v", err)
	}
	fmt.Println("   Actor registered.")

	// 2. Resume Actor (spawns container and gets IP)
	fmt.Printf("3. Calling Control.ResumeActor(%s/%s)...\n", *atespace, *actorName)
	resumeResp, err := ctrlClient.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: *atespace, Name: *actorName},
	})
	if err != nil {
		log.Fatalf("ResumeActor failed: %v", err)
	}

	actor := resumeResp.GetActor()
	workerIP := actor.GetAteomPodIp()
	fmt.Printf("   Actor resumed! Container Name: %s, Worker IP: %s, Status: %v\n",
		actor.GetAteomPodName(), workerIP, actor.GetStatus())

	if workerIP == "" {
		log.Fatalf("Error: empty worker IP returned from ResumeActor")
	}

	// 3. Connect to Worker Harness/Agent Service if available
	workerAddr := fmt.Sprintf("%s:50053", workerIP)
	fmt.Printf("4. Checking Worker HarnessService at %s...\n", workerAddr)

	dialCtx, dialCancel := context.WithTimeout(ctx, 3*time.Second)
	defer dialCancel()

	workerConn, err := grpc.DialContext(dialCtx, workerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		fmt.Printf("   [Note: no worker service listening on %s (running base image %q). Control plane lifecycle verified!]\n", workerAddr, *template)
	} else {
		defer workerConn.Close()
		agentClient := proto.NewAgentServiceClient(workerConn)
		stream, err := agentClient.Connect(ctx, &proto.AgentRequest{
			ConversationId: "conv-local-substrate",
			ExecId:         "exec-local-substrate-1",
			Start: &proto.AgentStart{
				AgentId: *actorName,
				Messages: []*proto.Message{
					{
						Role: "user",
						Content: &proto.Content{
							Type: &proto.Content_Text{
								Text: &proto.TextContent{Text: *inputMsg},
							},
						},
					},
				},
			},
		})
		if err == nil {
			fmt.Println("5. Streaming execution turn...")
			for {
				resp, err := stream.Recv()
				if err != nil {
					break
				}
				if resp.GetEnd() != nil {
					fmt.Println("\n   [Worker completed turn]")
					break
				}
				if outputs := resp.GetOutputs(); outputs != nil {
					for _, m := range outputs.Messages {
						if txt := m.GetContent().GetText(); txt != nil {
							fmt.Printf("   >>> %s\n", txt.Text)
						}
					}
				}
			}
		}
	}

	// 4. Suspend Actor
	fmt.Printf("6. Calling Control.SuspendActor(%s/%s) to stop container...\n", *atespace, *actorName)
	suspendResp, err := ctrlClient.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
		ActorRef: &ateapipb.ActorRef{Atespace: *atespace, Name: *actorName},
	})
	if err != nil {
		log.Fatalf("SuspendActor failed: %v", err)
	}
	fmt.Printf("   Actor suspended (Status: %v). Container stopped.\n", suspendResp.GetActor().GetStatus())

	fmt.Println("\n=== Local Substrate on Apple Container demo completed successfully! ===")
}
