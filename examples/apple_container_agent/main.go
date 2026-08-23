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
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	"github.com/google/ax/proto"
)

const defaultPort = ":50051"

// server implements proto.AgentServiceServer for the Apple Container Agent.
type server struct {
	proto.UnimplementedAgentServiceServer
}

func (s *server) Connect(req *proto.AgentRequest, stream grpc.ServerStreamingServer[proto.AgentResponse]) error {
	start := req.GetStart()
	if start == nil {
		return errors.New("missing start message")
	}

	var messages []*proto.Message
	for _, input := range start.Messages {
		if input.GetContent().GetText() != nil {
			messages = append(messages, input)
		}
	}
	if len(messages) == 0 {
		return errors.New("no text inputs received")
	}

	lastMsg := messages[len(messages)-1].GetContent().GetText().Text
	hostname, _ := os.Hostname()

	replyText := fmt.Sprintf("[AppleContainerAgent on %s] Processed: %s", hostname, strings.TrimSpace(lastMsg))

	responseMsg := &proto.Message{
		Role: "assistant",
		Content: &proto.Content{
			Type: &proto.Content_Text{
				Text: &proto.TextContent{
					Text: replyText,
				},
			},
		},
	}

	if err := stream.Send(&proto.AgentResponse{
		ConversationId: req.ConversationId,
		ExecId:         req.ExecId,
		Type: &proto.AgentResponse_Outputs{
			Outputs: &proto.AgentOutputs{
				Messages: []*proto.Message{responseMsg},
			},
		},
	}); err != nil {
		return err
	}

	return stream.Send(&proto.AgentResponse{
		ConversationId: req.ConversationId,
		ExecId:         req.ExecId,
		Type: &proto.AgentResponse_End{
			End: &proto.AgentEnd{},
		},
	})
}

func (s *server) HealthCheck(ctx context.Context, req *proto.HealthCheckRequest) (*proto.HealthCheckResponse, error) {
	return &proto.HealthCheckResponse{
		Healthy: true,
		Message: "AppleContainerAgent is healthy",
	}, nil
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = defaultPort
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}

	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", port, err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterAgentServiceServer(grpcServer, &server{})

	fmt.Printf("AppleContainerAgent gRPC server listening on %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
