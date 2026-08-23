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

	"github.com/google/ax/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const defaultPort = ":50053"

type server struct {
	proto.UnimplementedAgentServiceServer
}

func (s *server) Connect(req *proto.AgentRequest, stream grpc.ServerStreamingServer[proto.AgentResponse]) error {
	start := req.GetStart()
	if start == nil {
		return errors.New("missing start message")
	}

	var userText string
	for _, m := range start.GetMessages() {
		if txt := m.GetContent().GetText(); txt != nil {
			userText = txt.Text
		}
	}

	hostname, _ := os.Hostname()
	reply := fmt.Sprintf("[Local Substrate Actor on %s] Successfully processed: %q", hostname, strings.TrimSpace(userText))

	responseMsg := &proto.Message{
		Role: "assistant",
		Content: &proto.Content{
			Type: &proto.Content_Text{
				Text: &proto.TextContent{Text: reply},
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
		Message: "Local Substrate Actor is healthy",
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

	// Register standard gRPC health service for readiness checks
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	fmt.Printf("Local Substrate Actor worker listening on %s...\n", port)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
