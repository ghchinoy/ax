# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy module files and dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build agent binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/apple-container-agent ./examples/apple_container_agent/main.go

# Minimal runtime image
FROM debian:12-slim

WORKDIR /app
COPY --from=builder /bin/apple-container-agent /usr/local/bin/apple-container-agent

EXPOSE 50051

ENV PORT=50051

ENTRYPOINT ["/usr/local/bin/apple-container-agent"]
