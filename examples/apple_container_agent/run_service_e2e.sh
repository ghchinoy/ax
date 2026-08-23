#!/bin/bash
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

set -e

cleanup() {
    echo "Cleaning up background processes..."
    kill -9 $AGENT_PID 2>/dev/null || true
    kill -9 $(lsof -t -i:50051) 2>/dev/null || true
}
trap cleanup EXIT

echo "Cleaning up port 50051 if in use..."
kill -9 $(lsof -t -i:50051) 2>/dev/null || true

echo "Starting AppleContainerAgent server in background..."
go run examples/apple_container_agent/main.go &
AGENT_PID=$!

echo "Waiting for agent to be ready..."
sleep 2

echo "Running test client..."
go run examples/apple_container_agent/test/test_apple_container_agent.go
