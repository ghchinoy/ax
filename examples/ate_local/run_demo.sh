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

echo "=== ate-local: Local Agent Substrate on Apple Container Demo ==="

# Check if container CLI is installed
if ! command -v container &> /dev/null; then
    echo "Error: Apple 'container' CLI tool not found in PATH."
    exit 1
fi

cleanup() {
    echo "Cleaning up..."
    kill -9 $DAEMON_PID 2>/dev/null || true
    kill -9 $(lsof -t -i:50051) 2>/dev/null || true
}
trap cleanup EXIT

echo "1. Building ate-local daemon..."
go build -o bin/ate-local ./cmd/ate-local

echo "2. Cleaning up port 50051 if in use..."
kill -9 $(lsof -t -i:50051) 2>/dev/null || true

echo "3. Starting ate-local daemon in background..."
./bin/ate-local --config examples/ate_local/ate-local.yaml &
DAEMON_PID=$!

echo "Waiting for ate-local to listen on :50051..."
sleep 2

echo "4. Running Substrate Control client demo..."
go run ./examples/ate_local/demo_client.go --actor demo-actor-live --template default --input "Testing local Substrate on Apple Container"
