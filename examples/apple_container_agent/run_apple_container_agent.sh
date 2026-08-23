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

echo "=== Apple Container Agent Demo ==="

# Check if container CLI is installed
if ! command -v container &> /dev/null; then
    echo "Error: Apple 'container' CLI tool not found in PATH."
    exit 1
fi

echo "1. Building ax CLI binary..."
go build -o bin/ax ./cmd/ax

echo "2. Testing Sandbox Execution in Apple Container (debian:12)..."
./bin/ax exec --config examples/apple_container_agent/ax.yaml --agent AppleSandboxAgent --input "uname -a && cat /etc/os-release | head -n 3"

echo ""
echo "=== Apple Container Sandbox test completed successfully! ==="
