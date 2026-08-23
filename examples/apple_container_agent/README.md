# Apple Container Sandbox Agent for AX

This example demonstrates running AX agents and sandboxes inside macOS **Apple Containers** using the native `container` runtime CLI (`/usr/local/bin/container`).

## Features

- **Sandbox Mode (`mode: "sandbox"`)**: Directly runs shell commands, scripts, or programs inside an isolated container sandbox (e.g. `debian:12`, `ubuntu:24.04`, `alpine:latest`).
- **Service Mode (`mode: "service"`)**: Starts an isolated container hosting an AX gRPC Agent Service, connects to it over a mapped port, streams the execution turn, and cleanly tears it down on completion.
- **Resource Controls**: Configure CPU cores (`cpus`), memory limits (`memory`), work directory (`work_dir`), volume mounts (`volumes`), and environment variables (`env`).
- **Session Modes**: Ephemeral (default: clean container per turn) or persistent per conversation (`keep_alive: true`).

## Prerequisites

1. macOS with Apple `container` CLI installed (available at `/usr/local/bin/container` or in `PATH`).
2. Go 1.25+ installed.

## Configuration in `ax.yaml`

```yaml
registry:
  apple_container_agents:
    # Sandbox mode: runs commands directly in an isolated container
    - id: "AppleSandboxAgent"
      name: "Apple Container Sandbox Agent"
      description: "Executes shell commands in an isolated Apple container sandbox."
      image: "debian:12"
      mode: "sandbox"
      command: ["/bin/sh", "-c"]
      cpus: 2
      memory: "1024M"

    # Service mode: runs an AX gRPC Agent service inside a container
    - id: "AppleServiceAgent"
      name: "Apple Container Service Agent"
      description: "Runs an AX gRPC Agent service inside an Apple container."
      image: "ax-apple-agent:latest"
      mode: "service"
      port: 50051
      host_port: 0 # auto-allocated host port
      cpus: 2
      memory: "1024M"
```

## Running the Sandbox Example

Run the automated demo script:

```bash
./examples/apple_container_agent/run_apple_container_agent.sh
```

Or execute directly using the `ax` CLI:

```bash
# Build ax CLI
go build -o bin/ax ./cmd/ax

# Run command inside Apple Container sandbox
./bin/ax exec --config examples/apple_container_agent/ax.yaml --agent AppleSandboxAgent --input "uname -a"
```

## Building a Custom Agent Image

To build an AX agent image with the Apple container builder:

```bash
container build -t ax-apple-agent:latest -f examples/apple_container_agent/Dockerfile .
```
