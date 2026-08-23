# Apple Container Sandbox Agent

The **Apple Container Sandbox Agent** (`AppleContainerAgent`) integrates AX with macOS's native `container` runtime CLI, providing secure, isolated execution environments on Apple silicon and macOS.

## Architecture

```
                 +--------------------------+
                 |       AX Controller      |
                 +------------+-------------+
                              |
                     Registry / Router
                              |
               +--------------v---------------+
               |     AppleContainerAgent      |
               +--------------+---------------+
                              |
                Subprocess CLI (/usr/local/bin/container)
                              |
         +--------------------+--------------------+
         |                                         |
+--------v-------------------+    +----------------v-------------------+
|     Sandbox Mode (Exec)     |    |        Service Mode (gRPC)         |
|  - Ephemeral container run |    |  - `container run -d -p ...`       |
|  - Runs scripts / commands |    |  - Waits for TCP readiness         |
|  - Streams stdout to turns |    |  - Connects gRPC AgentService      |
|  - Auto-cleanup on exit    |    |  - Clean teardown on finish        |
+----------------------------+    +------------------------------------+
```

## Modes of Operation

### 1. Sandbox Mode (`mode: "sandbox"` or `"exec"`)
In Sandbox mode, the agent receives user or planner inputs (commands, code, queries) and executes them inside an isolated container instance using `container run --progress none --rm ...`. Output is streamed line-by-line back through AX's output channels.

### 2. Service Mode (`mode: "service"`)
In Service mode, the container runs a gRPC service implementing `proto.AgentService`. AX launches the container with port publishing (`-p <hostPort>:<containerPort>`), waits for the service to be healthy, establishes a gRPC connection, and forwards `Connect` streams.

## Configuration Schema

In `ax.yaml`:

```yaml
registry:
  apple_container_agents:
    - id: "AppleSandboxAgent"             # Required: Unique agent ID
      name: "Apple Container Sandbox"     # Optional: Display name
      description: "Code execution sandbox" # Optional: Description for planner
      image: "debian:12"                  # Required: OCI/Docker image name
      mode: "sandbox"                     # "sandbox" or "service" (default: service if port > 0)
      port: 50051                         # Service port (service mode)
      host_port: 0                        # Host port (0 = auto-allocated)
      command: ["/bin/sh", "-c"]          # Command/entrypoint prefix
      work_dir: "/workspace"              # Working directory inside container
      env:                                # Environment variables
        ENV_VAR: "value"
      volumes:                            # Volume mounts
        - "/path/on/host:/path/in/container"
      cpus: 2                             # CPU cores
      memory: "2048M"                     # Memory limit
      keep_alive: false                   # true to reuse container across turns
      binary_path: "container"            # Path to container CLI (optional)
```

## CLI Usage

```bash
# Execute directly with the agent
ax exec --config ax.yaml --agent AppleSandboxAgent --input "cat /etc/os-release"

# Run as background server
ax serve --config ax.yaml
```
