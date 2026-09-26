# Local Agent Substrate on Apple Container (`ate-local`)

`ate-local` is a lightweight, local control plane that implements the **Agent Substrate Control API** (`github.com/agent-substrate/substrate/pkg/proto/ateapipb.ControlServer`), backed by the native macOS **Apple `container`** CLI runtime (`/usr/local/bin/container`).

---

## Why `ate-local`?

Agent Substrate is designed for high-density, multi-tenant agent execution on Kubernetes with gVisor. However, for local development on macOS, running a full Kubernetes cluster (`kind` or GKE), Valkey/Redis, GCS, and Envoy proxies is heavy and complex.

`ate-local` provides a local alternative:
- Speaks the exact same `Control` gRPC API (`CreateActor`, `ResumeActor`, `SuspendActor`, `DeleteActor`, `ListActors`) that AX uses when communicating with Agent Substrate.
- Backed by native macOS Apple containers running on the lightweight macOS virtualization framework.
- Each resumed actor receives a real, routable IPv4 address on the `vmnet` bridge (`192.168.64.x`), enabling direct gRPC streaming turns to the actor.

---

## Architectural Mapping

| Agent Substrate Concept | Production (Kubernetes + gVisor) | Local (`ate-local` + Apple Container) |
|---|---|---|
| **Control Plane** | `ate-api-server` + Redis | `ate-local` in-memory daemon |
| **Worker Pool** | Pre-warmed Pods running `ateom-gvisor` | Ephemeral or pooled Apple containers |
| **Actor Creation** | Record in Redis | In-memory record + template mapping |
| **Actor Resume** | Hydrate snapshot into worker pod, return Pod IP | Start/run Apple container, return `192.168.64.x` IP |
| **Actor Suspend** | Process checkpoint to GCS snapshot | `container stop`, freeing CPU and RAM |
| **Actor Ingress** | Envoy `ext_proc` + `atunnel` | Direct gRPC to container IP:port |

---

## API RPCs Implemented

- `CreateAtespace` / `GetAtespace` / `ListAtespaces` / `DeleteAtespace`: Manages namespaces for actors.
- `CreateActorTemplate` / `GetActorTemplate` / `ListActorTemplates` / `DeleteActorTemplate`: Manages container templates (`image`, `command`, `env`, `readyz` port).
- `CreateActor`: Registers an actor bound to an `ActorTemplate`.
- `ResumeActor`: Starts/runs the Apple container (`container run -d ...`), inspects the `vmnet` interface, and returns the container IP in `Status.WorkerAssignment.WorkerPodIp`.
- `SuspendActor`: Stops the container (`container stop --time 2`), sets state to `ACTOR_STATE_SUSPENDED`.
- `PauseActor`: Stops the container, sets state to `ACTOR_STATE_PAUSED`.
- `DeleteActor`: Forces deletion of the container (`container delete --force`).
- `GetActor` / `ListActors`: Returns actor metadata, state, and worker IP assignment.
- `CreateActorEgressPolicy` / `GetActorEgressPolicy` / `UpdateActorEgressPolicy` / `DeleteActorEgressPolicy`: Records per-actor egress allowlists for `TaskReconciler`.
- `ListWorkers`: Lists running Apple containers as active workers.

---

## Quickstart: the full AX stack on your Mac

`ate-local` replaces Substrate, but AX has three more pieces. The whole local stack is:

```
ax CLI ──gRPC──> ax-server ──> Valkey ──stream──> ax-controller ──Control gRPC──> ate-local ──> Apple container
```

`examples/ate_local/local-stack.sh` starts all of it in the right order:

```bash
make local-up                       # or: examples/ate_local/local-stack.sh up
source .ax-local/env                # sets AX_SERVER=localhost:8090 and adds ax to PATH

ax apply -f examples/ate_local/task-local.yaml
ax resume local-task                # Tasks are created Suspended
ax describe task local-task         # Phase: Running, Ready: True
ax ssh local-task -- uname -sm      # runs inside the Apple container
ax delete task local-task

make local-down
```

| Command | What it does |
|---|---|
| `local-stack.sh up` | Checks prerequisites and ports, builds the binaries (plus a `linux/arm64` `ax-task-runner`), then starts Valkey, `ate-local`, `ax-controller` and `ax-server`, waiting for each to accept connections before starting the next. Safe to run again. |
| `local-stack.sh down [--keep-valkey]` | Stops only the processes it started, in reverse order, and removes the Valkey container. |
| `local-stack.sh status` | Shows each component, its PID and port, and any `ate-*` actor containers. |
| `local-stack.sh logs [component]` | Follows `.ax-local/logs/*.log`, or `container logs` for `valkey`. |
| `local-stack.sh reset` | `down`, removes leftover actor containers, then `up`. |

Ports default to Valkey `6379`, `ate-local` `50051` and `ax-server` `8090`. Override them with `AX_LOCAL_REDIS_PORT`, `AX_LOCAL_SUBSTRATE_PORT` and `AX_LOCAL_SERVER_PORT`. State (binaries, logs, PID files, generated config) goes in `.ax-local/`, or `AX_LOCAL_DIR`.

### What AX needs from Redis

`ax-server` and `ax-controller` are separate processes that communicate through Redis. `ate-local`, the `ax` CLI and task containers never connect to it. AX uses Redis for:

- **Task storage:** Tasks, Models and Workspaces as protojson values (`SET`, `GET`, `MGET`, `DEL` in `MULTI/EXEC`).
- **Work queue:** `ax-server` appends to the `ax:stream:tasks` stream; `ax-controller` consumes it as the `ax-controllers` consumer group (`XADD`, `XGROUP CREATE`, `XREADGROUP`, `XACK`).
- **Live status:** `ax watch` subscribes to a per-task channel (`PUBLISH`, `SUBSCRIBE`).

Any Redis-protocol server at version 5.0 or later works. The local stack uses Valkey (BSD licensed, also used by Substrate) running in Apple `container`, so no host install is needed.

### Why start order matters

If `ax-controller` starts before `ate-local`, its gRPC connection goes into reconnect backoff, which grows to about two minutes. Calls made during a backoff wait fail with `connection refused` even after `ate-local` is up. `local-stack.sh` avoids this by waiting for each port before starting the next component.

### Running the components by hand

```bash
container run -d --name ax-valkey -p 127.0.0.1:6379:6379 valkey/valkey:8-alpine
./bin/ate-local --config examples/ate_local/ate-local.yaml
./bin/ax-controller --substrate-endpoint 127.0.0.1:50051 --substrate-plaintext --redis-addr 127.0.0.1:6379
./bin/ax-server --addr 127.0.0.1:8090 --redis-addr 127.0.0.1:6379
./bin/ax --server localhost:8090 get tasks
```

Use `127.0.0.1` rather than `localhost` for Redis: `localhost` can resolve to IPv6 `[::1]`, which the published container port does not listen on.

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `redis: ... dial tcp [::1]:6379: connect: connection refused` | Valkey isn't running, or `localhost` resolved to IPv6. Start Valkey and use `--redis-addr 127.0.0.1:6379`. |
| `http2: frame too large, note that the frame header looked like an HTTP/1.1 header` | `ax` reached a non-gRPC server. Without `--server` or `AX_SERVER`, `ax` connects to `localhost:8080`, which is often used by other tools. Run `source .ax-local/env`. |
| Task `Failed` with `AtespaceCreationFailed ... connection refused` | `ate-local` wasn't running when the controller reconciled. The controller does not retry failed events; delete and re-apply the Task. |
| Task stuck in `Terminating` | A delete event failed. Run `ax delete task <name>` again once `ate-local` is reachable; it republishes the event. |
| Task never becomes `Running` | Tasks start `Suspended`; run `ax resume <name>`. |
| Actor container fails to start with the upstream `ax-task-runner` image | That image is `linux/amd64`. Use `examples/ate_local/task-local.yaml`, which runs `debian:12` with the arm64 runner that `local-stack.sh` mounts in. |

Valkey and `ate-local` both keep state in memory. Restart them together (`local-stack.sh reset`) to keep tasks and actors consistent.

---

## Limitations (v1)

1. **Process Snapshotting**: Apple `container` does not currently expose a native userspace process checkpoint/restore primitive (like `runsc --checkpoint`). `SuspendActor` stops the container and `ResumeActor` starts/creates it. Memory state is not preserved across hibernation in v1.
2. **Single-host Scope**: Runs locally on macOS.
3. **Architecture**: Apple `container` runs `linux/arm64` guests. Images published only for `linux/amd64`, including the current `ax-task-runner` image, need an arm64 build.
