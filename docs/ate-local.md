# Local Agent Substrate on Apple Container (`ate-local`)

`ate-local` is a lightweight, local control plane that implements the **Agent Substrate Control API** (`go.ate.dev/substrate/pkg/proto/ateapipb.ControlServer`), backed by the native macOS **Apple `container`** CLI runtime (`/usr/local/bin/container`).

---

## Why `ate-local`?

Agent Substrate is designed for high-density, multi-tenant agent execution on Kubernetes with gVisor. However, for local development on macOS, running a full Kubernetes cluster (`kind` or GKE), gVisor, GCS, and Envoy proxies is heavy and complex.

`ate-local` provides a local alternative:
- Speaks the exact same `Control` gRPC API (`CreateActor`, `ResumeActor`, `SuspendActor`, `RevertActor`, `DeleteActor`, `ListActors`) that AX uses when communicating with Agent Substrate.
- Backed by native macOS Apple containers running on the lightweight macOS virtualization framework.
- Each resumed actor receives a real, routable IPv4 address on the `vmnet` bridge (`192.168.64.x`), enabling direct gRPC streaming turns to the actor.

---

## Architectural Mapping

| Agent Substrate Concept | Production (Kubernetes + gVisor) | Local (`ate-local` + Apple Container) |
|---|---|---|
| **Control Plane** | `ate-api-server` + Redis | `ate-local` in-memory daemon |
| **Worker Pool** | Pre-warmed Pods running `ateworker-gvisor` | Ephemeral or pooled Apple containers |
| **Actor Creation** | Record in Redis | In-memory record + template mapping |
| **Actor Resume** | Hydrate snapshot into worker pod, return Pod IP | Start/run Apple container, return `192.168.64.x` IP |
| **Actor Suspend** | Process checkpoint to GCS snapshot | `container stop`, freeing CPU and RAM |
| **Actor Ingress** | Envoy `ext_proc` + `atunnel` | Direct gRPC to container IP:port |

---

## API RPCs Implemented

- `CreateAtespace` / `GetAtespace` / `ListAtespaces` / `DeleteAtespace`: Manages namespaces for actors.
- `CreateActorTemplate` / `GetActorTemplate` / `ListActorTemplates` / `DeleteActorTemplate`: Manages container templates (`image`, `command`, `env`, `wakeup_probe` port, `resources`).
- `CreateActor`: Registers an actor bound to an `ActorTemplate`.
- `ResumeActor`: Starts/runs the Apple container (`container run -d ...`), inspects the `vmnet` interface, and returns the container IP in `Status.WorkerAssignment.WorkerPodIps`.
- `SuspendActor`: Stops the container (`container stop --time 2`), sets state to `ACTOR_STATE_SUSPENDED`.
- `RevertActor`: Stops the container and reverts state to `ACTOR_STATE_SUSPENDED`.
- `DeleteActor`: Forces deletion of the container (`container delete --force`).
- `GetActor` / `ListActors`: Returns actor metadata, state, and worker IP assignment.
- `CreateActorEgressPolicy` / `GetActorEgressPolicy` / `UpdateActorEgressPolicy` / `DeleteActorEgressPolicy`: Records per-actor egress allowlists for `TaskReconciler`.
- `ListWorkers`: Lists running Apple containers as active workers.

---

## Quickstart: the full AX stack on your Mac

`ate-local` replaces Substrate, and AX stores its resources in PostgreSQL. The whole local stack is:

```
ax CLI ──gRPC──> ax-server ──Control gRPC──> ate-local ──> Apple container
                    │
                    └──> PostgreSQL (resource store, locks)
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
| `local-stack.sh up` | Checks prerequisites and ports, builds the binaries (plus a `linux/arm64` `ax-task-runner`), then starts PostgreSQL, `ate-local` and `ax-server`, waiting for each to accept connections before starting the next. Safe to run again. |
| `local-stack.sh down [--keep-postgres]` | Stops only the processes it started, in reverse order, and removes the PostgreSQL container. |
| `local-stack.sh status` | Shows each component, its PID and port, and any `ate-*` actor containers. |
| `local-stack.sh logs [component]` | Follows `.ax-local/logs/*.log`, or `container logs` for `postgres`. |
| `local-stack.sh reset` | `down`, removes leftover actor containers, then `up`. |

Ports default to PostgreSQL `5432`, `ate-local` `50051` and `ax-server` `8090`. Override them with `AX_LOCAL_POSTGRES_PORT`, `AX_LOCAL_SUBSTRATE_PORT` and `AX_LOCAL_SERVER_PORT`. State (binaries, logs, PID files, generated config) goes in `.ax-local/`, or `AX_LOCAL_DIR`.

### What AX needs from PostgreSQL and `ate-local`

`ax-server` coordinates with Substrate directly under distributed locks. `ate-local`, the `ax` CLI and task containers never connect to PostgreSQL. AX uses PostgreSQL and `ate-local` for:

- **Resource storage (PostgreSQL):** Tasks, Models and Workspaces as protojson JSONB documents (`internal/store/postgres`, tables created automatically on startup).
- **Distributed locks (PostgreSQL):** per-resource lease locks (`locks` table) with automatic lease renewal to serialize operations safely.
- **Live actor status (`ate-local`):** `ax-server` reads the live actor state and worker IP from `ate-local` (`GetActor` / `ListActors`) on every `GetTask` and `ListTasks`.

The local stack runs `postgres:17-alpine` in Apple `container`, so no host PostgreSQL installation is needed.

### Why start order matters

If `ax-server` starts before `ate-local` or PostgreSQL, its connections fail or enter reconnect backoff. `local-stack.sh` avoids this by waiting for PostgreSQL (`pg_isready`) and `ate-local` ports before starting `ax-server`.

### Running the components by hand

```bash
container run -d --name ax-postgres -e POSTGRES_USER=ax -e POSTGRES_PASSWORD=ax -e POSTGRES_DB=ax -p 127.0.0.1:5432:5432 postgres:17-alpine
./bin/ate-local --config examples/ate_local/ate-local.yaml
./bin/ax-server --addr 127.0.0.1:8090 --store "postgres://ax:ax@127.0.0.1:5432/ax?sslmode=disable" \
  --substrate-endpoint 127.0.0.1:50051 --substrate-plaintext \
  --template default --template-atespace default
./bin/ax --server localhost:8090 get tasks
```

Use `127.0.0.1` rather than `localhost` for PostgreSQL: `localhost` can resolve to IPv6 `[::1]`, which the published container port does not listen on.

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `opening store ... dial tcp [::1]:5432: connect: connection refused` | PostgreSQL isn't running, or `localhost` resolved to IPv6. Start PostgreSQL and use `127.0.0.1:5432`. |
| `http2: frame too large, note that the frame header looked like an HTTP/1.1 header` | `ax` reached a non-gRPC server. Without `--server` or `AX_SERVER`, `ax` connects to `localhost:8080`, which is often used by other tools. Run `source .ax-local/env`. |
| Task `Pending` with `SubstrateUnavailable` | `ate-local` wasn't running or was temporarily unreachable when `ax-server` reconciled. Start `ate-local`; `ax-server` retries transient errors with backoff. |
| Task never becomes `Running` | Tasks start `Suspended`; run `ax resume <name>`. |
| Actor container fails to start with the upstream `ax-task-runner` image | That image is `linux/amd64`. Use `examples/ate_local/task-local.yaml`, which runs `debian:12` with the arm64 runner that `local-stack.sh` mounts in. |

PostgreSQL (in an ephemeral container) and `ate-local` both keep state for the current session. Restart them together (`local-stack.sh reset`) to keep tasks and actors consistent.

---

## Limitations (v1)

1. **Process Snapshotting**: Apple `container` does not currently expose a native userspace process checkpoint/restore primitive (like `runsc --checkpoint`). `SuspendActor` stops the container and `ResumeActor` starts/creates it. Memory state is not preserved across hibernation in v1.
2. **Single-host Scope**: Runs locally on macOS.
3. **Architecture**: Apple `container` runs `linux/arm64` guests. Images published only for `linux/amd64`, including the current `ax-task-runner` image, need an arm64 build.
