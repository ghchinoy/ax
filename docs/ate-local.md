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

- `CreateAtespace(CreateAtespaceRequest)`: Registers namespace for actors.
- `GetAtespace(GetAtespaceRequest)`: Checks namespace existence.
- `ListAtespaces(ListAtespacesRequest)`: Lists namespaces.
- `DeleteAtespace(DeleteAtespaceRequest)`: Deletes namespace if empty.
- `CreateActor(CreateActorRequest)`: Registers actor with template binding.
- `ResumeActor(ResumeActorRequest)`: Starts Apple container, inspects network, returns `AteomPodIp`.
- `SuspendActor(SuspendActorRequest)`: Stops container, sets status to `STATUS_SUSPENDED`.
- `PauseActor(PauseActorRequest)`: Pauses container.
- `DeleteActor(DeleteActorRequest)`: Forces deletion of container.
- `GetActor(GetActorRequest)`: Returns actor state.
- `ListActors(ListActorsRequest)`: Returns all actors in atespace.
- `ListWorkers(ListWorkersRequest)`: Lists running containers as active workers.
- `DebugClear(DebugClearRequest)`: Stops and deletes all managed containers.

---

## Usage

### 1. Start `ate-local`

```bash
go build -o bin/ate-local ./cmd/ate-local
./bin/ate-local --config examples/ate_local/ate-local.yaml
```

### 2. Configure `ax` to use local Substrate

Point AX's Substrate/ATE endpoint to `localhost:50051`:

```yaml
ate:
  endpoint: "localhost:50051"
```

---

## Limitations (v1)

1. **Process Snapshotting**: Apple `container` does not currently expose a native userspace process checkpoint/restore primitive (like `runsc --checkpoint`). `SuspendActor` stops the container and `ResumeActor` starts/creates it. Memory state is not preserved across hibernation in v1.
2. **Single-host Scope**: Runs locally on macOS.
