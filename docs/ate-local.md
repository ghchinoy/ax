# Local Agent Substrate (`ate-local`): Architecture, Multi-Cluster Mobility & Operations Guide

`ate-local` is a standalone, zero-Kubernetes local control and data plane that implements the **Agent Substrate Control gRPC API** (`go.ate.dev/substrate/api/ateapi/v1.ControlServer` on Agent Substrate `v1.0.0`) together with an embedded **`atenet-router`** (`ate-target-actor` HTTP/1.1 + HTTP/2 `h2c` reverse proxy), backed by either the native macOS **Apple `container`** runtime (`/usr/local/bin/container` on `Virtualization.framework`) or **Podman** (`podman`).

It allows `ax-server`, `TaskReconciler`, and the `ax` CLI to provision, resume, inspect (`ax ssh`), suspend, and migrate real Linux sandboxed tasks locally on a developer machine without requiring a Kubernetes cluster (`kind` or GKE), gVisor, Envoy (`atenet-router`), or Google Cloud Storage.

> **Upstream Tracking Issue:** [google/ax#602](https://github.com/google/ax/issues/602)

---

## 1. Motivation & Upstream Alignment

Running AX against a full Agent Substrate installation requires Kubernetes 1.36+ (with `ClusterTrustBundle` and `PodCertificateRequest` beta APIs), worker nodes running Linux kernel $\ge 6.8$ (for OverlayFS `lowerdir+` via `fsconfig`), gVisor (`runsc`), Envoy, and a writable object-store bucket (`docs/installation.md`).

`ate-local` provides a lightweight local implementation of the Substrate Control API that plugs directly into `ax-server` via `--substrate-endpoint 127.0.0.1:50051 --substrate-plaintext`, requiring **zero changes to core AX packages** (`internal/server`, `internal/controller`, `internal/substrate`, `internal/store`, `pkg/apis/v1alpha1`).

### Related Upstream Issues & Pull Requests

| Upstream Issue / PR | Relationship to `ate-local` |
|---|---|
| **[#602](https://github.com/google/ax/issues/602) (`ate-local` Proposal)** | Primary tracking issue and stacked PR breakdown for `ate-local`. |
| **[#526](https://github.com/google/ax/issues/526) / [PR #603](https://github.com/google/ax/pull/603) (`ax local` on Kind)** & **[PR #560](https://github.com/google/ax/pull/560) (Roadmap)** | Complementary local tiers: PR #603 (`ax local <start\|status\|stop\|delete>`, closing #526) provisions full Kubernetes 1.36 + Substrate `v1.0.0` + gVisor inside `kind` (requiring a rootful Docker/Podman daemon, a Linux $\ge 6.8$ kernel, and a 4–8 GiB VM on macOS, while explicitly leaving a Phase 2 native macOS hook in `internal/install/cluster/provider.go`). `ate-local` fulfills the zero-Kubernetes roadmap goal (#560) in ~3.3s with 0 GiB idle VM overhead on Apple `container` microVMs and full support for **rootless Podman** and kernels $< 6.8$. |
| **[#543](https://github.com/google/ax/issues/543) / [PR #627](https://github.com/google/ax/pull/627) (`ax studio`)**, **[PR #618](https://github.com/google/ax/pull/618) (`ax promote`)** & **[PR #590](https://github.com/google/ax/pull/590) (`hack/hero-demo`)** | Enables zero-VM laptop sandboxes for AX Studio (#543/#627), complements `ax promote` (#618) with shared-snapshot `/workspace` hydration across Apple `container` and Podman clusters, and drives all 5 live steps of `hack/hero-demo/run.sh` (`ATE_LOCAL=1`). |
| **[PR #620](https://github.com/google/ax/pull/620) (`--runner-volume-image` `/opt/ax`)** & **[PR #621](https://github.com/google/ax/pull/621) (`/run/ax/*` launcher files)** | Supports both default `/usr/local/bin/ax-task-runner` injection and `--runner-volume-image` (`/opt/ax/ax-task-runner`) bind-mounts on stock and pure agent images without requiring registry image pulls. |
| **[Epic #563](https://github.com/google/ax/issues/563): `Profile` ([#586](https://github.com/google/ax/pull/586)), `Task` ([#587](https://github.com/google/ax/pull/587)), `Gateway` ([#585](https://github.com/google/ax/pull/585)), & Warm `Workspace` `Tag`s ([#584](https://github.com/google/ax/issues/584) / [#592](https://github.com/google/ax/pull/592))** | Supports `TaskSpec.profile` (`image`, `resources`, `ateActorTemplate`), `Gateway` egress policy binding (`default` `EgressPolicy`), snapshot `Tag` CRUD (`CreateTag`, `GetTag`, `ListTags`, `DeleteTag`) + `CreateActor(source_tag)`, and `ATENET_ROUTER_ADDR` routing for `ax ssh` after #587 removed `TaskStatus.worker_ip`. |
| **[#518](https://github.com/google/ax/issues/518) / [PR #551](https://github.com/google/ax/pull/551) (Keep maiden-run marker on `/workspace`)** & **[#555](https://github.com/google/ax/issues/555) (`/ax-state` volume)** | `ate-local` implements `SNAPSHOT_FIDELITY_VOLUMES` semantics by archiving `/workspace` to `<snapshot_dir>/<atespace>/<actor>.tar.gz` on `SuspendActor` (and copying to `tags/<tag>.tar.gz` on `CreateTag`) and hydrating it on `ResumeActor`. Because #551 stores `.ax/initialized` inside `/workspace`, resuming a migrated task on a second cluster preserves agent edits without re-running maiden workspace setup. |
| **[PR #561](https://github.com/google/ax/pull/561) (Substrate `v1.0.0`)**, **[#539](https://github.com/google/ax/issues/539) (`UpdateTime`)** & **[PR #556](https://github.com/google/ax/pull/556) (`actorUid`)** | Implements `go.ate.dev/substrate/api/ateapi/v1.ControlServer` (`v1.0.0`), populates `Metadata.Uid`, `Metadata.CreateTime`, and `Metadata.UpdateTime`, and records completed `Snapshot` generations on suspend. |

---

## 2. Architecture & Runtime Backends

### Production Substrate vs. `ate-local`

| Agent Substrate Concept | Production (Kubernetes + gVisor) | `ate-local` (`runtime: apple`) | `ate-local` (`runtime: podman`) |
|---|---|---|---|
| **Control Plane** | `ate-api-server` | `ate-local` gRPC daemon | `ate-local` gRPC daemon |
| **Container Runtime** | `ateworker-gvisor` Pods (`runsc`) | macOS native Apple `container` (`Virtualization.framework`) | Podman (`podman` on Podman Machine VM) |
| **Actor Networking (`WorkerPodIps[0]`)** | Pod IP routed via `atenet-router` | Direct routable `vmnet` bridge IP (`192.168.64.x`) | Published localhost port (`127.0.0.1:<hostPort>`) |
| **Actor Suspend (`SuspendActor`)** | `runsc` checkpoint / volume snapshot to GCS | Archives `/workspace` to `snapshot_dir` + `container stop` | Archives `/workspace` to `snapshot_dir` + `podman stop` |
| **Actor Resume (`ResumeActor`)** | Hydrates snapshot into worker pod | Starts/creates Apple container + restores `/workspace` snapshot | Starts/creates Podman container + restores `/workspace` snapshot |
| **Guest Exec (`ax ssh`) & Ingress** | `atenet-router` (`ate-target-actor`) | Embedded `atenet-router` on `ate-local` (`ATENET_ROUTER_ADDR=127.0.0.1:50051`) -> `192.168.64.x:80` | Embedded `atenet-router` on `ate-local` (`ATENET_ROUTER_ADDR=127.0.0.1:50052`) -> `127.0.0.1:<hostPort>` |

### Why Both Runtimes Work Without Modifying `ax` or `ax-server`

1. **Readiness Probing (`TaskReconciler.Resume`):** `internal/controller/reconciler.go` inspects `WorkerPodIps[0]` with `net.SplitHostPort(workerIP)`:
   - **Apple `container` (`runtime: apple`):** Assigns each container a dedicated IPv4 address on the macOS `vmnet` bridge (`192.168.64.x`). `ate-local` returns `"192.168.64.x"` in `WorkerPodIps[0]`.
   - **Podman (`runtime: podman`):** Launches containers with `-p 127.0.0.1::<port>` and parses `podman inspect` (`NetworkSettings.Ports`) to return `"127.0.0.1:<hostPort>"` in `WorkerPodIps[0]`.
2. **Embedded `atenet-router` Data Plane (`ax ssh` & HTTP/gRPC Wake-on-Request):**
   - `ax ssh` (`cmd/ax/main.go`) connects through `ATENET_ROUTER_ADDR`, sending HTTP/2 (`h2c`) gRPC requests with the `ate-target-actor: <atespace>/<actor>` metadata header.
   - `ate-local` multiplexes HTTP/1.1 and unencrypted HTTP/2 (`h2c`) on its listener (`:50051`): requests carrying `ate-target-actor` wake suspended actors on demand and stream directly to the actor's `192.168.64.x:80` (Apple `container`) or `127.0.0.1:<hostPort>` (Podman) endpoint, while Control gRPC RPCs dispatch to `ControlServer`.

### Control API Surface Implemented (`internal/atelocal`)

- **Atespaces:** `CreateAtespace`, `GetAtespace`, `ListAtespaces`, `DeleteAtespace`
- **ActorTemplates:** `CreateActorTemplate`, `GetActorTemplate`, `ListActorTemplates`, `DeleteActorTemplate` (populates `Metadata.Uid`; maps `containers[0]` image, command, env, wakeup probe port, and CPU/memory resource limits)
- **Actors:** `CreateActor` (including `source_tag` seeding), `GetActor`, `ListActors`, `ResumeActor`, `SuspendActor`, `RevertActor`, `DeleteActor`
- **Snapshot Tags:** `CreateTag`, `GetTag`, `ListTags`, `DeleteTag`
- **Egress Policies:** `CreateActorEgressPolicy`, `GetActorEgressPolicy`, `UpdateActorEgressPolicy`, `DeleteActorEgressPolicy`
- **Workers:** `ListWorkers`

---

## 3. Single-Cluster Quickstart (Apple `container`)

The single-cluster local stack runs:

```
ax CLI ──gRPC (:8090)──────────> ax-server ──Control gRPC (:50051)──> ate-local ──> Apple container
  │                                  │                                    ▲
  │                                  └──> PostgreSQL (:5432, store/locks) │
  └─ATENET_ROUTER_ADDR (:50051)───────────ate-target-actor (h2c / HTTP)───┘
```

Start the stack with `make local-up` (or `./examples/ate_local/local-stack.sh up`):

```bash
make local-up                       # starts PostgreSQL, ate-local, and ax-server in order
source .ax-local/env                # sets AX_SERVER=localhost:8090, ATENET_ROUTER_ADDR=127.0.0.1:50051, and PATH

ax apply -f examples/ate_local/task-local.yaml
ax resume local-task                # Tasks are created in Suspended state
ax describe task local-task         # State: Working, Actor: local-task, Actor UID: ..., Ready: True
ax ssh local-task -- uname -sm      # executes inside the Linux arm64 sandbox via ATENET_ROUTER_ADDR
ax delete task local-task

make local-down
```

### `local-stack.sh` Commands

| Command | Description |
|---|---|
| `local-stack.sh up` | Checks prerequisites and ports, builds `ax`, `ax-server`, `ate-local`, and `linux/arm64` `ax-task-runner`, then starts PostgreSQL, `ate-local`, and `ax-server` in dependency order. |
| `local-stack.sh down [--keep-postgres]` | Stops `ax-server` and `ate-local` in reverse order, and removes the PostgreSQL container unless `--keep-postgres` is passed. |
| `local-stack.sh status` | Shows each component, its PID and port, and any running/stopped `ate-*` actor containers across Apple `container` and `podman`. |
| `local-stack.sh logs [component]` | Follows `.ax-local/logs/*.log`, or `container logs --follow` for `postgres`. |
| `local-stack.sh reset` | Runs `down`, removes any leftover `ate-*` actor containers, and runs `up`. |
| `local-stack.sh multi-up` | Starts the dual-cluster topology: Cluster A on Apple `container` (`:8090` / `:50051`) and Cluster B on `podman` (`:8091` / `:50052`), sharing `.ax-local/snapshots/`. |
| `local-stack.sh multi-down [--keep-postgres]` | Stops both clusters and optionally preserves PostgreSQL. |
| `local-stack.sh migrate <task> [atespace]` | Suspends `<task>` on Cluster A (snapshotting `/workspace`), exports & applies the task manifest to Cluster B, and resumes it on Cluster B (hydrating `/workspace`). |

---

## 4. Dual-Cluster Task Mobility Design & Walkthrough (Apple `container` $\leftrightarrow$ `podman`)

To demonstrate and test moving stateful AX tasks across distinct Agent Substrate clusters and container runtimes on a single Mac, `local-stack.sh multi-up` stands up two isolated AX + Substrate stacks that share a local snapshot store (`.ax-local/snapshots/`):

```
  Cluster A (Apple container)                     Cluster B (Podman)
  ───────────────────────────                     ──────────────────
  ax --server localhost:8090                      ax --server localhost:8091
              │                                               │
              ▼                                               ▼
       ax-server (:8090)                              ax-server-b (:8091)
       (Postgres DB: ax)                          (Postgres DB: ax_cluster_b)
              │                                               │
              ▼                                               ▼
       ate-local (:50051)                             ate-local-b (:50052)
       runtime: apple                                 runtime: podman
       prefix:  ate-apple                             prefix:  ate-podman
              │                                               │
     ┌────────┴────────┐                             ┌────────┴────────┐
     ▼                 ▼                             ▼                 ▼
Apple container        │                             │         Podman container
(192.168.64.x:80)      │                             │         (127.0.0.1:<port>)
                       ▼                             ▼
                 ┌─────────────────────────────────────────┐
                 │      Shared Snapshot Storage            │
                 │  .ax-local/snapshots/<atespace>/<actor> │
                 └─────────────────────────────────────────┘
```

### How Cross-Cluster Snapshot Migration Works

1. **Execution on Cluster A (`apple`):**
   - `ax-task-runner` boots inside an Apple `container` (`ate-apple-default-local-task`), initializes `/workspace`, writes `/workspace/.ax/initialized` (or `/workspace/.git/.ax/initialized` for Git workspaces, per [#551](https://github.com/google/ax/pull/551)), and serves `/readyz?check=workspace` and the guest gRPC `ProcessService`/`FileSystemService` on port 80.
   - The agent (or user via `ax ssh`) modifies files inside `/workspace`.
2. **Checkpoint on Suspend (`SuspendActor` on Cluster A):**
   - When `ax --server $AX_SERVER_A suspend local-task` is called, Cluster A's `ate-local` archives `/workspace` (`tar -czf - -C /workspace .`) from the running container and atomically writes `.ax-local/snapshots/default/local-task.tar.gz` before stopping the container.
3. **Manifest Promotion (`Cluster A` $\rightarrow$ `Cluster B`):**
   - The task spec is exported from Cluster A (`ax --server $AX_SERVER_A get task local-task -o yaml`), stripped of its runtime `status:` block, and applied to Cluster B (`ax --server $AX_SERVER_B apply -f -`), creating the task in `Suspended` phase on Cluster B.
4. **Hydration on Resume (`ResumeActor` on Cluster B):**
   - When `ax --server $AX_SERVER_B resume local-task` is called, Cluster B's `ate-local` launches a fresh container on its runtime (`ate-podman-default-local-task`), waits for port readiness, and extracts `.ax-local/snapshots/default/local-task.tar.gz` into `/workspace` before returning `WorkerPodIps: ["127.0.0.1:<hostPort>"]` to `ax-server-b`.
   - Because `/workspace/.ax/initialized` is preserved in the snapshot, the task's workspace state and agent edits are immediately available on Cluster B.

### Step-by-Step Dual-Cluster Demo

*(Prerequisite for Podman on Cluster B: `brew install podman && podman machine init && podman machine start`. If `podman` is not installed, `multi-up` automatically runs Cluster B as a second isolated Apple `container` cluster with prefix `ate-cluster-b`.)*

```bash
# 1. Start both clusters (Cluster A on :8090, Cluster B on :8091)
make local-multi-up
source .ax-local/env

# 2. Create and resume a task on Cluster A (Apple container)
ATENET_ROUTER_ADDR=$ATENET_ROUTER_ADDR_A ax --server $AX_SERVER_A apply -f examples/ate_local/task-local.yaml
ATENET_ROUTER_ADDR=$ATENET_ROUTER_ADDR_A ax --server $AX_SERVER_A resume local-task
ATENET_ROUTER_ADDR=$ATENET_ROUTER_ADDR_A ax --server $AX_SERVER_A describe task local-task

# 3. Write work inside /workspace on Cluster A
ATENET_ROUTER_ADDR=$ATENET_ROUTER_ADDR_A ax --server $AX_SERVER_A ssh local-task -- sh -c \
  'echo "checkpointed on Cluster A ($(hostname))" > /workspace/progress.txt && cat /workspace/progress.txt'

# 4. Migrate the task from Cluster A (:8090) to Cluster B (:8091)
./examples/ate_local/local-stack.sh migrate local-task

# 5. Inspect and verify the migrated task on Cluster B
ATENET_ROUTER_ADDR=$ATENET_ROUTER_ADDR_B ax --server $AX_SERVER_B describe task local-task
ATENET_ROUTER_ADDR=$ATENET_ROUTER_ADDR_B ax --server $AX_SERVER_B ssh local-task -- cat /workspace/progress.txt

# 6. Tear down both clusters when finished
make local-multi-down
```

---

## 5. Reviewer Guide & Code Layout

All `ate-local` code is isolated from core AX packages so it adds zero maintenance burden to the production Kubernetes/Substrate code paths:

| Path | Purpose |
|---|---|
| `cmd/ate-local/main.go` | CLI entrypoint for the local Substrate Control + `atenet-router` HTTP/2 (`h2c`) daemon (`--config`, `--addr`, `--runtime`, `--snapshot-dir`, `--container-prefix`). |
| `internal/atelocal/config.go` | YAML configuration loader and defaults (`runtime`, `snapshot_dir`, `container_prefix`, `templates`). |
| `internal/atelocal/runner.go` | `ContainerRunner` interface with `apple` (`/usr/local/bin/container`) and `podman` (`podman`) implementations, CLI flag translation, and `inspect` JSON parsers. |
| `internal/atelocal/server.go` | In-memory `ateapi/v1.ControlServer` implementation, embedded `atenet-router` (`Handler` / `NewHTTPServer` with wake-on-request), `Tag` CRUD + `source_tag` seeding, `ActorTemplate` resource mapping, and `/workspace` `.tar.gz` snapshot save/restore. |
| `internal/atelocal/server_test.go` | Unit tests for `ControlServer` RPCs, `Gateway` egress binding, `Tag` + `atenet-router` proxy (`TestTagAndRouterProxy`), `PodmanRunner` CLI/inspect translation, cross-cluster `apple` $\rightarrow$ `podman` snapshot migration, and opt-in live Apple `container` reconciler + `ax ssh` integration tests. |
| `examples/ate_local/local-stack.sh` | Single-cluster (`up`/`down`) and dual-cluster (`multi-up`/`multi-down`/`migrate`) orchestration script. |
| `docs/ate-local.md` | Canonical architecture, upstream alignment, and operations documentation. |

---

## 6. Troubleshooting & Known Limitations

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `opening store ... dial tcp [::1]:5432: connect: connection refused` | PostgreSQL isn't running, or `localhost` resolved to IPv6. Start PostgreSQL and use `127.0.0.1:5432`. |
| `http2: frame too large, note that the frame header looked like an HTTP/1.1 header` | `ax` reached a non-gRPC server. Without `--server` or `AX_SERVER`, `ax` connects to `localhost:8080`, which is often used by other tools. Run `source .ax-local/env`. |
| Task creation or resume fails with `SubstrateUnavailable` | `ate-local` wasn't running or was temporarily unreachable when `ax-server` reconciled. Start `ate-local`; `ax-server` retries transient errors with backoff. |
| Task stays `Suspended` (never reaches `Working`) | Tasks start `Suspended`; run `ax resume <name>`. |
| `ax ssh` attempts `kubectl` port-forward to `ate-system` | `ATENET_ROUTER_ADDR` is unset in your shell. Run `source .ax-local/env` (or pass `ATENET_ROUTER_ADDR=127.0.0.1:50051`). |
| Actor container fails to start with the upstream `ax-task-runner` image | That image is `linux/amd64`. Use `examples/ate_local/task-local.yaml`, which runs `debian:12` with the `linux/arm64` runner binary that `local-stack.sh` builds and mounts into `/usr/local/bin/ax-task-runner`. |

### Limitations

1. **Volumes-Fidelity Snapshotting (`SNAPSHOT_FIDELITY_VOLUMES`):** Neither Apple `container` nor Podman on macOS exposes a gVisor-style userspace memory checkpoint/restore primitive (`runsc --checkpoint`). `SuspendActor` snapshots `/workspace` and stops the container, and `ResumeActor` restores `/workspace` into the resumed/migrated container. In-memory process state is not preserved across suspend/resume.
2. **Architecture:** Apple Silicon Macs run `linux/arm64` containers natively in both Apple `container` and `podman`. `local-stack.sh` cross-compiles `ax-task-runner` for `linux/arm64` and mounts it into task containers via the default template.
