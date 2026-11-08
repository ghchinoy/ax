# `examples/ate_local`: Local Agent Substrate & Multi-Cluster Demo

This directory contains configuration, manifests, and helper scripts for running AX locally on macOS via `ate-local` (backed by Apple `container` and/or `podman`) without Kubernetes.

For the full architecture, upstream issue/PR context, and dual-cluster migration design, see **[`docs/ate-local.md`](../../docs/ate-local.md)**.

---

## Files in This Directory

| File | Description |
|---|---|
| `local-stack.sh` | Starts and manages the full local AX stack (`up`, `down`, `status`, `logs`, `reset`) as well as the dual-cluster migration topology (`multi-up`, `multi-down`, `migrate`). |
| `task-local.yaml` | Example `Workspace` and `Task` manifest (`debug: true`) configured for local `linux/arm64` execution. |
| `ate-local.yaml` | Standalone configuration example for running `cmd/ate-local` directly. |
| `run_demo.sh` / `demo_client.go` / `worker/` | Standalone `ateapi/v1.ControlServer` gRPC demo exercising `ate-local` directly without `ax-server`. |

---

## Quickstart

### Single Cluster (Apple `container`)

```bash
make local-up
source .ax-local/env

ax apply -f examples/ate_local/task-local.yaml
ax resume local-task
ax describe task local-task
ax ssh local-task -- uname -sm
ax delete task local-task

make local-down
```

### Dual Cluster Task Migration (Apple `container` $\leftrightarrow$ `podman`)

```bash
make local-multi-up
source .ax-local/env

ax --server $AX_SERVER_A apply -f examples/ate_local/task-local.yaml
ax --server $AX_SERVER_A resume local-task
ax --server $AX_SERVER_A ssh local-task -- sh -c 'echo "step 1 on cluster A" > /workspace/progress.txt'

./examples/ate_local/local-stack.sh migrate local-task

ax --server $AX_SERVER_B describe task local-task
ax --server $AX_SERVER_B ssh local-task -- cat /workspace/progress.txt

make local-multi-down
```
