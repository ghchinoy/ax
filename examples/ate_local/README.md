# ate-local: Local Agent Substrate on Apple Container

`ate-local` is a standalone daemon that implements the **Agent Substrate `Control` gRPC API** (`ateapipb.ControlServer`), backed by the macOS native **Apple `container` runtime**.

It allows upstream `ax` (and other Substrate clients) to run agent harnesses inside isolated Apple containers locally on macOS without requiring a Kubernetes cluster, GCS, Redis, or cloud resources.

---

## Architecture

```
                 +--------------------------------+
                 |       AX Controller / CLI      |
                 +---------------+----------------+
                                 |
                 Control gRPC    | (CreateActor, ResumeActor, SuspendActor)
                 target: :50051  |
                                 v
                 +--------------------------------+
                 |           ate-local            |
                 | (Substrate Control gRPC Server)|
                 +---------------+----------------+
                                 |
                      CLI subprocess commands
                      (/usr/local/bin/container)
                                 |
               +-----------------+-----------------+
               |                                   |
    container run -d ...                container inspect / stop
               |                                   |
               v                                   v
    +-----------------------+           +-----------------------+
    |   Actor Container 1   |           |   Actor Container 2   |
    | (192.168.64.x:50053)  |           | (192.168.64.y:50053)  |
    |  - HarnessService     |           |  - HarnessService     |
    +-----------------------+           +-----------------------+
```

---

## How It Works

1. **`CreateActor`**: Registers an actor in an atespace (e.g. `ax/my-actor`) associated with an `ActorTemplate`.
2. **`ResumeActor`**:
   - Launches an Apple container for the actor (`container run -d --name ate-<atespace>-<name> ...`).
   - Retrieves the container's routable IP on the macOS `vmnet` bridge (`192.168.64.x`).
   - Returns the IP as `Actor.AteomPodIp`.
   - AX then dials `AteomPodIp:50053` directly to stream execution turns over `proto.HarnessService` / `proto.AgentService`.
3. **`SuspendActor`**: Stops the container (`container stop`), freeing up CPU and memory resources on the Mac.
4. **`DeleteActor`**: Tears down and deletes the container (`container delete --force`).

---

## Quickstart Demo

Run the automated demo:

```bash
./examples/ate_local/run_demo.sh
```

Or run step-by-step:

### 1. Build and Start `ate-local`

```bash
go build -o bin/ate-local ./cmd/ate-local
./bin/ate-local --config examples/ate_local/ate-local.yaml
```

### 2. Run an Actor turn

```bash
go run ./examples/ate_local/demo_client.go --actor my-test-actor --input "Hello Substrate!"
```

---

## Configuration (`ate-local.yaml`)

```yaml
address: "127.0.0.1:50051"
ready_timeout: "20s"

templates:
  # General container template
  default:
    image: "debian:12"
    port: 50053
    command: ["sleep", "infinity"]
    cpus: 2
    memory: "1024M"

  # Custom harness template
  ax-harness-antigravity-template:
    image: "ax-harness-antigravity:latest"
    port: 50053
    cpus: 4
    memory: "2048M"
    env:
      PYTHONUNBUFFERED: "1"
```
