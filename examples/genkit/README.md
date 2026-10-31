# Genkit Go runner

`genkit-runner` is a [runner](../../docs/runner.md) that serves a [Genkit Go](https://genkit.dev/docs/go/overview/) agent configured declaratively in the `Task` manifest. It embeds the standard `runner` package, so workspace setup, probes, guest services (`ax ssh`), and shutdown work as they do with the default runner.

When a task leaves `spec.command` empty and sets `spec.config`, the runner builds a store-backed Genkit agent from `spec.config` (using Genkit's [Dotprompt](https://genkit.dev/docs/go/dotprompt/) frontmatter schema) and serves it on port 80 over both:

- **A2A JSON-RPC**: `POST /` and `GET /.well-known/agent-card.json`
- **Genkit native HTTP agent routes**: `POST /agents/<name>`, `POST /agents/<name>/getSnapshot`, `POST /agents/<name>/waitForSnapshot`, and `POST /agents/<name>/abort`

Session snapshots are persisted in `/workspace/.genkit/snapshots` using `localstore.NewFileSessionStore`, so multi-turn conversations automatically survive `ax suspend` and `ax resume`.

A task that sets `spec.command` runs that command unchanged.

## Build the image

The image is built from [`Dockerfile.genkit`](../../Dockerfile.genkit):

```bash
make push-genkit-image GENKIT_REPO=<your-registry>/<image>
```

Pin the printed digest in `spec.image`.

## Configure a task

`spec.config` accepts any [Dotprompt frontmatter](https://genkit.dev/docs/go/dotprompt/) key (`model`, `config`, `maxTurns`, `tools`, `use`, `input`, `output`), plus `name`, `description`, `instruction` (rendered as the system prompt), or `promptFile` (path to an existing `.prompt` file in the workspace):

```yaml
apiVersion: ax.io/v1alpha1
kind: Task
metadata:
  name: genkit-triage
spec:
  image: "gcr.io/my-project/genkit-runner@sha256:..."
  config:
    name: triage_agent
    description: Triages failing tests.
    model: googleai/gemini-3.8-flash
    instruction: |
      You are a careful engineer. Reproduce failures before fixing them.
      Write a report of what you found to /workspace/genkit-output.md.
    config:
      maxOutputTokens: 8192
    maxTurns: 15
    use:
      - name: genkit-middleware/filesystem
        config:
          rootDirectory: /workspace
          allowWriteAccess: true
      - name: genkit-middleware/skills
  env:
    - name: GEMINI_API_KEY
      value: "${GEMINI_API_KEY}"
  workspace:
    name: itsdangerous-genkit
  debug: true
```

## Talking to the agent

Send requests through the `atenet-router` with `ate-target-actor: <atespace>/<task>`:

```bash
kubectl -n ate-system port-forward svc/atenet-router 8001:80

# 1. Send a message via A2A JSON-RPC
curl -H "ate-target-actor: default/genkit-triage" -H "Content-Type: application/json" \
  http://localhost:8001/ -d '{
    "jsonrpc": "2.0", "id": 1, "method": "message/send",
    "params": {"message": {"role": "user", "messageId": "m1", "contextId": "ctx-1",
      "parts": [{"kind": "text", "text": "Summarize the repository structure."}]}}
  }'

# 2. Or call Genkit's native agent HTTP endpoint (supports ?stream=true, detach, getSnapshot, abort)
curl -H "ate-target-actor: default/genkit-triage" -H "Content-Type: application/json" \
  http://localhost:8001/agents/triage_agent -d '{
    "data": {"message": {"role": "user", "content": [{"text": "What files did you inspect?"}]}},
    "init": {"sessionId": "ctx-1"}
  }'
```

## Workspace MCP and Skills

- **MCP servers**: Servers declared in the bound Workspace's `spec.mcp` are connected via `mcp.NewGenkitMCPClient` (`Stdio` for `command`/`args`, `StreamableHTTP` for `endpoint`) and registered as tools on the agent.
- **Skills**: When the Workspace sets `spec.skills.path`, the runner automatically configures `genkit-middleware/skills` for that path.

## Testing locally

To run the runner outside a cluster against local YAML files:

```bash
go build -o bin/genkit-runner ./cmd/runners/genkit-runner
GEMINI_API_KEY=... ./bin/genkit-runner --task-file task.yaml --workspace-file workspace.yaml --port 8080 --metadata-port 8099
```
