#!/usr/bin/env bash
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

# local-stack.sh runs the full AX stack on macOS without Kubernetes:
#
#   ax CLI -> ax-server -> ate-local -> Apple container
#                \-> PostgreSQL (resource store, locks)
#
# Components are started in dependency order, and each one must accept
# connections before the next starts. This keeps ax-server from starting
# before ate-local or PostgreSQL and sitting in gRPC reconnect backoff.
#
# Usage: examples/ate_local/local-stack.sh {up|down|status|logs [component]|reset|multi-up|multi-down|migrate <task> [atespace]}
#
# Ports, runtimes, and names can be overridden with environment variables:
#   AX_LOCAL_RUNTIME            (default apple; or podman)
#   AX_LOCAL_CLUSTER_B_RUNTIME  (default podman if installed, else apple)
#   AX_LOCAL_POSTGRES_PORT      (default 5432)
#   AX_LOCAL_SUBSTRATE_PORT     (default 50051)
#   AX_LOCAL_SERVER_PORT        (default 8090; 8080 is avoided because it is a common default)
#   AX_LOCAL_SUBSTRATE_B_PORT   (default 50052; used by multi-up for Cluster B)
#   AX_LOCAL_SERVER_B_PORT      (default 8091; used by multi-up for Cluster B)
#   AX_LOCAL_POSTGRES_NAME      (default ax-local-postgres)
#   AX_LOCAL_POSTGRES_IMAGE     (default postgres:17-alpine)
#   AX_LOCAL_DIR                (default ./.ax-local)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${REPO_ROOT}"

RUNTIME="${AX_LOCAL_RUNTIME:-apple}"
if [[ -n "${AX_LOCAL_CLUSTER_B_RUNTIME:-}" ]]; then
  CLUSTER_B_RUNTIME="${AX_LOCAL_CLUSTER_B_RUNTIME}"
elif command -v podman >/dev/null 2>&1; then
  CLUSTER_B_RUNTIME="podman"
else
  CLUSTER_B_RUNTIME="apple"
fi
POSTGRES_PORT="${AX_LOCAL_POSTGRES_PORT:-5432}"
SUBSTRATE_PORT="${AX_LOCAL_SUBSTRATE_PORT:-50051}"
SERVER_PORT="${AX_LOCAL_SERVER_PORT:-8090}"
SUBSTRATE_B_PORT="${AX_LOCAL_SUBSTRATE_B_PORT:-50052}"
SERVER_B_PORT="${AX_LOCAL_SERVER_B_PORT:-8091}"
POSTGRES_NAME="${AX_LOCAL_POSTGRES_NAME:-ax-local-postgres}"
POSTGRES_IMAGE="${AX_LOCAL_POSTGRES_IMAGE:-postgres:17-alpine}"
STATE_DIR="${AX_LOCAL_DIR:-${REPO_ROOT}/.ax-local}"
BIN_DIR="${STATE_DIR}/bin"
PID_DIR="${STATE_DIR}/pids"
LOG_DIR="${STATE_DIR}/logs"
SNAPSHOT_DIR="${STATE_DIR}/snapshots"
CONFIG_FILE="${STATE_DIR}/ate-local.yaml"
CONFIG_B_FILE="${STATE_DIR}/ate-local-b.yaml"
ENV_FILE="${STATE_DIR}/env"
HOST="127.0.0.1"
WAIT_SECONDS=30

# Processes this script manages, in start order.
COMPONENTS=(ate-local ax-server ate-local-b ax-server-b)

info() { printf '==> %s\n' "$*"; }
warn() { printf 'warning: %s\n' "$*" >&2; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

usage() {
  sed -n '17,32p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
  exit 1
}

# port_owner prints "command (pid N)" for the process listening on a TCP port.
port_owner() {
  local pid
  pid="$(lsof -nP -t -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | head -n1 || true)"
  [[ -z "${pid}" ]] && return 1
  printf '%s (pid %s)' "$(ps -p "${pid}" -o comm= 2>/dev/null | xargs basename 2>/dev/null)" "${pid}"
}

port_open() { nc -z "${HOST}" "$1" >/dev/null 2>&1; }

wait_for_port() {
  local name="$1" port="$2" log="${3:-}"
  local deadline=$((SECONDS + WAIT_SECONDS))
  while ! port_open "${port}"; do
    if (( SECONDS >= deadline )); then
      warn "${name} did not accept connections on ${HOST}:${port} within ${WAIT_SECONDS}s"
      [[ -n "${log}" && -f "${log}" ]] && tail -n 20 "${log}" >&2
      return 1
    fi
    sleep 0.2
  done
}

pid_of() {
  local file="${PID_DIR}/$1.pid"
  [[ -f "${file}" ]] && cat "${file}"
}

alive() {
  local pid
  pid="$(pid_of "$1")" || return 1
  [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null
}

postgres_running() {
  container list 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "${POSTGRES_NAME}"
}

postgres_exists() {
  container list --all 2>/dev/null | awk 'NR>1 {print $1}' | grep -qx "${POSTGRES_NAME}"
}

check_prereqs() {
  [[ "$(uname -s)" == "Darwin" ]] || die "local-stack.sh requires macOS (Apple container)"
  command -v container >/dev/null || die "Apple 'container' CLI not found in PATH"
  command -v go >/dev/null || die "go not found in PATH"
  command -v nc >/dev/null || die "nc not found in PATH"
  container list >/dev/null 2>&1 || die "Apple container is not responding; try 'container system start'"
}

# check_port fails if a port is held by something this script did not start.
check_port() {
  local name="$1" port="$2" ours="$3"
  local owner
  if owner="$(port_owner "${port}")"; then
    if [[ "${ours}" == "yes" ]]; then
      return 0
    fi
    die "port ${port} (needed by ${name}) is in use by ${owner}. Stop it or set the matching AX_LOCAL_*_PORT variable."
  fi
}

build() {
  info "Building binaries into ${BIN_DIR}"
  mkdir -p "${BIN_DIR}/linux_arm64"
  go build -o "${BIN_DIR}/ate-local" ./cmd/ate-local
  go build -o "${BIN_DIR}/ax-server" ./cmd/ax-server
  go build -o "${BIN_DIR}/ax" ./cmd/ax
  # Apple container runs arm64 Linux guests; the published ax-task-runner image is amd64.
  GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
    go build -trimpath -ldflags="-s -w" -o "${BIN_DIR}/linux_arm64/ax-task-runner" ./cmd/ax-task-runner
}

write_cluster_config() {
  local file="$1" port="$2" runtime="$3" prefix="$4"
  cat >"${file}" <<EOF
# Generated by local-stack.sh; edits are overwritten by 'up' / 'multi-up'.
address: "${HOST}:${port}"
runtime: "${runtime}"
container_prefix: "${prefix}"
snapshot_dir: "${SNAPSHOT_DIR}"
ready_timeout: "20s"
templates:
  # ate-local copies this template's volumes into each custom template,
  # so every Task gets the arm64 runner.
  default:
    image: "debian:12"
    port: 80
    volumes:
      - "${BIN_DIR}/linux_arm64/ax-task-runner:/usr/local/bin/ax-task-runner"
    cpus: 2
    memory: "1024M"
EOF
}

write_config() {
  write_cluster_config "${CONFIG_FILE}" "${SUBSTRATE_PORT}" "${RUNTIME}" "ate"
}

write_env() {
  cat >"${ENV_FILE}" <<EOF
# source this file to point the ax CLI at the local stack
export AX_SERVER=localhost:${SERVER_PORT}
export AX_SERVER_A=localhost:${SERVER_PORT}
export AX_SERVER_B=localhost:${SERVER_B_PORT}
export PATH="${BIN_DIR}:\${PATH}"
EOF
}

start_postgres() {
  if postgres_running; then
    info "PostgreSQL already running (${POSTGRES_NAME})"
  else
    check_port "PostgreSQL" "${POSTGRES_PORT}" no
    if postgres_exists; then
      info "Starting existing PostgreSQL container ${POSTGRES_NAME}"
      container start "${POSTGRES_NAME}" >/dev/null
    else
      info "Starting PostgreSQL (${POSTGRES_IMAGE}) in Apple container"
      container run -d --progress none --name "${POSTGRES_NAME}" \
        -e POSTGRES_USER=ax -e POSTGRES_PASSWORD=ax -e POSTGRES_DB=ax \
        -p "${HOST}:${POSTGRES_PORT}:5432" --memory 512M "${POSTGRES_IMAGE}" >/dev/null
    fi
  fi
  wait_for_port "PostgreSQL" "${POSTGRES_PORT}"
  local deadline=$((SECONDS + WAIT_SECONDS))
  while ! container exec "${POSTGRES_NAME}" pg_isready -U ax -d ax >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      die "PostgreSQL did not become ready within ${WAIT_SECONDS}s"
    fi
    sleep 0.2
  done
}

start_component() {
  local name="$1" port="$2"
  shift 2
  local log="${LOG_DIR}/${name}.log"
  if alive "${name}"; then
    info "${name} already running (pid $(pid_of "${name}"))"
    return 0
  fi
  check_port "${name}" "${port}" no
  info "Starting ${name} on ${HOST}:${port}"
  nohup "$@" >"${log}" 2>&1 &
  echo $! >"${PID_DIR}/${name}.pid"
  if ! wait_for_port "${name}" "${port}" "${log}"; then
    stop_component "${name}"
    die "${name} failed to start; see ${log}"
  fi
}

stop_component() {
  local name="$1" pid
  pid="$(pid_of "${name}")" || { rm -f "${PID_DIR}/${name}.pid"; return 0; }
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    info "Stopping ${name} (pid ${pid})"
    kill -TERM "${pid}" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "${pid}" 2>/dev/null || break
      sleep 0.2
    done
    if kill -0 "${pid}" 2>/dev/null; then
      warn "${name} did not exit after SIGTERM; sending SIGKILL"
      kill -KILL "${pid}" 2>/dev/null || true
    fi
  fi
  rm -f "${PID_DIR}/${name}.pid"
}

cmd_up() {
  check_prereqs
  mkdir -p "${PID_DIR}" "${LOG_DIR}" "${SNAPSHOT_DIR}"

  # Fail before building anything if a port is taken by something else.
  postgres_running || check_port "PostgreSQL" "${POSTGRES_PORT}" no
  alive ate-local || check_port "ate-local" "${SUBSTRATE_PORT}" no
  alive ax-server || check_port "ax-server" "${SERVER_PORT}" no

  build
  write_config
  write_env

  start_postgres
  start_component ate-local "${SUBSTRATE_PORT}" \
    "${BIN_DIR}/ate-local" --config "${CONFIG_FILE}"
  start_component ax-server "${SERVER_PORT}" \
    "${BIN_DIR}/ax-server" \
    --addr "${HOST}:${SERVER_PORT}" \
    --store "postgres://ax:ax@${HOST}:${POSTGRES_PORT}/ax?sslmode=disable" \
    --substrate-endpoint "${HOST}:${SUBSTRATE_PORT}" \
    --substrate-plaintext \
    --template default \
    --template-atespace default

  echo
  cmd_status
  cat <<EOF

The local AX stack is up. Point the ax CLI at it:

  source ${ENV_FILE#"${REPO_ROOT}/"}

Then try:

  ax apply -f examples/ate_local/task-local.yaml
  ax resume local-task
  ax describe task local-task
  ax ssh local-task -- uname -sm
  ax delete task local-task

Logs: examples/ate_local/local-stack.sh logs [component]
Stop: examples/ate_local/local-stack.sh down
EOF
  if owner="$(port_owner 8080)"; then
    [[ "${SERVER_PORT}" != "8080" ]] && warn "port 8080 is used by ${owner}. Without AX_SERVER set, ax connects to 8080 by default."
  fi
}

cmd_multi_up() {
  check_prereqs
  if [[ "${CLUSTER_B_RUNTIME}" == "podman" ]]; then
    command -v podman >/dev/null 2>&1 || die "podman not found in PATH for Cluster B (install podman or set AX_LOCAL_CLUSTER_B_RUNTIME=apple)"
    podman info >/dev/null 2>&1 || die "podman is not responding; try 'podman machine start'"
  fi
  mkdir -p "${PID_DIR}" "${LOG_DIR}" "${SNAPSHOT_DIR}"

  postgres_running || check_port "PostgreSQL" "${POSTGRES_PORT}" no
  alive ate-local || check_port "ate-local (Cluster A)" "${SUBSTRATE_PORT}" no
  alive ax-server || check_port "ax-server (Cluster A)" "${SERVER_PORT}" no
  alive ate-local-b || check_port "ate-local-b (Cluster B)" "${SUBSTRATE_B_PORT}" no
  alive ax-server-b || check_port "ax-server-b (Cluster B)" "${SERVER_B_PORT}" no

  local cluster_b_prefix="ate-${CLUSTER_B_RUNTIME}"
  if [[ "${CLUSTER_B_RUNTIME}" == "apple" ]]; then
    cluster_b_prefix="ate-cluster-b"
  fi
  build
  write_cluster_config "${CONFIG_FILE}" "${SUBSTRATE_PORT}" "apple" "ate-apple"
  write_cluster_config "${CONFIG_B_FILE}" "${SUBSTRATE_B_PORT}" "${CLUSTER_B_RUNTIME}" "${cluster_b_prefix}"
  write_env

  start_postgres
  container exec "${POSTGRES_NAME}" psql -U ax -d ax -tc "SELECT 1 FROM pg_database WHERE datname = 'ax_cluster_b'" | grep -q 1 || \
    container exec "${POSTGRES_NAME}" psql -U ax -d ax -c "CREATE DATABASE ax_cluster_b" >/dev/null

  start_component ate-local "${SUBSTRATE_PORT}" \
    "${BIN_DIR}/ate-local" --config "${CONFIG_FILE}"
  start_component ax-server "${SERVER_PORT}" \
    "${BIN_DIR}/ax-server" \
    --addr "${HOST}:${SERVER_PORT}" \
    --store "postgres://ax:ax@${HOST}:${POSTGRES_PORT}/ax?sslmode=disable" \
    --substrate-endpoint "${HOST}:${SUBSTRATE_PORT}" \
    --substrate-plaintext \
    --template default \
    --template-atespace default

  start_component ate-local-b "${SUBSTRATE_B_PORT}" \
    "${BIN_DIR}/ate-local" --config "${CONFIG_B_FILE}"
  start_component ax-server-b "${SERVER_B_PORT}" \
    "${BIN_DIR}/ax-server" \
    --addr "${HOST}:${SERVER_B_PORT}" \
    --store "postgres://ax:ax@${HOST}:${POSTGRES_PORT}/ax_cluster_b?sslmode=disable" \
    --substrate-endpoint "${HOST}:${SUBSTRATE_B_PORT}" \
    --substrate-plaintext \
    --template default \
    --template-atespace default

  echo
  cmd_status
  cat <<EOF

Dual-cluster AX stack is up:
  Cluster A (apple):               AX_SERVER_A=localhost:${SERVER_PORT}  -> ate-local (${HOST}:${SUBSTRATE_PORT})
  Cluster B (${CLUSTER_B_RUNTIME}):               AX_SERVER_B=localhost:${SERVER_B_PORT}  -> ate-local-b (${HOST}:${SUBSTRATE_B_PORT})
  Shared /workspace snapshot dir:  ${SNAPSHOT_DIR#"${REPO_ROOT}/"}

Try migrating a task across clusters:
  source ${ENV_FILE#"${REPO_ROOT}/"}
  ax --server \$AX_SERVER_A apply -f examples/ate_local/task-local.yaml
  ax --server \$AX_SERVER_A resume local-task
  ax --server \$AX_SERVER_A ssh local-task -- sh -c 'echo "written on cluster A" > /workspace/progress.txt'
  examples/ate_local/local-stack.sh migrate local-task
  ax --server \$AX_SERVER_B ssh local-task -- cat /workspace/progress.txt
EOF
}

cmd_migrate() {
  local task_name="${1:-}"
  local atespace="${2:-default}"
  [[ -n "${task_name}" ]] || die "usage: local-stack.sh migrate <task-name> [atespace]"
  local ax_bin="${BIN_DIR}/ax"
  [[ -x "${ax_bin}" ]] || ax_bin="ax"

  info "1/3 Suspending task '${task_name}' (atespace '${atespace}') on Cluster A (localhost:${SERVER_PORT}) to snapshot /workspace..."
  "${ax_bin}" --server "localhost:${SERVER_PORT}" -n "${atespace}" suspend "${task_name}"

  info "2/3 Exporting task '${task_name}' from Cluster A and applying to Cluster B (localhost:${SERVER_B_PORT})..."
  "${ax_bin}" --server "localhost:${SERVER_PORT}" -n "${atespace}" get task "${task_name}" -o yaml |
    awk '/^status:/ { exit } { print }' |
    "${ax_bin}" --server "localhost:${SERVER_B_PORT}" -n "${atespace}" apply -f -

  info "3/3 Resuming task '${task_name}' on Cluster B (localhost:${SERVER_B_PORT}) and hydrating /workspace snapshot..."
  "${ax_bin}" --server "localhost:${SERVER_B_PORT}" -n "${atespace}" resume "${task_name}"
  "${ax_bin}" --server "localhost:${SERVER_B_PORT}" -n "${atespace}" describe task "${task_name}"
}

cmd_down() {
  local keep_postgres=no
  [[ "${1:-}" == "--keep-postgres" || "${1:-}" == "--keep-db" ]] && keep_postgres=yes
  # Stop in reverse dependency order.
  stop_component ax-server-b
  stop_component ate-local-b
  stop_component ax-server
  stop_component ate-local
  if [[ "${keep_postgres}" == "no" ]] && postgres_exists; then
    info "Removing PostgreSQL container ${POSTGRES_NAME}"
    container delete --force "${POSTGRES_NAME}" >/dev/null 2>&1 || true
  fi
  # ate-local stops actor containers on shutdown; report any that remain.
  local leftovers
  leftovers="$(container list --all 2>/dev/null | awk 'NR>1 && $1 ~ /^ate-/ {print $1}' || true)"
  if [[ -n "${leftovers}" ]]; then
    warn "actor containers remain (remove with 'reset' or 'container delete --force'):"
    printf '  %s\n' ${leftovers} >&2
  fi
}

status_line() {
  local name="$1" port="$2" state="stopped" pid="-"
  if [[ "${name}" == "postgres" ]]; then
    postgres_running && state="running" && pid="container"
  elif alive "${name}"; then
    state="running"
    pid="$(pid_of "${name}")"
  fi
  local reach="-"
  if [[ "${port}" != "-" ]]; then
    port_open "${port}" && reach="yes" || reach="no"
  fi
  printf '%-14s %-10s %-10s %-8s %s\n' "${name}" "${state}" "${pid}" "${port}" "${reach}"
}

cmd_status() {
  printf '%-14s %-10s %-10s %-8s %s\n' COMPONENT STATE PID PORT REACHABLE
  status_line postgres "${POSTGRES_PORT}"
  status_line ate-local "${SUBSTRATE_PORT}"
  status_line ax-server "${SERVER_PORT}"
  if alive ate-local-b || alive ax-server-b; then
    status_line ate-local-b "${SUBSTRATE_B_PORT}"
    status_line ax-server-b "${SERVER_B_PORT}"
  fi
  local actors podman_actors
  actors="$(container list --all 2>/dev/null | awk 'NR>1 && $1 ~ /^ate-/ {print "  [apple]  " $1 "  " $5 "  " $6}' || true)"
  if command -v podman >/dev/null 2>&1; then
    podman_actors="$(podman ps -a --format '{{.Names}}  {{.Status}}  {{.Ports}}' 2>/dev/null | awk '$1 ~ /^ate-/ {print "  [podman] " $0}' || true)"
  else
    podman_actors=""
  fi
  echo
  if [[ -n "${actors}" || -n "${podman_actors}" ]]; then
    echo "Actor containers:"
    [[ -n "${actors}" ]] && echo "${actors}"
    [[ -n "${podman_actors}" ]] && echo "${podman_actors}"
  else
    echo "Actor containers: none"
  fi
  if [[ "${AX_SERVER:-}" != "localhost:${SERVER_PORT}" ]]; then
    echo
    echo "AX_SERVER is '${AX_SERVER:-<unset>}' in this shell; run: source ${ENV_FILE#"${REPO_ROOT}/"}"
  fi
}

cmd_logs() {
  local name="${1:-}"
  if [[ -z "${name}" ]]; then
    tail -n 50 -f "${LOG_DIR}"/*.log
  elif [[ "${name}" == "postgres" ]]; then
    container logs --follow "${POSTGRES_NAME}"
  else
    [[ -f "${LOG_DIR}/${name}.log" ]] || die "no log for '${name}' (components: ${COMPONENTS[*]} postgres)"
    tail -n 50 -f "${LOG_DIR}/${name}.log"
  fi
}

cmd_reset() {
  cmd_down
  local actors
  actors="$(container list --all 2>/dev/null | awk 'NR>1 && $1 ~ /^ate-/ {print $1}' || true)"
  for a in ${actors}; do
    info "Removing actor container ${a}"
    container delete --force "${a}" >/dev/null 2>&1 || true
  done
  if command -v podman >/dev/null 2>&1; then
    local podman_actors
    podman_actors="$(podman ps -a --format '{{.Names}}' 2>/dev/null | awk '$1 ~ /^ate-/ {print $1}' || true)"
    for a in ${podman_actors}; do
      info "Removing podman actor container ${a}"
      podman rm --force "${a}" >/dev/null 2>&1 || true
    done
  fi
  cmd_up
}

case "${1:-}" in
  up) cmd_up ;;
  down) shift; cmd_down "$@" ;;
  multi-up) cmd_multi_up ;;
  multi-down) shift; cmd_down "$@" ;;
  migrate) shift; cmd_migrate "$@" ;;
  status) cmd_status ;;
  logs) shift; cmd_logs "$@" ;;
  reset) cmd_reset ;;
  *) usage ;;
esac
