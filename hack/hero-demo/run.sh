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

# AX v0.1 hero demo harness.
#
# Usage: ./hack/hero-demo/run.sh [--step N] [--remote-context <kube-context>]
#
# Runs the five hero demo steps in order and prints a scoreboard
# (🟢 PASS / 🟡 STUB / 🔴 FAIL). By default it makes no calls to a cluster, so it
# runs cleanly anywhere. Set RUN_LIVE_STEPS=1 to run the steps against the active
# AX server (kube context, or ate-local when ATE_LOCAL=1 / AX_SERVER is set).
#
# Environment:
#   AX_BIN          ax CLI to use (default: .ax-local/bin/ax when ATE_LOCAL=1, ax on PATH, else ./bin/ax)
#   RUN_LIVE_STEPS  1 to call the AX server (default: 0)
#   ATE_LOCAL       1 to auto-start/target examples/ate_local/local-stack.sh multi-up (default: 0)
#   STARTER_IMAGE   Optional digest-pinned starter image replacing <starter-image>@sha256:<digest>
#   STUDIO_URL      AX Studio base URL (default: http://127.0.0.1:8080)
#   REMOTE_CONTEXT  kube context of a remote cluster for Step 5 (same as --remote-context)
#   AX_TIMEOUT      seconds allowed per ax call in live mode (default: 120)

set -uo pipefail

HERO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${HERO_DIR}/../.." && pwd)"

RUN_LIVE_STEPS="${RUN_LIVE_STEPS:-0}"
ATE_LOCAL="${ATE_LOCAL:-0}"
STUDIO_URL="${STUDIO_URL:-http://127.0.0.1:8080}"
REMOTE_CONTEXT="${REMOTE_CONTEXT:-}"
STARTER_IMAGE="${STARTER_IMAGE:-}"
AX_TIMEOUT="${AX_TIMEOUT:-120}"
ONLY_STEP=""

LOCAL_DIR="${AX_LOCAL_DIR:-${REPO_ROOT}/.ax-local}"
if [[ "${ATE_LOCAL}" == "1" && -f "${LOCAL_DIR}/env" ]]; then
  # shellcheck disable=SC1091
  source "${LOCAL_DIR}/env"
fi

if [[ -z "${AX_BIN:-}" ]]; then
  if [[ "${ATE_LOCAL}" == "1" && -x "${LOCAL_DIR}/bin/ax" ]]; then
    AX_BIN="${LOCAL_DIR}/bin/ax"
  elif command -v ax >/dev/null 2>&1; then
    AX_BIN="ax"
  else
    AX_BIN="${REPO_ROOT}/bin/ax"
  fi
fi

WORKSPACE_MANIFEST="${HERO_DIR}/workspace.yaml"
GATEWAY_MANIFEST="${HERO_DIR}/gateway.yaml"
STARTER_MANIFEST="${HERO_DIR}/task-starter.yaml"
CUSTOM_MANIFEST="${HERO_DIR}/task-custom.yaml"
WORKSPACE_NAME="hero-demo-ws"
GATEWAY_NAME="hero-demo-gw"
STARTER_TASK="hero-demo-starter-task"
CUSTOM_TASK="hero-demo-custom-task"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --step)
      ONLY_STEP="${2:-}"
      shift 2
      ;;
    --remote-context)
      REMOTE_CONTEXT="${2:-}"
      shift 2
      ;;
    -h | --help)
      awk '/^# AX v0.1 hero demo harness/{p=1} p&&!/^#/{exit} p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $1 (see --help)" >&2
      exit 2
      ;;
  esac
done
if [[ -n "${ONLY_STEP}" && ! "${ONLY_STEP}" =~ ^[1-5]$ ]]; then
  echo "--step takes a number from 1 to 5" >&2
  exit 2
fi

STEPS=(
  "1. Local Up & Studio"
  "2. Warm Workspace Snapshot"
  "3. Starter Task, Session & Resume"
  "4. Custom Image Task"
  "5. Remote Cluster Promotion"
)
STATUS=("" "" "" "" "")
NOTE=("" "" "" "" "")

mark_pass() { STATUS[$1]="🟢 PASS"; NOTE[$1]="$2"; }
mark_stub() { STATUS[$1]="🟡 STUB"; NOTE[$1]="$2"; }
mark_fail() { STATUS[$1]="🔴 FAIL"; NOTE[$1]="$2"; }

live() { [[ "${RUN_LIVE_STEPS}" == "1" ]]; }
is_ate_local() { [[ "${ATE_LOCAL}" == "1" || -n "${ATENET_ROUTER_ADDR:-}" ]]; }

have_ax() { [[ -x "${AX_BIN}" ]] || command -v "${AX_BIN}" >/dev/null 2>&1; }

run_ax() {
  if command -v timeout >/dev/null 2>&1; then
    timeout "${AX_TIMEOUT}" "${AX_BIN}" "$@"
  else
    command "${AX_BIN}" "$@"
  fi
}

ax_quiet() {
  local out
  if out="$(run_ax "$@" 2>&1)"; then
    return 0
  fi
  printf '    %s\n' "${out//$'\n'/$'\n'    }" >&2
  return 1
}

# TODO(@Lukeluds): Drop this once task-starter.yaml has the published image.
starter_image_is_placeholder() { grep -q '<starter-image>' "${STARTER_MANIFEST}"; }

render_starter_manifest() {
  local dst="$1"
  if [[ -n "${STARTER_IMAGE}" ]]; then
    sed "s|<starter-image>@sha256:<digest>|${STARTER_IMAGE}|g" "${STARTER_MANIFEST}" > "${dst}"
  else
    cp "${STARTER_MANIFEST}" "${dst}"
  fi
}

# ------------------------------------------------------------------------------
# Step 1: Local Up & AX Studio
# ------------------------------------------------------------------------------
step1_local_up() {
  local idx=0
  echo "==> [Step 1] Local Up & AX Studio"
  if [[ "${ATE_LOCAL}" == "1" ]] && live; then
    if ! "${REPO_ROOT}/examples/ate_local/local-stack.sh" multi-up >/dev/null; then
      mark_fail "${idx}" "examples/ate_local/local-stack.sh multi-up failed"
      return
    fi
    # shellcheck disable=SC1091
    source "${LOCAL_DIR}/env"
    AX_BIN="${LOCAL_DIR}/bin/ax"
  fi
  if ! have_ax; then
    mark_stub "${idx}" "ax CLI not found; run 'make install' or set AX_BIN"
    return
  fi
  if ! live; then
    if "${AX_BIN}" local --help >/dev/null 2>&1; then
      mark_stub "${idx}" "ax local start ready (or set RUN_LIVE_STEPS=1 ATE_LOCAL=1); wire Studio health check (#543)"
    else
      mark_stub "${idx}" "ax CLI found; set RUN_LIVE_STEPS=1 (and ATE_LOCAL=1 for local stack)"
    fi
    return
  fi
  if ! ax_quiet get workspaces; then
    mark_fail "${idx}" "ax-server not reachable"
    return
  fi
  if curl -fsS --max-time 5 "${STUDIO_URL}/healthz" >/dev/null 2>&1; then
    mark_pass "${idx}" "ax-server reachable and AX Studio healthy at ${STUDIO_URL}"
  elif is_ate_local; then
    mark_pass "${idx}" "ate-local stack reachable (${AX_SERVER:-localhost:8090}, router ${ATENET_ROUTER_ADDR:-127.0.0.1:50051})"
  else
    mark_stub "${idx}" "ax-server reachable; AX Studio not running at ${STUDIO_URL}"
  fi
}

# ------------------------------------------------------------------------------
# Step 2: Warm Workspace Snapshot
# ------------------------------------------------------------------------------
step2_warm_workspace() {
  local idx=1
  echo "==> [Step 2] Warm Workspace Snapshot"
  if ! live; then
    mark_stub "${idx}" "Set RUN_LIVE_STEPS=1 to apply workspace.yaml"
    return
  fi
  if ! ax_quiet apply -f "${WORKSPACE_MANIFEST}"; then
    mark_fail "${idx}" "ax apply -f workspace.yaml failed"
    return
  fi
  if is_ate_local; then
    mark_pass "${idx}" "Workspace ${WORKSPACE_NAME} applied (warm Tag auto-minted by ate-local on first attach)"
  else
    mark_stub "${idx}" "Workspace ${WORKSPACE_NAME} applied; wire the warm snapshot check"
  fi
}

# ------------------------------------------------------------------------------
# Step 3: Starter Task, Session & Resume
# ------------------------------------------------------------------------------
step3_starter_task_session() {
  local idx=2
  echo "==> [Step 3] Starter Task, Session & Resume"
  if ! live; then
    mark_stub "${idx}" "Set RUN_LIVE_STEPS=1 to run the starter task lifecycle"
    return
  fi
  # A task can only bind a Gateway that already exists.
  if ! ax_quiet apply -f "${GATEWAY_MANIFEST}"; then
    mark_fail "${idx}" "ax apply -f gateway.yaml failed"
    return
  fi
  if [[ -z "${STARTER_IMAGE}" ]] && starter_image_is_placeholder; then
    if is_ate_local; then
      STARTER_IMAGE="ghcr.io/ghchinoy/ax-images/genkit-runner@sha256:f5d7420468b7f1dec8304d7efa1dbe0ca2bb86a1e552181f823630412e01ee06"
    else
      mark_stub "${idx}" "Gateway ${GATEWAY_NAME} applied; the starter image is still a placeholder"
      return
    fi
  fi
  local rendered
  rendered="$(mktemp "${TMPDIR:-/tmp}/hero-starter-XXXXXX.yaml")"
  render_starter_manifest "${rendered}"
  # Tasks are immutable, so start from a fresh one on every run.
  run_ax delete task "${STARTER_TASK}" >/dev/null 2>&1 || true
  if ! ax_quiet apply -f "${rendered}"; then
    rm -f "${rendered}"
    mark_fail "${idx}" "ax apply -f task-starter.yaml failed"
    return
  fi
  rm -f "${rendered}"
  local verb
  for verb in resume suspend resume; do
    if ! ax_quiet "${verb}" task "${STARTER_TASK}"; then
      mark_fail "${idx}" "ax ${verb} task ${STARTER_TASK} failed"
      return
    fi
  done
  if is_ate_local; then
    mark_pass "${idx}" "Starter image (${STARTER_IMAGE##*/}) applied and survived resume -> suspend -> resume"
  else
    mark_stub "${idx}" "Apply + resume/suspend/resume OK; wire the second Session turn"
  fi
}

# ------------------------------------------------------------------------------
# Step 4: Custom Image Task Without Starter Template
# ------------------------------------------------------------------------------
step4_custom_image_task() {
  local idx=3
  echo "==> [Step 4] Custom Image Task - No Starter Template"
  if ! live || ! is_ate_local; then
    : "${CUSTOM_MANIFEST}" "${CUSTOM_TASK}"
    mark_stub "${idx}" "Wire custom image + command Task (needs runner injection, or set ATE_LOCAL=1)"
    return
  fi
  run_ax delete task "${CUSTOM_TASK}" >/dev/null 2>&1 || true
  if ! ax_quiet apply -f "${CUSTOM_MANIFEST}" || ! ax_quiet resume task "${CUSTOM_TASK}"; then
    mark_fail "${idx}" "Applying or resuming ${CUSTOM_TASK} failed"
    return
  fi
  local cname="ate-apple-default-${CUSTOM_TASK}"
  container inspect "${cname}" >/dev/null 2>&1 || cname="ate-default-${CUSTOM_TASK}"
  local logs=""
  for _ in $(seq 1 30); do
    logs="$(container logs "${cname}" 2>&1 || true)"
    if echo "${logs}" | grep -q "OK" && echo "${logs}" | grep -q "task command completed successfully"; then
      break
    fi
    sleep 0.3
  done
  if ! echo "${logs}" | grep -q "task command completed successfully"; then
    mark_fail "${idx}" "Custom python:3.12-slim unit test command did not complete cleanly"
    return
  fi
  ax_quiet suspend task "${CUSTOM_TASK}" || true
  mark_pass "${idx}" "Stock python:3.12-slim ran injected ax-task-runner on warm workspace and passed unittest"
}

# ------------------------------------------------------------------------------
# Step 5: Remote Cluster Promotion
# ------------------------------------------------------------------------------
step5_remote_promote() {
  local idx=4
  echo "==> [Step 5] Remote Cluster Promotion"
  if [[ -n "${REMOTE_CONTEXT}" ]]; then
    if ! have_ax; then
      mark_fail "${idx}" "ax CLI not found; run 'make install' or set AX_BIN"
      return
    fi
    if [[ -z "${STARTER_IMAGE}" ]] && starter_image_is_placeholder; then
      mark_stub "${idx}" "No starter task to promote to ${REMOTE_CONTEXT}; the starter image is still a placeholder"
      return
    fi
    # Copies the starter task that Step 3 runs, with the Gateway and Workspace it
    # binds, to the remote cluster and suspends it here. --replace lets a rerun
    # start from a fresh task there.
    if ax_quiet promote task "${STARTER_TASK}" --to "${REMOTE_CONTEXT}" --replace --suspend-source; then
      mark_pass "${idx}" "${STARTER_TASK} promoted to ${REMOTE_CONTEXT}"
    else
      mark_fail "${idx}" "ax promote task ${STARTER_TASK} --to ${REMOTE_CONTEXT} failed"
    fi
    return
  fi

  if live && is_ate_local && [[ -n "${AX_SERVER_B:-}" ]]; then
    ATENET_ROUTER_ADDR="${ATENET_ROUTER_ADDR_B:-127.0.0.1:50052}" run_ax --server "${AX_SERVER_B}" delete task "${CUSTOM_TASK}" >/dev/null 2>&1 || true
    if ATENET_ROUTER_ADDR="${ATENET_ROUTER_ADDR_B:-127.0.0.1:50052}" ax_quiet --server "${AX_SERVER_B}" apply -f "${WORKSPACE_MANIFEST}" &&
      ATENET_ROUTER_ADDR="${ATENET_ROUTER_ADDR_B:-127.0.0.1:50052}" ax_quiet --server "${AX_SERVER_B}" apply -f "${CUSTOM_MANIFEST}" &&
      ATENET_ROUTER_ADDR="${ATENET_ROUTER_ADDR_B:-127.0.0.1:50052}" ax_quiet --server "${AX_SERVER_B}" resume task "${CUSTOM_TASK}"; then
      mark_pass "${idx}" "Promoted workspace.yaml & task-custom.yaml unchanged to Cluster B (${AX_SERVER_B})"
      return
    fi
    mark_fail "${idx}" "Promoting manifests to Cluster B (${AX_SERVER_B}) failed"
    return
  fi

  mark_stub "${idx}" "Set REMOTE_CONTEXT or --remote-context (or ATE_LOCAL=1 multi-up) to promote"
}

FUNCS=(step1_local_up step2_warm_workspace step3_starter_task_session step4_custom_image_task step5_remote_promote)
for i in "${!FUNCS[@]}"; do
  if [[ -n "${ONLY_STEP}" && "${ONLY_STEP}" != "$((i + 1))" ]]; then
    continue
  fi
  "${FUNCS[$i]}"
done

echo ""
echo "================================================================================"
echo "  AX v0.1 HERO DEMO SCOREBOARD$([[ "${RUN_LIVE_STEPS}" == "1" ]] && echo " (live)")"
echo "================================================================================"
for i in "${!STEPS[@]}"; do
  if [[ -n "${ONLY_STEP}" && "${ONLY_STEP}" != "$((i + 1))" ]]; then
    continue
  fi
  printf "  %-34s | %-7s | %s\n" "${STEPS[$i]}" "${STATUS[$i]}" "${NOTE[$i]}"
done
echo "================================================================================"
