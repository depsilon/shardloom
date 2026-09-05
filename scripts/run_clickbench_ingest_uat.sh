#!/usr/bin/env bash
set -euo pipefail

# Local engineering harness for ClickBench ingest UAT. This runs the ShardLoom
# CLI directly and redirects stdout/stderr to files so the process cannot block
# on an undrained JSON pipe. It is not an official benchmark runner.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
uat_root="${SHARDLOOM_CLICKBENCH_UAT_ROOT:-$HOME/LocalData/shardloom/clickbench-100m-uat}"
binary="${SHARDLOOM_BIN:-}"
source_path=""
input_format="parquet"
target_path=""
memory_gb="${SHARDLOOM_MEMORY_GB:-24}"
max_parallelism="${SHARDLOOM_MAX_PARALLELISM:-2}"
replace_existing="false"
progress_interval_seconds="${SHARDLOOM_CLICKBENCH_UAT_PROGRESS_INTERVAL_SECONDS:-30}"
max_runtime_seconds="${SHARDLOOM_CLICKBENCH_UAT_MAX_RUNTIME_SECONDS:-1800}"
max_artifact_gb="${SHARDLOOM_CLICKBENCH_UAT_MAX_ARTIFACT_GB:-64}"
stable_artifact_seconds="${SHARDLOOM_CLICKBENCH_UAT_STABLE_ARTIFACT_SECONDS:-120}"
stable_artifact_min_gb="${SHARDLOOM_CLICKBENCH_UAT_STABLE_ARTIFACT_MIN_GB:-25}"
idle_cpu_percent="${SHARDLOOM_CLICKBENCH_UAT_IDLE_CPU_PERCENT:-1}"
min_progress_seconds="${SHARDLOOM_CLICKBENCH_UAT_MIN_PROGRESS_SECONDS:-360}"
min_progress_gb="${SHARDLOOM_CLICKBENCH_UAT_MIN_PROGRESS_GB:-1}"
source_residency_check="${SHARDLOOM_CLICKBENCH_UAT_SOURCE_RESIDENCY_CHECK:-true}"
source_residency_min_gb="${SHARDLOOM_CLICKBENCH_UAT_SOURCE_RESIDENCY_MIN_GB:-1}"
min_free_gib="${SHARDLOOM_CLICKBENCH_UAT_MIN_FREE_GIB:-12}"
max_workspace_gib="${SHARDLOOM_CLICKBENCH_UAT_MAX_WORKSPACE_GIB:-100}"
max_log_mib="${SHARDLOOM_CLICKBENCH_UAT_MAX_LOG_MIB:-256}"

usage() {
  cat <<'USAGE'
usage: scripts/run_clickbench_ingest_uat.sh [options]

Options:
  --uat-root PATH
  --binary PATH
  --source PATH
  --input-format FORMAT
  --target PATH
  --memory-gb N
  --max-parallelism N
  --replace-existing
  --progress-interval-seconds N
  --max-runtime-seconds N
  --max-artifact-gb N
  --min-free-gib N          Keep this much disk headroom (default 12)
  --max-workspace-gib N     Bound retained UAT files across runs (default 100)
  --max-log-mib N           Bound retained logs across runs (default 256)
  --stable-artifact-seconds N
  --stable-artifact-min-gb N
  --idle-cpu-percent N
  --min-progress-seconds N
  --min-progress-gb N
  --skip-source-residency-check
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uat-root) uat_root="$2"; shift 2 ;;
    --binary) binary="$2"; shift 2 ;;
    --source) source_path="$2"; shift 2 ;;
    --input-format) input_format="$2"; shift 2 ;;
    --target) target_path="$2"; shift 2 ;;
    --memory-gb) memory_gb="$2"; shift 2 ;;
    --max-parallelism) max_parallelism="$2"; shift 2 ;;
    --replace-existing) replace_existing="true"; shift ;;
    --progress-interval-seconds) progress_interval_seconds="$2"; shift 2 ;;
    --max-runtime-seconds) max_runtime_seconds="$2"; shift 2 ;;
    --max-artifact-gb) max_artifact_gb="$2"; shift 2 ;;
    --min-free-gib) min_free_gib="$2"; shift 2 ;;
    --max-workspace-gib) max_workspace_gib="$2"; shift 2 ;;
    --max-log-mib) max_log_mib="$2"; shift 2 ;;
    --stable-artifact-seconds) stable_artifact_seconds="$2"; shift 2 ;;
    --stable-artifact-min-gb) stable_artifact_min_gb="$2"; shift 2 ;;
    --idle-cpu-percent) idle_cpu_percent="$2"; shift 2 ;;
    --min-progress-seconds) min_progress_seconds="$2"; shift 2 ;;
    --min-progress-gb) min_progress_gb="$2"; shift 2 ;;
    --skip-source-residency-check) source_residency_check="false"; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

source_path="${source_path:-$uat_root/sources/hits.parquet}"
target_path="${target_path:-$uat_root/vortex/hits-parquet-100m.vortex}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="$uat_root/logs/ingest_cli_uat_gated_$timestamp"
stdout_path="$log_dir/stdout.json"
stderr_path="$log_dir/stderr.txt"
progress_path="$log_dir/progress.jsonl"
summary_path="$log_dir/prepare_summary.json"
cmd_path="$log_dir/prepare.cmd.txt"
native_timing_path="$log_dir/native_timing.json"
native_pid_path="$log_dir/native.pid"
# Refuse unsafe destinations before creating directories or replacing anything.
storage_guard=(python3 "$repo_root/scripts/local_uat_storage.py"
  --root "$uat_root" --source "$source_path" --target "$target_path" --logs "$log_dir"
  --min-free-gib "$min_free_gib" --max-workspace-gib "$max_workspace_gib"
  --max-log-mib "$max_log_mib")
"${storage_guard[@]}" --paths-only
"${storage_guard[@]}" --reserve-gib "$max_artifact_gb"

if [[ -z "$binary" ]]; then
  cargo_target="$(cd "$repo_root" && cargo metadata --offline --no-deps --format-version 1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])')"
  binary="$cargo_target/release/shardloom"
fi
if [[ ! -x "$binary" ]]; then
  echo "ShardLoom binary is not executable: $binary" >&2
  exit 66
fi
mkdir -p "$uat_root"
lock_dir="$uat_root/.ingest-uat.lock"
if ! mkdir "$lock_dir" 2>/dev/null; then
  echo "UAT workspace is locked: $lock_dir; another run may be active. Inspect a stale lock before removing it." >&2
  exit 75
fi
pid=""
stop_child() {
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    for _ in {1..50}; do
      if ! kill -0 "$pid" 2>/dev/null; then
        return
      fi
      sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
  fi
}
cleanup_run() {
  stop_child
  rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup_run EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$log_dir" "$(dirname "$target_path")"

target_dir="$(dirname "$target_path")"
target_name="$(basename "$target_path")"

cmd=(
  "$binary" prepare dataframe
  --input "$source_path"
  --input-format "$input_format"
  --output "$target_path"
  --memory-gb "$memory_gb"
  --max-parallelism "$max_parallelism"
  --format json
)
if [[ "$replace_existing" == "true" ]]; then
  cmd+=(--allow-overwrite)
fi

printf '%q ' "${cmd[@]}" > "$cmd_path"
printf '\n' >> "$cmd_path"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

file_size_bytes() {
  if [[ -e "$1" ]]; then
    local size
    if size="$(stat -f '%z' "$1" 2>/dev/null)" && [[ "$size" =~ ^[0-9]+$ ]]; then
      printf '%s' "$size"
    else
      stat -c '%s' "$1"
    fi
  else
    printf '0'
  fi
}

file_allocated_bytes() {
  if [[ ! -e "$1" ]]; then
    printf '0\n'
    return 0
  fi
  local allocated_kb
  allocated_kb="$(du -sk "$1" | awk '{print $1}')"
  printf '%s\n' "$((allocated_kb * 1024))"
}

preflight_source_residency() {
  if [[ "$source_residency_check" != "true" ]]; then
    return 0
  fi
  if [[ ! -f "$source_path" ]]; then
    echo "source path does not exist or is not a file: $source_path" >&2
    return 66
  fi
  local logical_bytes allocated_bytes min_bytes
  logical_bytes="$(file_size_bytes "$source_path")"
  allocated_bytes="$(file_allocated_bytes "$source_path")"
  min_bytes="$((source_residency_min_gb * 1024 * 1024 * 1024))"
  if (( logical_bytes >= min_bytes && allocated_bytes < min_bytes )); then
    cat >&2 <<EOF
source file appears sparse or not locally resident:
  path: $source_path
  logical_bytes: $logical_bytes
  allocated_bytes: $allocated_bytes
Download or materialize the official source before running ingest UAT, or pass --skip-source-residency-check if this is intentional.
EOF
    return 78
  fi
}

preflight_status=0
preflight_source_residency || preflight_status="$?"
if [[ "$preflight_status" -ne 0 ]]; then
  source_bytes="$(file_size_bytes "$source_path")"
  source_allocated_bytes="$(file_allocated_bytes "$source_path")"
  target_exists="false"
  if [[ -e "$target_path" ]]; then
    target_exists="true"
  fi
  target_bytes="$(file_size_bytes "$target_path")"
  cat > "$summary_path" <<JSON
{
  "schema_version": "shardloom.clickbench.ingest_cli_uat_gated.v1",
  "claim_boundary": "local CLI replacement-ingest UAT only; no official benchmark claim",
  "created_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "command_file": "$(json_escape "$cmd_path")",
  "stdout_path": "$(json_escape "$stdout_path")",
  "stderr_path": "$(json_escape "$stderr_path")",
  "progress_path": "$(json_escape "$progress_path")",
  "log_dir": "$(json_escape "$log_dir")",
  "returncode": $preflight_status,
  "stop_reason": "source_residency_preflight_failed",
  "elapsed_seconds": 0,
  "source": "$(json_escape "$source_path")",
  "source_bytes": $source_bytes,
  "source_allocated_bytes": $source_allocated_bytes,
  "target": "$(json_escape "$target_path")",
  "target_exists": $target_exists,
  "target_bytes": $target_bytes,
  "memory_gb": $memory_gb,
  "max_parallelism": $max_parallelism,
  "max_runtime_seconds": $max_runtime_seconds,
  "max_artifact_gb": $max_artifact_gb,
  "stable_artifact_seconds": $stable_artifact_seconds,
  "stable_artifact_min_gb": $stable_artifact_min_gb,
  "idle_cpu_percent": $idle_cpu_percent,
  "min_progress_seconds": $min_progress_seconds,
  "min_progress_gb": $min_progress_gb,
  "progress_sample_count": 0,
  "stdout_json_ok": false
}
JSON
  printf 'SUMMARY '
  cat "$summary_path"
  exit "$preflight_status"
fi

if [[ "$replace_existing" == "true" ]]; then
  # Replacement authorizes only this exact output, not backups, source files,
  # similarly named files, or staging files whose ownership is unknown.
  rm -f -- "$target_path"
fi

candidate_bytes() {
  local total=0
  local max=0
  local count=0
  local path size
  for path in "$target_path" "$target_dir/.$target_name.shardloom-tmp-"*; do
    if [[ ! -f "$path" || -L "$path" ]]; then
      continue
    fi
    size="$(file_size_bytes "$path")"
    total=$((total + size))
    if (( size > max )); then
      max="$size"
    fi
    count=$((count + 1))
  done
  printf '%s %s %s\n' "$count" "$total" "$max"
}

gb_rounded() {
  awk -v bytes="$1" 'BEGIN { printf "%.3f", bytes / 1000000000 }'
}

cpu_percent() {
  ps -o pcpu= -p "$1" 2>/dev/null | awk 'NF { print $1; exit }'
}

float_ge() {
  awk -v a="$1" -v b="$2" 'BEGIN { exit !(a >= b) }'
}

float_le() {
  awk -v a="$1" -v b="$2" 'BEGIN { exit !(a <= b) }'
}

elapsed_seconds() {
  local now
  now="$(date +%s)"
  echo $((now - start_epoch))
}

stop_reason="process_completed"
returncode=0
progress_count=0
last_max_file_bytes="-1"
stable_since_epoch=0
start_epoch="$(date +%s)"

python3 "$repo_root/scripts/timed_native_command.py" --timing "$native_timing_path" \
  --pid-file "$native_pid_path" -- "${cmd[@]}" >"$stdout_path" 2>"$stderr_path" &
pid="$!"

while kill -0 "$pid" 2>/dev/null; do
  if ! "${storage_guard[@]}" > "$log_dir/storage.json" 2> "$log_dir/storage_error.txt"; then
    stop_reason="storage_budget_guard_failed"
    cat "$log_dir/storage_error.txt" >&2
    stop_child
    break
  fi
  elapsed="$(elapsed_seconds)"
  read -r candidate_count candidate_total_bytes max_file_bytes < <(candidate_bytes)
  candidate_total_gb="$(gb_rounded "$candidate_total_bytes")"
  max_file_gb="$(gb_rounded "$max_file_bytes")"
  # The child may finish between kill -0 and this sample.
  native_pid="$(cat "$native_pid_path" 2>/dev/null || printf '%s' "$pid")"
  cpu="$(cpu_percent "$native_pid" || true)"
  cpu="${cpu:-0}"
  printf '{"elapsed_seconds":%s,"candidate_file_count":%s,"candidate_total_gb":%s,"max_file_gb":%s,"process_cpu_percent":%s}\n' \
    "$elapsed" "$candidate_count" "$candidate_total_gb" "$max_file_gb" "$cpu" | tee -a "$progress_path"
  progress_count=$((progress_count + 1))

  if float_ge "$candidate_total_gb" "$max_artifact_gb"; then
    stop_reason="max_artifact_gb_exceeded"
    stop_child
    break
  fi
  if (( elapsed >= max_runtime_seconds )); then
    stop_reason="max_runtime_seconds_exceeded"
    stop_child
    break
  fi
  if (( min_progress_seconds > 0 )) \
    && (( elapsed >= min_progress_seconds )) \
    && float_le "$max_file_gb" "$min_progress_gb"; then
    stop_reason="min_progress_gb_not_reached"
    stop_child
    break
  fi

  if float_ge "$max_file_gb" "$stable_artifact_min_gb" \
    && [[ "$max_file_bytes" == "$last_max_file_bytes" ]] \
    && float_le "$cpu" "$idle_cpu_percent"; then
    if (( stable_since_epoch == 0 )); then
      stable_since_epoch="$(date +%s)"
    elif (( $(date +%s) - stable_since_epoch >= stable_artifact_seconds )); then
      stop_reason="stable_artifact_idle_timeout"
      stop_child
      break
    fi
  else
    stable_since_epoch=0
  fi
  last_max_file_bytes="$max_file_bytes"
  sleep "$progress_interval_seconds"
done

if wait "$pid"; then
  returncode=0
else
  returncode="$?"
fi
pid=""
if [[ "$stop_reason" != "process_completed" && "$returncode" -eq 0 ]]; then
  returncode=78
fi
# Catch a fast producer that completed between samples.
if ! "${storage_guard[@]}" > "$log_dir/storage.json" 2> "$log_dir/storage_error.txt"; then
  stop_reason="storage_budget_guard_failed"
  returncode=78
fi

end_epoch="$(date +%s)"
elapsed=$((end_epoch - start_epoch))
native_seconds="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["seconds"])' "$native_timing_path" 2>/dev/null || printf 'null')"
native_peak_rss_bytes="$(python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1])).get("peak_rss_bytes")))' "$native_timing_path" 2>/dev/null || printf 'null')"
if [[ "$native_seconds" == "null" && "$returncode" -eq 0 ]]; then
  returncode=78
  stop_reason="native_timing_missing"
fi
source_bytes="$(file_size_bytes "$source_path")"
source_allocated_bytes="$(file_allocated_bytes "$source_path")"
target_exists="false"
if [[ -e "$target_path" ]]; then
  target_exists="true"
fi
target_bytes="$(file_size_bytes "$target_path")"
stdout_json_ok="false"
if python3 -c 'import json,sys; value=json.load(open(sys.argv[1])); sys.exit(0 if isinstance(value, dict) else 1)' "$stdout_path" 2>/dev/null; then
  stdout_json_ok="true"
fi
if [[ "$stdout_json_ok" != "true" && "$returncode" -eq 0 ]]; then
  returncode=78
  stop_reason="invalid_native_json_output"
fi

cat > "$summary_path" <<JSON
{
  "schema_version": "shardloom.clickbench.ingest_cli_uat_gated.v1",
  "claim_boundary": "local CLI replacement-ingest UAT only; no official benchmark claim",
  "created_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "command_file": "$(json_escape "$cmd_path")",
  "stdout_path": "$(json_escape "$stdout_path")",
  "stderr_path": "$(json_escape "$stderr_path")",
  "progress_path": "$(json_escape "$progress_path")",
  "log_dir": "$(json_escape "$log_dir")",
  "returncode": $returncode,
  "stop_reason": "$(json_escape "$stop_reason")",
  "elapsed_seconds": $elapsed,
  "native_process_seconds": $native_seconds,
  "native_peak_rss_bytes": $native_peak_rss_bytes,
  "native_timing_path": "$(json_escape "$native_timing_path")",
  "source": "$(json_escape "$source_path")",
  "source_bytes": $source_bytes,
  "source_allocated_bytes": $source_allocated_bytes,
  "target": "$(json_escape "$target_path")",
  "target_exists": $target_exists,
  "target_bytes": $target_bytes,
  "memory_gb": $memory_gb,
  "max_parallelism": $max_parallelism,
  "max_runtime_seconds": $max_runtime_seconds,
  "max_artifact_gb": $max_artifact_gb,
  "stable_artifact_seconds": $stable_artifact_seconds,
  "stable_artifact_min_gb": $stable_artifact_min_gb,
  "idle_cpu_percent": $idle_cpu_percent,
  "min_progress_seconds": $min_progress_seconds,
  "min_progress_gb": $min_progress_gb,
  "progress_sample_count": $progress_count,
  "stdout_json_ok": $stdout_json_ok
}
JSON

printf 'SUMMARY '
cat "$summary_path"

if [[ "$returncode" -eq 0 ]]; then
  exit 0
fi
exit "$returncode"
