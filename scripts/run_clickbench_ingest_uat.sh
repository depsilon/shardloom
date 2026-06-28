#!/usr/bin/env bash
set -euo pipefail

# Local engineering harness for ClickBench ingest UAT. This runs the ShardLoom
# CLI directly and redirects stdout/stderr to files so the process cannot block
# on an undrained JSON pipe. It is not an official benchmark runner.

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
uat_root="${SHARDLOOM_CLICKBENCH_UAT_ROOT:-$HOME/Desktop/shardloom-clickbench-100m-uat}"
binary="${SHARDLOOM_BIN:-$repo_root/target/release/shardloom}"
source_path="$uat_root/sources/hits.parquet"
input_format="parquet"
target_path="$uat_root/vortex/hits-parquet-100m.vortex"
max_parallelism="${SHARDLOOM_MAX_PARALLELISM:-2}"
replace_existing="false"
progress_interval_seconds=30
max_runtime_seconds=720
max_artifact_gb=38
stable_artifact_seconds=90
stable_artifact_min_gb=25
idle_cpu_percent=1
min_progress_seconds=360
min_progress_gb=1

usage() {
  cat <<'USAGE'
usage: scripts/run_clickbench_ingest_uat.sh [options]

Options:
  --uat-root PATH
  --binary PATH
  --source PATH
  --input-format FORMAT
  --target PATH
  --max-parallelism N
  --replace-existing
  --progress-interval-seconds N
  --max-runtime-seconds N
  --max-artifact-gb N
  --stable-artifact-seconds N
  --stable-artifact-min-gb N
  --idle-cpu-percent N
  --min-progress-seconds N
  --min-progress-gb N
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --uat-root) uat_root="$2"; shift 2 ;;
    --binary) binary="$2"; shift 2 ;;
    --source) source_path="$2"; shift 2 ;;
    --input-format) input_format="$2"; shift 2 ;;
    --target) target_path="$2"; shift 2 ;;
    --max-parallelism) max_parallelism="$2"; shift 2 ;;
    --replace-existing) replace_existing="true"; shift ;;
    --progress-interval-seconds) progress_interval_seconds="$2"; shift 2 ;;
    --max-runtime-seconds) max_runtime_seconds="$2"; shift 2 ;;
    --max-artifact-gb) max_artifact_gb="$2"; shift 2 ;;
    --stable-artifact-seconds) stable_artifact_seconds="$2"; shift 2 ;;
    --stable-artifact-min-gb) stable_artifact_min_gb="$2"; shift 2 ;;
    --idle-cpu-percent) idle_cpu_percent="$2"; shift 2 ;;
    --min-progress-seconds) min_progress_seconds="$2"; shift 2 ;;
    --min-progress-gb) min_progress_gb="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
  esac
done

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="$uat_root/logs/ingest_cli_uat_gated_$timestamp"
stdout_path="$log_dir/stdout.json"
stderr_path="$log_dir/stderr.txt"
progress_path="$log_dir/progress.jsonl"
summary_path="$log_dir/prepare_summary.json"
cmd_path="$log_dir/prepare.cmd.txt"
mkdir -p "$log_dir" "$(dirname "$target_path")"

target_dir="$(dirname "$target_path")"
target_name="$(basename "$target_path")"
target_stem="$target_name"
target_ext=""
if [[ "$target_name" == *.* ]]; then
  target_stem="${target_name%.*}"
  target_ext="${target_name##*.}"
fi

remove_existing_candidates() {
  if [[ -d "$target_dir" ]]; then
    if [[ -n "$target_ext" ]]; then
      find "$target_dir" -maxdepth 1 -type f \( \
        -name "$target_name" \
        -o -name "$target_name.*" \
        -o -name ".$target_name.shardloom-tmp-*" \
        -o -name "$target_stem [0-9]*.$target_ext" \
      \) -delete
    else
      find "$target_dir" -maxdepth 1 -type f \( \
        -name "$target_name" \
        -o -name "$target_name.*" \
        -o -name ".$target_name.shardloom-tmp-*" \
        -o -name "$target_name [0-9]*" \
      \) -delete
    fi
  fi
}

if [[ "$replace_existing" == "true" ]]; then
  remove_existing_candidates
fi

cmd=(
  "$binary" prepare dataframe
  --input "$source_path"
  --input-format "$input_format"
  --output "$target_path"
  --max-parallelism "$max_parallelism"
  --format json
)
if [[ "$replace_existing" != "true" ]]; then
  cmd+=(--allow-overwrite)
fi

printf '%q ' "${cmd[@]}" > "$cmd_path"
printf '\n' >> "$cmd_path"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

file_size_bytes() {
  if [[ -e "$1" ]]; then
    stat -f '%z' "$1"
  else
    printf '0'
  fi
}

candidate_bytes() {
  local total=0
  local max=0
  local count=0
  local path size
  if [[ -d "$target_dir" ]]; then
    if [[ -n "$target_ext" ]]; then
      while IFS= read -r -d '' path; do
        size="$(file_size_bytes "$path")"
        total=$((total + size))
        if (( size > max )); then
          max="$size"
        fi
        count=$((count + 1))
      done < <(find "$target_dir" -maxdepth 1 -type f \( \
        -name "$target_name" \
        -o -name "$target_name.*" \
        -o -name ".$target_name.shardloom-tmp-*" \
        -o -name "$target_stem [0-9]*.$target_ext" \
      \) -print0)
    else
      while IFS= read -r -d '' path; do
        size="$(file_size_bytes "$path")"
        total=$((total + size))
        if (( size > max )); then
          max="$size"
        fi
        count=$((count + 1))
      done < <(find "$target_dir" -maxdepth 1 -type f \( \
        -name "$target_name" \
        -o -name "$target_name.*" \
        -o -name ".$target_name.shardloom-tmp-*" \
        -o -name "$target_name [0-9]*" \
      \) -print0)
    fi
  fi
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

"${cmd[@]}" >"$stdout_path" 2>"$stderr_path" &
pid="$!"

while kill -0 "$pid" 2>/dev/null; do
  elapsed="$(elapsed_seconds)"
  read -r candidate_count candidate_total_bytes max_file_bytes < <(candidate_bytes)
  candidate_total_gb="$(gb_rounded "$candidate_total_bytes")"
  max_file_gb="$(gb_rounded "$max_file_bytes")"
  cpu="$(cpu_percent "$pid")"
  cpu="${cpu:-0}"
  printf '{"elapsed_seconds":%s,"candidate_file_count":%s,"candidate_total_gb":%s,"max_file_gb":%s,"process_cpu_percent":%s}\n' \
    "$elapsed" "$candidate_count" "$candidate_total_gb" "$max_file_gb" "$cpu" | tee -a "$progress_path"
  progress_count=$((progress_count + 1))

  if float_ge "$candidate_total_gb" "$max_artifact_gb"; then
    stop_reason="max_artifact_gb_exceeded"
    kill "$pid" 2>/dev/null || true
    break
  fi
  if (( elapsed >= max_runtime_seconds )); then
    stop_reason="max_runtime_seconds_exceeded"
    kill "$pid" 2>/dev/null || true
    break
  fi
  if (( min_progress_seconds > 0 )) \
    && (( elapsed >= min_progress_seconds )) \
    && float_le "$max_file_gb" "$min_progress_gb"; then
    stop_reason="min_progress_gb_not_reached"
    kill "$pid" 2>/dev/null || true
    break
  fi

  if float_ge "$max_file_gb" "$stable_artifact_min_gb" \
    && [[ "$max_file_bytes" == "$last_max_file_bytes" ]] \
    && float_le "$cpu" "$idle_cpu_percent"; then
    if (( stable_since_epoch == 0 )); then
      stable_since_epoch="$(date +%s)"
    elif (( $(date +%s) - stable_since_epoch >= stable_artifact_seconds )); then
      stop_reason="stable_artifact_idle_timeout"
      kill "$pid" 2>/dev/null || true
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

end_epoch="$(date +%s)"
elapsed=$((end_epoch - start_epoch))
source_bytes="$(file_size_bytes "$source_path")"
target_exists="false"
if [[ -e "$target_path" ]]; then
  target_exists="true"
fi
target_bytes="$(file_size_bytes "$target_path")"
stdout_json_ok="false"
if [[ -s "$stdout_path" ]] && [[ "$(head -c 1 "$stdout_path")" == "{" ]]; then
  stdout_json_ok="true"
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
  "source": "$(json_escape "$source_path")",
  "source_bytes": $source_bytes,
  "target": "$(json_escape "$target_path")",
  "target_exists": $target_exists,
  "target_bytes": $target_bytes,
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
