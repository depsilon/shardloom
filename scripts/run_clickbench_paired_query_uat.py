#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Counterbalanced old/new native queries using the existing guarded UAT clock.

Both roles run three times per query, with order reversed at each pair and query.
This controls temporal drift better than comparing suites from separate days; it
does not flush the OS cache or claim cold-storage, production or official rank.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import io
import json
import math
import os
from pathlib import Path
import platform
import re
import signal
import statistics
import subprocess
import sys
import tarfile

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from run_clickbench_query_uat import equivalent, extract_result, file_sha256, read_json_log, run_profiled_command, score


def role_order(query, run, reverse=False):
    return ("control", "candidate") if (query + run + int(reverse)) % 2 == 0 else ("candidate", "control")


def generation(path):
    s = path.stat()
    return {"device": s.st_dev, "inode": s.st_ino, "size_bytes": s.st_size,
            "mtime_ns": s.st_mtime_ns, "ctime_ns": s.st_ctime_ns}


def host_snapshot():
    """Outside native timing; VM deltas are host-wide, not query attribution."""
    result = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(), "load_average": os.getloadavg()}
    if sys.platform == "darwin":
        completed = subprocess.run(["vm_stat"], capture_output=True, text=True, timeout=10)
        if completed.returncode:
            result["vm_stat_error"] = completed.stderr
        else:
            wanted = {"Pages free", "Pages inactive", "Pages reactivated", "File-backed pages",
                      "Anonymous pages", "Pages occupied by compressor", "Compressions",
                      "Decompressions", "Pageins", "Pageouts", "Swapins", "Swapouts"}
            result["vm_stat"] = {m[0]: int(m[1]) for m in re.findall(r'^([^:\n]+):\s+(\d+)\.', completed.stdout, re.MULTILINE) if m[0] in wanted}
            page = re.search(r'page size of (\d+) bytes', completed.stdout)
            result["vm_page_size_bytes"] = int(page[1]) if page else None
    return result


def archive_completed_logs(paths, target, guard):
    """Archive only caller-owned closed files; verify every byte before removal."""
    if not paths or len({p.name for p in paths}) != len(paths):
        raise ValueError("archive requires nonempty unique filenames")
    if any(p.parent != target.parent or p == target for p in paths):
        raise ValueError("archive must own only sibling run files")
    raw = {p.name: p.read_bytes() for p in paths}
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:xz") as archive:
        for name, data in raw.items():
            item = tarfile.TarInfo(name)
            item.size = len(data)
            archive.addfile(item, io.BytesIO(data))
    packed = buffer.getvalue()
    block = max(4096, os.statvfs(target.parent).f_frsize)
    guard(math.ceil(len(packed) / block) * block)
    with target.open("xb") as stream:
        stream.write(packed)
    with tarfile.open(target, "r:xz") as archive:
        if archive.getnames() != list(raw):
            raise ValueError("archive member set changed")
        for name, data in raw.items():
            if archive.extractfile(name).read() != data:
                raise ValueError("archive does not preserve completed evidence")
    guard(0)
    for path in paths:
        if path.read_bytes() != raw[path.name]:
            raise ValueError("completed log changed before archival")
    receipt = {"path": str(target), "sha256": file_sha256(target),
               "members": [{"name": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
                           for name, data in raw.items()]}
    for path in paths:
        path.unlink()
    guard(0)
    return receipt


def paired_scores(records, selected):
    if any(r.get("role") not in ("control", "candidate") for r in records):
        raise ValueError("unknown paired role")
    scores = {role: score([r for r in records if r["role"] == role], 43, selected)
              for role in ("control", "candidate")}
    complete = all(s["complete"] for s in scores.values())
    result = {"complete": complete, "roles": scores, "score_scope": "full_43" if selected == list(range(1, 44)) else "targeted_only"}
    if complete:
        result["per_query"] = []
        for query in selected:
            by_role = {role: sorted((r for r in records if r["role"] == role and r["query"] == query), key=lambda r: r["run"])
                       for role in scores}
            times = {role: [r["seconds"] for r in runs] for role, runs in by_role.items()}
            result["per_query"].append({"query": query, "seconds": times,
                "median_seconds": {role: statistics.median(t) for role, t in times.items()},
                "paired_candidate_minus_control_seconds": [c - b for c, b in zip(times["candidate"], times["control"])]})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for role in ("control", "candidate"):
        parser.add_argument(f"--{role}-binary", type=Path, required=True)
        parser.add_argument(f"--{role}-commit", required=True)
    for name in ("input", "uat-root", "reference-dir"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    parser.add_argument("--queries", type=Path, default=Path(__file__).resolve().parents[1] / "benchmarks/clickbench/queries.sql")
    parser.add_argument("--query-ids")
    parser.add_argument("--memory-gb", type=int, default=24)
    parser.add_argument("--max-parallelism", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--reverse-order", action="store_true")
    args = parser.parse_args()
    if args.memory_gb <= 0 or args.max_parallelism <= 0 or not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("memory, parallelism and timeout must be positive")
    try:
        selected = [int(q) for q in args.query_ids.split(",")] if args.query_ids else list(range(1, 44))
        score([], 43, selected)
    except ValueError as error:
        parser.error(str(error))
    root = require_local_path(args.uat_root, Path.home(), sys.platform)
    source = require_local_path(args.input, Path.home(), sys.platform)
    logs = root / "logs" / ("paired43_" + dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ"))

    def guard(reserve=0):
        return check_budgets(root, source, logs, min_free_bytes=12*GIB, reserve_bytes=reserve,
                             max_workspace_bytes=100*GIB, max_log_bytes=256*MIB-reserve)

    guard()
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".ingest-uat.lock"
    lock.mkdir()
    summary = {}

    def save():
        data = json.dumps(summary, separators=(",", ":"), allow_nan=False).encode() + b"\n"
        block = max(4096, os.statvfs(logs).f_frsize)
        guard(math.ceil(len(data) / block) * block)
        pending = logs / "summary.pending.json"
        with pending.open("xb") as stream:
            stream.write(data)
        pending.replace(logs / "summary.json")
        guard()

    try:
        logs.mkdir(parents=True)
        queries = [q.strip() for q in "\n".join(line for line in args.queries.read_text().splitlines()
                   if not line.lstrip().startswith("--")).split(";") if q.strip()]
        if len(queries) != 43:
            raise ValueError("expected the pinned 43-statement query file")
        reference_paths = {q: args.reference_dir / f"q{q:02d}_run1.stdout.json" for q in selected}
        references = {q: extract_result(read_json_log(path)) for q, path in reference_paths.items()}
        binaries = {role: getattr(args, role + "_binary").resolve() for role in ("control", "candidate")}
        identities = {role: {"path": str(path), "sha256": file_sha256(path), "commit": getattr(args, role + "_commit")}
                      for role, path in binaries.items()}
        harness = {str(Path(__file__).with_name(name).resolve()): file_sha256(Path(__file__).with_name(name))
                   for name in (Path(__file__).name, "run_clickbench_query_uat.py", "timed_native_command.py", "local_uat_storage.py")}
        original = generation(source)
        records = []
        summary = {"schema_version": "shardloom.clickbench.counterbalanced_pairs.v1", "binaries": identities,
                   "harness_sha256": harness,
                   "source": str(source), "source_generation": original, "queries_sha256": file_sha256(args.queries),
                   "memory_gb": args.memory_gb, "max_parallelism": args.max_parallelism,
                   "platform": platform.platform(), "cpu_count": os.cpu_count(), "selected_query_ids": selected,
                   "queries": {str(q): queries[q-1] for q in selected},
                   "command_template": ["{binary}", "run", "sql", "--input", str(source), "--input-format", "vortex", "--sql", "{sql}",
                                        "--request", "collect", "--bounded", "true", "--memory-gb", str(args.memory_gb), "--max-parallelism", str(args.max_parallelism), "--format", "json"],
                   "reference_values_sha256": {str(q): hashlib.sha256(json.dumps(v, sort_keys=True, allow_nan=False).encode()).hexdigest() for q, v in references.items()},
                   "timing_boundary": "native process creation through complete public CLI output and exit; host snapshots and archival excluded",
                   "cache_policy": "fresh process per operation; OS cache shared and uncontrolled; no answer cache or forced purge",
                   "order_policy": "alternate role order at each pair and query", "reverse_order": args.reverse_order,
                   "host_counter_scope": "host-wide VM observations, not unique query traffic or exclusive attribution",
                   "correctness_boundary": "complete returned values against retained native outputs; finite floats use 1e-12 tolerance; not an independent oracle",
                   "records": records, "archives": [], "scores": paired_scores(records, selected)}
        save()
        for query in selected:
            closed = []
            for run in range(1, 4):
                for position, role in enumerate(role_order(query, run, args.reverse_order), 1):
                    prefix = logs / f"q{query:02d}_run{run}_{role}"
                    command = [str(binaries[role]), "run", "sql", "--input", str(source), "--input-format", "vortex",
                               "--sql", queries[query-1], "--request", "collect", "--bounded", "true",
                               "--memory-gb", str(args.memory_gb), "--max-parallelism", str(args.max_parallelism), "--format", "json"]
                    before = host_snapshot()
                    record = run_profiled_command(command, prefix, args.timeout, guard)
                    record.update(query=query, run=run, role=role, pair_position=position,
                                  host_before=before, host_after=host_snapshot(), passed=False)
                    try:
                        if record["returncode"] or record["guard_failures"]:
                            raise ValueError("native command or watchdog failed")
                        actual = extract_result(read_json_log(prefix.with_suffix(".stdout.json")))
                        if not equivalent(actual, references[query]):
                            raise ValueError("complete result differs from retained reference")
                        if generation(source) != original:
                            raise ValueError("source generation changed")
                        record.update(passed=True, validation="complete_values",
                                      result_sha256=hashlib.sha256(json.dumps(actual, sort_keys=True, allow_nan=False).encode()).hexdigest())
                    except (ValueError, OSError) as error:
                        record["failure"] = str(error)
                    closed.extend(prefix.with_suffix(suffix) for suffix in (".stdout.json", ".stderr.txt", ".timing.json", ".pid"))
                    records.append(record)
                    summary["scores"] = paired_scores(records, selected)
                    save()
                    print(json.dumps({k: record[k] for k in ("query", "run", "role", "pair_position", "seconds", "passed")}), flush=True)
                    if not record["passed"]:
                        return 1
            summary["archives"].append(archive_completed_logs(closed, logs / f"q{query:02d}_completed.tar.xz", guard))
            save()
        if (any(file_sha256(path) != identities[role]["sha256"] for role, path in binaries.items())
                or file_sha256(args.queries) != summary["queries_sha256"]
                or any(file_sha256(Path(path)) != digest for path, digest in harness.items())):
            raise ValueError("binary, query file or harness changed")
        summary["complete_result_validation"] = all(r.get("validation") == "complete_values" and r["passed"] for r in records)
        summary["completed_identity_check"] = True
        save()
        return 0
    except BaseException as error:
        if summary:
            summary["failure"] = str(error) or type(error).__name__
            summary["completed_identity_check"] = False
            summary["complete_result_validation"] = False
            # Preserve the last atomic summary if storage admission itself failed.
            try:
                save()
            except (OSError, ValueError):
                pass
        raise
    finally:
        lock.rmdir()
        print(f"UAT evidence: {logs}", flush=True)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
