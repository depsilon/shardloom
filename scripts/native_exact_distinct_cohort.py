#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Freeze and run the declared exact DISTINCT affected-owner cohort.

`prepare` creates inputs/references and freezes identities without running queries.
`run` consumes that immutable protocol once. `smoke` is a 100-row, unscored API
check; it is not a benchmark and cannot produce a performance disposition.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import gc
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import resource
import shutil
import statistics
import subprocess
import sys
import time

from query_timing_report import TIMING_BOUNDARY, TIMING_CLOCK, query_phase_timings

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python/src"))
ROWS = 5_000_000
GROUPS = 50_000
ROW_GROUP_ROWS = 262_144
MEMORY_BYTES = 2 << 30
MAX_BYTES = 2 << 30
MAX_RESULT_BYTES = 64 << 20
MIN_FREE_BYTES = 1 << 30
SCHEMA = "shardloom.native_exact_distinct_cohort.v1"


def need(condition, message):
    if not condition:
        raise ValueError(message)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def identity(path):
    path = Path(path).resolve(strict=True)
    stat = path.stat()
    return {"path": str(path), "bytes": stat.st_size, "sha256": sha(path),
            "generation": [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]}


def check_identity(record, *, full):
    path = Path(record["path"])
    stat = path.stat()
    need([stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns] == record["generation"],
         f"file generation changed: {path}")
    if full:
        need(sha(path) == record["sha256"], f"file digest changed: {path}")


def guard(work, extra=0):
    used = sum(path.stat().st_size for path in work.rglob("*") if path.is_file())
    need(used + extra <= MAX_BYTES, "2 GiB cohort artifact ceiling exceeded")
    need(shutil.disk_usage(work).free >= MIN_FREE_BYTES + extra,
         "cohort requires at least 1 GiB free storage plus pending output")


def write_json(path, value):
    data = (json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    need(len(data) <= MAX_RESULT_BYTES, f"individual JSON artifact exceeds 64 MiB: {path}")
    path.write_bytes(data)


def write_arrow(path, table):
    import pyarrow as pa
    need(table.nbytes <= MAX_RESULT_BYTES, f"Arrow result exceeds 64 MiB: {path}")
    with pa.OSFile(str(path), "wb") as sink:
        with pa.ipc.new_file(sink, table.schema) as writer:
            writer.write_table(table)


def load_arrow(path):
    import pyarrow as pa
    with pa.memory_map(str(path), "r") as source:
        return pa.ipc.open_file(source).read_all()


def context_snapshot():
    snapshot = {"utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "platform": platform.platform(), "python": sys.version,
                "logical_cpus": os.cpu_count(),
                "cpu_affinity": sorted(os.sched_getaffinity(0)),
                "load_average": os.getloadavg(),
                "runner_maxrss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
    for path in ["/proc/meminfo", "/proc/cpuinfo", "/sys/fs/cgroup/cpu.max",
                 "/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"]:
        if Path(path).is_file():
            snapshot[path] = Path(path).read_text()
    snapshot["processes"] = subprocess.check_output(
        ["ps", "-eo", "pid,ppid,stat,pcpu,pmem,comm"], text=True)
    return snapshot


def worker_snapshot(client):
    # Read-only transport evidence; query execution uses only the public API.
    process = client._worker_process
    if process is None:
        return None
    need(not client._worker_disabled and process.poll() is None,
         "requested persistent worker was lost or disabled")
    root = Path("/proc") / str(process.pid)
    result = {"pid": process.pid, "cpu_affinity": sorted(os.sched_getaffinity(process.pid))}
    for name in ["status", "stat", "io"]:
        result[name] = (root / name).read_text()
    return result


def checked_binaries(provenance_path):
    provenance = json.loads(provenance_path.read_text())
    need(provenance.get("configuration_pair_matched") is True, "unmatched artifact pair")
    result = {}
    for role in ["base", "candidate"]:
        item = provenance[role]
        directory = Path(item["directory"])
        manifest_path = directory / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        need(manifest == item["manifest"], f"{role} artifact manifest differs from provenance")
        binary = identity(directory / "shardloom")
        expected = manifest["files"]["shardloom"]
        need(binary["bytes"] == expected["bytes"] and binary["sha256"] == expected["sha256"],
             f"{role} executable does not match sealed manifest")
        tree = subprocess.check_output(
            ["git", "rev-parse", item["source_revision"] + "^{tree}"], cwd=ROOT, text=True).strip()
        need(tree == item["source_tree"], f"{role} source tree mismatch")
        if manifest["identity"]["revision"] != item["source_revision"]:
            build = item.get("build_commit", {})
            need(build.get("sha") == manifest["identity"]["revision"]
                 and build.get("tree", {}).get("sha") == tree,
                 f"{role} test-merge build does not have the requested source tree")
        result[role] = {"binary": binary, "manifest": identity(manifest_path),
                        "source_revision": item["source_revision"], "source_tree": tree}
    base, candidate = (provenance[role]["manifest"] for role in ["base", "candidate"])
    for key in ["profile", "requested_features"]:
        need(base[key] == candidate[key], f"artifact build setting differs: {key}")
    for key in ["compiler_features", "compiler_profile", "command"]:
        need(base["build"][key] == candidate["build"][key], f"artifact compiler setting differs: {key}")
    for key in ["rustc", "cargo", "cargo_lock_sha256", "architecture", "os"]:
        need(base["identity"][key] == candidate["identity"][key], f"artifact identity differs: {key}")
    return provenance, result


def python_source_hash():
    digest = hashlib.sha256()
    for path in sorted((ROOT / "python/src/shardloom").rglob("*.py")):
        digest.update(str(path.relative_to(ROOT)).encode() + b"\0")
        digest.update(path.read_bytes())
    return digest.hexdigest()


def cases(fact, dim):
    def literal(path):
        return "'" + str(path).replace("'", "''") + "'"
    f, d = literal(fact), literal(dim)
    return [
        ("Q01_inner_join_control", ["region"],
         f"SELECT d.region AS region, COUNT(*) AS row_count, SUM(f.value) AS total_value "
         f"FROM {f} f INNER JOIN {d} d ON f.grp = d.key GROUP BY d.region"),
        ("Q02_grouped_50000_control", ["grp"],
         f"SELECT grp, COUNT(*) AS row_count, SUM(value) AS total_value FROM {f} GROUP BY grp"),
        ("Q03_filtered_count_value", [],
         f"SELECT COUNT(value) AS count_value FROM {f} WHERE flag = true"),
        ("Q04_scalar_count_value", [], f"SELECT COUNT(value) AS count_value FROM {f}"),
        ("Q05_distinct_low_tag", ["low_tag"], f"SELECT DISTINCT low_tag FROM {f}"),
        ("Q06_scalar_count_distinct_high", [],
         f"SELECT COUNT(DISTINCT high_tag) AS distinct_count FROM {f}"),
        ("Q07_grouped_count_distinct", ["grp"],
         f"SELECT grp, COUNT(DISTINCT low_tag) AS distinct_count, COUNT(*) AS row_count, "
         f"SUM(value) AS total_value FROM {f} GROUP BY grp"),
        ("Q08_float_distinct_serial_control", [],
         f"SELECT SUM(float_value) AS total_float, COUNT(DISTINCT high_tag) AS distinct_count FROM {f}"),
    ]


def generate(work, rows):
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq
    ids = np.arange(rows, dtype=np.int64)
    groups = ids % GROUPS
    values = (ids * 7) % 100 - 20
    invalid = ids % 17 == 0
    low_domain = pa.array([f"low-{i:04d}" for i in range(1024)])
    high_domain = pa.array([f"high-{i:05d}" for i in range(GROUPS)])
    table = pa.table({
        "id": pa.array(ids), "grp": pa.array(groups),
        "value": pa.array(values, mask=invalid),
        "float_value": pa.array(values.astype(np.float64) * 0.5, mask=invalid),
        "low_tag": pa.DictionaryArray.from_arrays(pa.array((ids % 1024).astype(np.int32)), low_domain),
        "high_tag": pa.DictionaryArray.from_arrays(pa.array(groups.astype(np.int32)), high_domain),
        "flag": pa.array(ids % 10 == 0),
    })
    # Declare input nullability explicitly; dictionary encoding is physical.
    table = table.cast(pa.schema([pa.field(field.name, field.type, field.name in {"value", "float_value"})
                                  for field in table.schema]))
    data = work / "inputs"
    data.mkdir()
    fact = data / "fact.parquet"
    pq.write_table(table, fact, row_group_size=ROW_GROUP_ROWS,
                   use_dictionary=["low_tag", "high_tag"], dictionary_pagesize_limit=16 << 20,
                   compression="zstd", write_statistics=True)
    dim_ids = np.arange(min(rows, GROUPS), dtype=np.int64)
    dimension = pa.Table.from_arrays([pa.array(dim_ids), pa.array(dim_ids % 16)],
        schema=pa.schema([pa.field("key", pa.int64(), False), pa.field("region", pa.int64(), False)]))
    dim = data / "dim.parquet"
    pq.write_table(dimension, dim, row_group_size=ROW_GROUP_ROWS, compression="zstd")
    del table, dimension, ids, groups, values, invalid
    gc.collect()
    # Independent exact formulas, using integer arithmetic before conversion to
    # the public SUM f64 result. Each group's numeric value is constant because
    # 50,000 is a multiple of 100. NULL periods are evaluated independently.
    group_ids = np.arange(min(rows, GROUPS), dtype=np.int64)
    counts = (rows - 1 - group_ids) // GROUPS + 1
    valid_counts = np.zeros(len(group_ids), dtype=np.int64)
    for repetition in range((rows + GROUPS - 1) // GROUPS):
        row_ids = group_ids + repetition * GROUPS
        valid_counts += ((row_ids < rows) & (row_ids % 17 != 0)).astype(np.int64)
    sums = ((group_ids * 7) % 100 - 20) * valid_counts
    region_counts = np.zeros(16, dtype=np.int64)
    region_valid = np.zeros(16, dtype=np.int64)
    region_sums = np.zeros(16, dtype=np.int64)
    np.add.at(region_counts, group_ids % 16, counts)
    np.add.at(region_valid, group_ids % 16, valid_counts)
    np.add.at(region_sums, group_ids % 16, sums)

    def result(names, arrays, nullable):
        return pa.Table.from_arrays(arrays, schema=pa.schema([
            pa.field(name, array.type, null) for name, array, null in zip(names, arrays, nullable)]))
    u64 = lambda values: pa.array(values, type=pa.uint64())
    f64 = lambda values, valid: pa.array(values, mask=np.asarray(valid) == 0, type=pa.float64())
    count_all = rows - (rows - 1) // 17 - 1
    count_filtered = (rows - 1) // 10 - (rows - 1) // 170
    active_regions = region_counts > 0
    references = [
        result(["region", "row_count", "total_value"],
               [pa.array(np.arange(16, dtype=np.int64)[active_regions]), u64(region_counts[active_regions]),
                f64(region_sums[active_regions], region_valid[active_regions])], [False, False, True]),
        result(["grp", "row_count", "total_value"],
               [pa.array(group_ids), u64(counts), f64(sums, valid_counts)], [False, False, True]),
        result(["count_value"], [u64([count_filtered])], [False]),
        result(["count_value"], [u64([count_all])], [False]),
        result(["low_tag"], [pa.array([f"low-{i:04d}" for i in range(min(rows, 1024))])], [False]),
        result(["distinct_count"], [u64([min(rows, GROUPS)])], [False]),
        result(["grp", "distinct_count", "row_count", "total_value"],
               [pa.array(group_ids), u64(np.minimum(counts, 1024 // math.gcd(GROUPS, 1024))),
                u64(counts), f64(sums, valid_counts)], [False, False, False, True]),
        result(["total_float", "distinct_count"],
               [pa.array([int(sums.sum()) * 0.5], type=pa.float64()), u64([min(rows, GROUPS)])], [True, False]),
    ]
    (work / "references").mkdir()
    for (name, _, _), reference in zip(cases(fact, dim), references):
        write_arrow(work / "references" / f"{name}.arrow", reference)
    parquet = pq.ParquetFile(fact)
    encodings = [{"row_group": rg, "column": parquet.metadata.row_group(rg).column(col).path_in_schema,
                  "encodings": list(parquet.metadata.row_group(rg).column(col).encodings)}
                 for rg in range(parquet.num_row_groups) for col in [4, 5]]
    need(all("RLE_DICTIONARY" in entry["encodings"] for entry in encodings),
         "generated Parquet string chunks lack dictionary encoding")
    return fact, dim, {"fact_schema": str(parquet.schema_arrow), "parquet_string_encodings": encodings}


def make_client(binary, tmp, timeout):
    from shardloom import ShardLoomClient
    return ShardLoomClient(binary=binary, transport="subprocess", use_persistent_worker=True, timeout=timeout,
                          memory_bytes=MEMORY_BYTES, max_parallelism=4,
                          env={"TMPDIR": str(tmp), "SHARDLOOM_PERSISTENT_WORKER": "true"})


def prepare(args):
    import numpy as np
    import pyarrow as pa
    import shardloom as sl
    work = args.work_dir.resolve()
    need(not work.exists(), "prepare requires a new work directory; existing observations are immutable")
    need(not work.is_relative_to(ROOT), "benchmark artifacts must live outside the Git checkout")
    work.mkdir(parents=True)
    (work / "tmp").mkdir()
    guard(work)
    provenance, binaries = checked_binaries(args.provenance.resolve(strict=True))
    write_json(work / "provenance.json", provenance)
    rows = 100 if args.command == "smoke" else ROWS
    fact, dim, data_info = generate(work, rows)
    sources = {"parquet": [fact, dim]}
    if "vortex" in args.formats:
        client = make_client(binaries["base"]["binary"]["path"], work / "tmp", args.timeout)
        try:
            for source in [fact, dim]:
                destination = source.with_suffix(".vortex")
                report = sl.read_parquet(source, client=client).prepare_vortex(destination, check=False)
                write_json(work / f"prepare_{source.stem}_vortex.json", dict(report.envelope.raw))
                fields = report.envelope.field_map
                need(report.envelope.status == "success", "native Vortex preparation did not complete")
                need(not report.envelope.fallback.attempted and fields.get("fallback_attempted") == "false"
                     and fields.get("external_engine_invoked") == "false", "Vortex preparation no-fallback evidence missing")
                need(destination.is_file(), "native Vortex preparation did not produce its artifact")
                guard(work)
            sources["vortex"] = [fact.with_suffix(".vortex"), dim.with_suffix(".vortex")]
        finally:
            client.close()
    declared = []
    for format_name in args.formats:
        for name, keys, sql in cases(*sources[format_name]):
            declared.append({"id": f"{format_name}.{name}", "format": format_name, "query": name,
                             "sql": sql, "sort_keys": keys,
                             "reference": identity(work / "references" / f"{name}.arrow")})
    protocol = {"schema": SCHEMA, "mode": "api_smoke_not_benchmark" if rows == 100 else "benchmark",
                "rows": rows, "cases": declared, "formats": args.formats,
                "warmups": 0 if rows == 100 else 1, "measured_repetitions": 1 if rows == 100 else 3,
                "memory_bytes": MEMORY_BYTES, "max_parallelism": 4, "timeout_seconds": args.timeout,
                "ordering": "per-case role order alternates by (case_index + repetition) parity; warmup first",
                "clock": TIMING_CLOCK, "boundary": TIMING_BOUNDARY,
                "primary": "sum_of_per_case_warm_engine_medians", "secondary": "geometric_mean_candidate_over_base",
                "activation": "full cohort: candidate Q06/Q07 source and operator workers >=2; Q08 operator workers ==1 for both roles",
                "cache": "one persistent worker per role per case; one warmup; OS cache uncontrolled; no answer reuse",
                "driver": identity(Path(__file__)), "timing_helper": identity(ROOT / "scripts/query_timing_report.py"),
                "python_source_sha256": python_source_hash(), "numpy_version": np.__version__,
                "pyarrow_version": pa.__version__, "binaries": binaries,
                "sources": {key: [identity(path) for path in paths] for key, paths in sources.items()},
                "provenance": identity(work / "provenance.json"), "data": data_info,
                "artifact_limit_bytes": MAX_BYTES, "query_result_limit_bytes": MAX_RESULT_BYTES,
                "claim_gate_status": "not_claim_grade"}
    write_json(work / "preparation_context.json", context_snapshot())
    write_json(work / "protocol.json", protocol)
    guard(work)
    print(json.dumps({"stage": "frozen", "protocol": str(work / "protocol.json"),
                      "protocol_sha256": sha(work / "protocol.json"), "cases": len(declared), "rows": rows}), flush=True)
    return work


def canonical(table, keys):
    return table.sort_by([(key, "ascending") for key in keys]) if keys and table.num_rows else table


def require_activation(case, role, fields, *, smoke):
    if smoke:
        return

    def workers(field):
        value = fields.get(field)
        need(isinstance(value, str) and value.isdecimal(), f"missing or malformed activation counter: {field}")
        return int(value)

    if role == "candidate" and case["query"] in {
        "Q06_scalar_count_distinct_high", "Q07_grouped_count_distinct",
    }:
        need(workers("native_relational_source_workers") >= 2
             and workers("native_relational_operator_workers") >= 2,
             f"parallel source/reducer activation absent: {case['id']} {role}")
    if case["query"] == "Q08_float_distinct_serial_control":
        need(workers("native_relational_operator_workers") == 1,
             f"floating reduction serial-order guard absent: {case['id']} {role}")


def run(work, *, smoke=False):
    import numpy as np
    import pyarrow as pa
    import shardloom as sl
    protocol_path = work / "protocol.json"
    protocol = json.loads(protocol_path.read_text())
    need(protocol["schema"] == SCHEMA, "unknown cohort protocol")
    need((protocol["mode"] == "api_smoke_not_benchmark") == smoke, "smoke and benchmark modes cannot mix")
    need(protocol["rows"] == (100 if smoke else ROWS), "frozen row count changed")
    for field in ["driver", "timing_helper", "provenance"]:
        check_identity(protocol[field], full=True)
    need(python_source_hash() == protocol["python_source_sha256"], "Python client source changed since freeze")
    need(np.__version__ == protocol["numpy_version"] and pa.__version__ == protocol["pyarrow_version"],
         "Python data dependency versions changed since freeze")
    identities = [item for sources in protocol["sources"].values() for item in sources]
    identities += [item[key] for item in protocol["binaries"].values() for key in ["binary", "manifest"]]
    identities += [case["reference"] for case in protocol["cases"]]
    for item in identities:
        check_identity(item, full=True)
    output = work / "observations"
    need(not output.exists(), "this frozen cohort already has observations; no replacement or best-of rerun")
    output.mkdir()
    summary = {"schema": SCHEMA, "status": "running", "mode": protocol["mode"],
               "protocol_sha256": sha(protocol_path), "records": [], "case_medians": [],
               "claim_gate_status": "not_claim_grade"}
    with (work.parent / ".native-exact-distinct-cohort.lock").open("a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        write_json(output / "host_before.json", context_snapshot())
        try:
            for case_index, case in enumerate(protocol["cases"]):
                expected = canonical(load_arrow(case["reference"]["path"]), case["sort_keys"])
                clients = {role: make_client(item["binary"]["path"], work / "tmp", protocol["timeout_seconds"])
                           for role, item in protocol["binaries"].items()}
                plans = {role: sl.sql(case["sql"], client=client) for role, client in clients.items()}
                need(all(plan.source._session is None for plan in plans.values()),
                     "cohort must execute directly without a session answer cache")
                try:
                    for repetition in range(-protocol["warmups"], protocol["measured_repetitions"]):
                        order = ["base", "candidate"] if (case_index + repetition) % 2 == 0 else ["candidate", "base"]
                        for position, role in enumerate(order):
                            guard(work, 2 * MAX_RESULT_BYTES)
                            for item in identities:
                                check_identity(item, full=False)
                            label = "warmup" if repetition < 0 else f"r{repetition + 1}"
                            prefix = output / f"{case['id']}.{role}.{label}"
                            record = {"case": case["id"], "role": role, "repetition": repetition,
                                      "warmup": repetition < 0, "pair_position": position,
                                      "sql": case["sql"], "passed": False,
                                      "worker_before": worker_snapshot(clients[role])}
                            summary["records"].append(record)
                            started = time.perf_counter()
                            result = plans[role].collect(check=False)
                            collect_seconds = time.perf_counter() - started
                            if result.envelope.status != "success":
                                write_json(prefix.with_suffix(prefix.suffix + ".report.json"), dict(result.envelope.raw))
                                raise ValueError(f"query did not complete: {case['id']} {role}: {result.envelope.status}")
                            arrow = result.to_arrow()
                            outer_seconds = time.perf_counter() - started
                            fields = dict(result.envelope.field_map)
                            record["worker_after"] = worker_snapshot(clients[role])
                            need(record["worker_after"] is not None, "query did not use its persistent worker")
                            write_json(prefix.with_suffix(prefix.suffix + ".report.json"), dict(result.envelope.raw))
                            write_arrow(prefix.with_suffix(prefix.suffix + ".arrow"), arrow)
                            require_activation(case, role, fields, smoke=smoke)
                            need(result.envelope.status == "success", "query did not complete")
                            need(fields.get("fallback_attempted") == "false"
                                 and fields.get("external_engine_invoked") == "false", "no-fallback evidence missing")
                            timings = query_phase_timings(fields, collect_seconds)
                            need(int(fields["native_query_execution_nanos"]) > 0, "native query clock did not advance")
                            need(fields.get("query_answer_cached", "false") == "false", "query answer cache was used")
                            actual = canonical(arrow, case["sort_keys"])
                            need(actual.schema.equals(expected.schema, check_metadata=False),
                                 f"typed schema mismatch: {case['id']} {role}: {actual.schema} != {expected.schema}")
                            need(actual.equals(expected, check_metadata=False), f"complete value mismatch: {case['id']} {role}")
                            record.update({"passed": True, "engine_execution_seconds": timings["engine_execution_seconds"],
                                           "native_query_execution_nanos": fields["native_query_execution_nanos"],
                                           "result_serialization_seconds": timings["result_serialization_seconds"],
                                           "collect_seconds": collect_seconds, "arrow_inclusive_seconds": outer_seconds,
                                           "rows": arrow.num_rows, "arrow_bytes": arrow.nbytes,
                                           "python_session_attached": False,
                                           "report": prefix.name + ".report.json", "arrow": prefix.name + ".arrow",
                                           "evidence": {key: value for key, value in fields.items() if any(token in key for token in
                                               ["worker", "dictionary", "epoch", "peak", "memory", "native_query_", "route", "provider",
                                                "persistent", "cache", "decoded", "fallback", "external_engine", "reservation"])}})
                            write_json(prefix.with_suffix(prefix.suffix + ".observation.json"), record)
                            del result, arrow, actual
                            gc.collect()
                            guard(work)
                            print(json.dumps({"stage": "query_complete", "case": case["id"], "role": role,
                                              "sample": label, "passed": True}), flush=True)
                finally:
                    for client in clients.values():
                        client.close()
                if not smoke:
                    medians = {role: statistics.median(record["engine_execution_seconds"] for record in summary["records"]
                               if record["case"] == case["id"] and record["role"] == role and not record["warmup"])
                               for role in ["base", "candidate"]}
                    summary["case_medians"].append({"case": case["id"], **medians,
                                                    "candidate_over_base": medians["candidate"] / medians["base"]})
            for item in identities:
                check_identity(item, full=True)
            need(sha(protocol_path) == summary["protocol_sha256"], "protocol changed during execution")
            summary["status"] = "passed"
            if not smoke:
                base = math.fsum(case["base"] for case in summary["case_medians"])
                candidate = math.fsum(case["candidate"] for case in summary["case_medians"])
                summary.update({"engine_seconds_sum_of_case_medians": {"base": base, "candidate": candidate},
                                "candidate_over_base_total": candidate / base,
                                "geometric_mean_candidate_over_base": math.exp(statistics.mean(
                                    math.log(case["candidate_over_base"]) for case in summary["case_medians"])),
                                "timing_gate": "lower_total" if candidate < base else "unchanged_or_higher_total",
                                "performance_acceptance": "requires_correctness_resource_ownership_review"})
        except BaseException as error:
            summary["status"] = "failed"
            summary["error"] = f"{type(error).__name__}: {error}"
            # An incomplete cohort has no accepted aggregate timing score.
            raise
        finally:
            write_json(output / "summary.json", summary)
            write_json(output / "host_after.json", context_snapshot())
            guard(work)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "run", "smoke"])
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--formats", nargs="+", choices=["parquet", "vortex"], default=["parquet", "vortex"])
    parser.add_argument("--timeout", type=float, default=180.0)
    args = parser.parse_args()
    need(os.name == "posix" and sys.platform == "linux", "this protocol requires Linux process evidence")
    need(args.formats[0] == "parquet" and len(set(args.formats)) == len(args.formats),
         "Parquet is required and formats must be unique")
    if args.command == "run":
        run(args.work_dir.resolve(strict=True))
    else:
        need(args.provenance is not None, "prepare/smoke requires --provenance")
        work = prepare(args)
        if args.command == "smoke":
            run(work, smoke=True)


if __name__ == "__main__":
    main()
