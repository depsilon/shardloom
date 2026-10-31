#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Freeze, observe once and replay the declared 48-case native grouped diagnostic.

This is instrumented attribution, not a production benchmark or baseline update.
Build the release test executable separately. Queries never invoke Cargo, an
external engine, a source generator or a recursive build/evidence inventory.
"""
from __future__ import annotations

import argparse
import importlib.util
import itertools
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO / "scripts"), str(REPO / "python/src"),
               str(REPO / "benchmarks/traditional_analytics")]

from local_uat_storage import GIB, MIB, check_budgets, require_local_path
from native_workflow_protocol import strict_json
from query_surface.corpus import corpus
from query_surface.evidence import generation, identity
from query_surface.semantics import TypedResult, compare, result_from_record, result_record
from resources import BenchmarkGuard
from shardloom._result_schema import python_rows, schema_fields

TEST = ("local_primitives::prepared_relational::tests::grouped_cost_tests::"
        "native_grouped_cost_attribution")
PHASES = ("source_intake", "group_preparation", "group_interning", "state_reservation",
          "measure_preparation", "typed_updates", "other_updates", "aggregate_remainder",
          "finalization", "downstream_excluded")
FACTORS = (("signed_value", "optional_value", "metric", "all_null"),
           ("group_key", "category", "group_key_category"),
           ("sum", "mixed"), ("all", "one_percent"))
SUPERVISOR_SHA = "1e83cd760be0dce608a3449b1b170db8d12e141f08e84c0ec7dfd347cb585e97"


def read(path):
    return strict_json(Path(path).read_text())


def write(path, value):
    with Path(path).open("x") as stream:
        json.dump(value, stream, indent=2, allow_nan=False)
        stream.write("\n")


def verify(items):
    for item in items:
        assert identity(Path(item["path"])) == item, item["path"]


def selected_cases(before):
    wanted = {f"aggregate_{value}_{groups}_{measures}_direct_{selection}":
              dict(value=value, groups=groups, measures=measures, selection=selection)
              for value, groups, measures, selection in itertools.product(*FACTORS)}
    selected = [dict(case, diagnostic=wanted[case["case_id"]])
                for case in before["cases"] if case["case_id"] in wanted]
    assert len(selected) == len({case["case_id"] for case in selected}) == 48
    assert {case["case_id"] for case in selected} == set(wanted)
    catalog = {case.id: case for case in corpus(interactions="all", include_pending=False)}
    for case in selected:
        declared = catalog[case["case_id"]]
        assert declared.identity == case["case_identity"]
        assert declared.relation.record() == case["relation"]
        assert dict(declared.factors) == dict(argument=case["diagnostic"]["value"],
            keys=case["diagnostic"]["groups"], measures=case["diagnostic"]["measures"],
            expression="direct", filter=case["diagnostic"]["selection"])
    return selected


def freeze(args):
    root = require_local_path(args.root, Path.home(), sys.platform)
    assert not root.exists()
    binary = args.binary.resolve(strict=True)
    require_local_path(binary, Path.home(), sys.platform)
    assert "/release/deps/" in str(binary) and binary.name.startswith("shardloom_vortex-")
    build = read(args.build_receipt)
    assert build["status"] == "passed" and build["returncode"] == 0
    assert build["process_group_drained"] and "--release" in build["command"]
    assert "--no-run" in build["command"] and "release-user-surfaces" in build["command"]
    build_log = args.build_receipt.with_suffix(".log")
    assert identity(build_log)["sha256"] == build["log_sha256"]
    assert f"Executable unittests src/lib.rs ({binary})" in build_log.read_text()
    assert subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO,
                                   text=True).strip() == build["head"]
    changed = build["changed_source_inventory"]
    for name, record in changed.items():
        if name.endswith((".rs", "Cargo.toml", "Cargo.lock")):
            current = identity(REPO / name)
            assert current["sha256"] == record["sha256"]
            assert current["generation"]["bytes"] == record["bytes"]
    # Listing tests cannot execute a query and proves the frozen binary contains
    # the intended entry point before admitting any observation.
    listing = subprocess.run([str(binary), "--list", "--ignored"], check=True,
                             capture_output=True, text=True, timeout=30).stdout
    assert f"{TEST}: test" in listing
    before = read(args.baseline / "inputs.json")
    observations = read(args.baseline / "native-observations.json")
    assert observations["status"] == "passed" and observations["complete"]
    assert observations["posthash_verified"] and len(observations["observations"]) == 347
    assert math.fsum(row["clocks"]["engine_execution_seconds"]
                     for row in observations["observations"]) == 12.055132613
    cases = selected_cases(before)
    assert len(before["sources"]["fact"]) == 1
    source = before["sources"]["fact"][0]
    originals = {item["path"]: item for item in before["frozen_files"]}
    inputs = [identity(Path(source))]
    assert inputs[0] == originals[source]
    for case in cases:
        reference = identity(Path(case["reference_path"]))
        assert reference == originals[case["reference_path"]]
        assert reference["sha256"] == case["reference_sha256"]
        inputs.append(reference)
    supervisor = identity(args.supervisor.resolve(strict=True))
    assert supervisor["sha256"] == SUPERVISOR_SHA
    names = set(subprocess.check_output(["git", "ls-files", "-z"], cwd=REPO)
                .decode().strip("\0").split("\0"))
    names.update(subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard", "-z"],
                                        cwd=REPO).decode().strip("\0").split("\0"))
    paths = {REPO / name for name in names if name.endswith(".rs")
             or Path(name).name in {"Cargo.toml", "Cargo.lock"}}
    paths.update((REPO / "scripts").glob("*.py"))
    paths.update((REPO / "python/src/shardloom").glob("*.py"))
    paths.update((REPO / "benchmarks/traditional_analytics/query_surface").glob("*.py"))
    paths.update(REPO / name for name in ("benchmarks/traditional_analytics/resources.py",
        "docs/architecture/native-grouped-cost-attribution-2026-10-31.md"))
    files = inputs + [identity(path) for path in sorted(paths)]
    files.extend([identity(binary), identity(args.build_receipt), identity(build_log), supervisor,
                  identity(args.baseline / "inputs.json"),
                  identity(args.baseline / "native-observations.json")])
    check_budgets(root.parent, root, root / "logs", min_free_bytes=12 * GIB,
                  reserve_bytes=64 * MIB, max_workspace_bytes=100 * GIB, max_log_bytes=192 * MIB)
    root.mkdir()
    (root / "jobs").mkdir()
    for index, case in enumerate(cases):
        job = dict(schema_version="shardloom.grouped_cost_job.v1", source_path=source,
                   case_id=case["case_id"], case_identity=case["case_identity"],
                   **case["diagnostic"])
        path = root / "jobs" / f"{index:02d}.json"
        write(path, job)
        case["job"] = identity(path)
        files.append(case["job"])
    write(root / "inputs.json", dict(schema_version="shardloom.grouped_cost_inputs.v1",
        cases=cases, frozen_files=files, binary=str(binary), supervisor=supervisor,
        build_receipt=str(args.build_receipt), source=source, dataset=before["dataset"],
        source_revision=subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
        modified_build_sources=changed, test=TEST, repeats=1, memory_bytes=2 * GIB,
        max_parallelism=2, python=sys.version, platform=platform.platform(),
        phase_order=PHASES, original_case_order=True, preparation_outside_engine=True,
        scope="48-case instrumented native relational attribution; not a production baseline",
        cache_policy="uncontrolled resident source; no eviction, prefetch or artificial pacing",
        comparison_engine_invoked=False, full43_executed=False))
    print(json.dumps(dict(status="frozen", cases=len(cases), root=str(root))), flush=True)


def validate(case, observation, receipt, log, catalog, *, job=None, reference=None):
    assert receipt["returncode"] == 0 and receipt["process_group_drained"]
    assert receipt["direct_child_reaped"] and not receipt["timed_out"]
    assert receipt["interrupted_signal"] is None and "error" not in receipt
    assert "test result: ok. 1 passed; 0 failed;" in log
    assert observation["schema_version"] == "shardloom.grouped_cost_observation.v1"
    assert observation["job"] == (read(case["job"]["path"]) if job is None else job)
    for name in ("complete_consumption", "native_io_certified"):
        assert observation[name] is True, name
    for name in ("fallback_attempted", "external_engine_invoked"):
        assert observation[name] is False, name
    assert observation["memory_grant_bytes"] == 2 * GIB and observation["max_parallelism"] == 2
    for name in ("final_reserved_bytes", "denied_reservations", "peak_reserved_bytes"):
        assert type(observation[name]) is int
    assert observation["final_reserved_bytes"] == observation["denied_reservations"] == 0
    assert 0 < observation["peak_reserved_bytes"] <= 2 * GIB
    for name in ("end_of_input_observed", "output_ownership_detached"):
        assert observation["source"][name] is True
    phases = observation["phases"]
    assert [phase["phase"] for phase in phases] == list(PHASES)
    for index, phase in enumerate(phases):
        for name in ("exclusive_nanos", "calls"):
            assert type(phase[name]) is int and phase[name] >= 0
        assert phase["engine_classified"] is (index < 9)
        assert phase["calls"] or phase["exclusive_nanos"] == 0
    for index in (0, 1, 2, 3, 4, 7, 8):
        assert phases[index]["calls"] > 0
    engine = observation["engine_execution_ns"]
    assert type(engine) is int and engine > 0
    classified = sum(phase["exclusive_nanos"] for phase in phases[:9])
    assert observation["classified_engine_ns"] == classified <= engine
    assert observation["unclassified_engine_ns"] == engine - classified
    schema = schema_fields(json.dumps(observation["actual_schema"]), "vortex.dtype.serde.v1")
    actual = TypedResult(schema, python_rows(observation["actual_rows"], schema, temporal_objects=False))
    reference = read(case["reference_path"]) if reference is None else reference
    assert reference["status"] == "passed" and reference["comparison_engine_invoked"] is False
    assert reference["case_id"] == case["case_id"] and reference["case_identity"] == case["case_identity"]
    compare(result_from_record(reference["reference"]), actual, catalog[case["case_id"]].contract)
    return result_record(actual)


def parse_observation(log):
    records = [line.split("GROUPED_COST ", 1)[1] for line in log.splitlines()
               if "GROUPED_COST " in line]
    assert len(records) == 1, "exactly one complete native observation required"
    return strict_json(records[0])


def observe(root):
    frozen = read(root / "inputs.json")
    files = frozen["frozen_files"] + [identity(root / "inputs.json")]
    verify(files)
    assert not (root / "observations.json").exists()
    logs = root / "logs"
    logs.mkdir()
    spec = importlib.util.spec_from_file_location("grouped_process_supervisor", frozen["supervisor"]["path"])
    supervisor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(supervisor)
    catalog = {case.id: case for case in corpus(interactions="all", include_pending=False)}
    summary = dict(schema_version="shardloom.grouped_cost_run.v1", status="running",
                   input_manifest=identity(root / "inputs.json"), complete=False, observations=[])
    with BenchmarkGuard(root, binary=Path(frozen["binary"]), memory_gb=2) as guard:
        try:
            for index, case in enumerate(frozen["cases"]):
                # Constant metadata checks only; recursive source inventories
                # and content hashes run before/after the whole query cohort.
                for item in files:
                    if item["path"] in (frozen["source"], frozen["binary"], case["job"]["path"]):
                        assert generation(Path(item["path"])) == item["generation"]
                log_path = logs / f"{index:02d}.log"
                command = [frozen["binary"], TEST, "--exact", "--ignored", "--nocapture", "--test-threads=1"]
                receipt = dict(command=command, job=case["job"], status="running")
                row = dict(case_id=case["case_id"], status="running", diagnostic=case["diagnostic"])
                try:
                    with log_path.open("xb") as stream:
                        code = supervisor.run(command, cwd=root, env=dict(os.environ,
                            SHARDLOOM_GROUPED_COST_JOB=case["job"]["path"], RUST_BACKTRACE="1"),
                            output=stream, timeout=180, evidence=receipt, guard=guard.check,
                            max_output_bytes=8 * MIB)
                    receipt["status"] = "passed" if code == 0 else "failed"
                    log = log_path.read_text()
                    observation = parse_observation(log)
                    actual = validate(case, observation, receipt, log, catalog)
                    row.update(status="passed", observation=observation, actual=actual)
                except BaseException as error:
                    receipt.update(status="failed", diagnostic_error=repr(error))
                    row.update(status="failed", error=repr(error))
                    raise
                finally:
                    if log_path.exists():
                        receipt["log"] = identity(log_path)
                    receipt_path = logs / f"{index:02d}.receipt.json"
                    write(receipt_path, receipt)
                    row["receipt"] = identity(receipt_path)
                    summary["observations"].append(row)
                print(json.dumps(dict(completed=index + 1, total=48, case_id=case["case_id"],
                                      status=row["status"])), flush=True)
            verify(files)
            summary.update(status="passed", complete=True, posthash_verified=True)
        except BaseException as error:
            summary.update(status="failed", error=repr(error))
            raise
        finally:
            summary["resource_samples"] = guard.samples
            write(root / "observations.json", summary)


def replay(root):
    frozen, summary = read(root / "inputs.json"), read(root / "observations.json")
    assert summary["status"] == "passed" and summary["complete"] and summary["posthash_verified"]
    assert summary["input_manifest"] == identity(root / "inputs.json")
    cases, rows = frozen["cases"], summary["observations"]
    assert len(cases) == len(rows) == 48
    wanted = {case["reference_path"] for case in cases}
    verify([item for item in frozen["frozen_files"] if item["path"] in wanted])
    catalog = {case.id: case for case in corpus(interactions="all", include_pending=False)}
    for case, row in zip(cases, rows):
        assert catalog[case["case_id"]].identity == case["case_identity"]
        assert row["case_id"] == case["case_id"] and row["status"] == "passed"
        verify([row["receipt"], case["job"]])
        receipt = read(row["receipt"]["path"])
        verify([receipt["log"]])
        log = Path(receipt["log"]["path"]).read_text()
        observation = parse_observation(log)
        assert observation == row["observation"]
        assert validate(case, observation, receipt, log, catalog) == row["actual"]
    return summarize(rows)


def summarize(rows):
    totals = {name: sum(row["observation"]["phases"][index]["exclusive_nanos"] for row in rows)
              for index, name in enumerate(PHASES[:9])}
    engine = sum(row["observation"]["engine_execution_ns"] for row in rows)
    totals["unclassified_engine"] = engine - sum(totals.values())
    return dict(status="passed", cases=len(rows), checked_rows=sum(len(row["actual"]["rows"]) for row in rows),
                instrumented_engine_seconds=engine / 1e9,
                phases={name: dict(engine_seconds=value / 1e9, fraction=value / engine)
                        for name, value in totals.items()}, production_speedup_claim=False,
                baseline_advanced=False, full43_executed=False)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("freeze", "observe", "replay"))
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--build-receipt", type=Path)
    parser.add_argument("--baseline", type=Path)
    parser.add_argument("--supervisor", type=Path)
    args = parser.parse_args()
    assert args.root.is_absolute()
    if args.stage == "freeze":
        assert all((args.binary, args.build_receipt, args.baseline, args.supervisor))
        freeze(args)
    elif args.stage == "observe":
        observe(args.root)
    else:
        print(json.dumps(replay(args.root), indent=2))


if __name__ == "__main__":
    main()
