# SPDX-License-Identifier: Apache-2.0
"""Supervised ShardLoom-only corpus run with immutable inputs and observations."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import shutil
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "python" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from local_uat_storage import require_local_path
from native_workflow_protocol import strict_json
from resources import BenchmarkGuard
from run_clickbench_query_uat import run_profiled_command
from query_surface.corpus import aggregate_cells, corpus
from query_surface.coverage import coverage_record, covering_selection
from query_surface.datasets import DatasetSpec, GENERATOR_VERSION, LAYOUTS, file_hash, write_dataset
from query_surface.evidence import generation, identity, preparation_fields, validate_dataset, validate_prepared, worker_outcome
from query_surface.report import markdown, summarize
from query_surface.specification import SURFACES, identifier


def write_json(path, data):
    with path.open("x") as stream:
        json.dump(data, stream, indent=2, allow_nan=False)
        stream.write("\n")


def supervise(command, prefix, timeout, guard, *, job_sha256=None):
    observation = run_profiled_command(command, prefix, timeout, guard.check)
    stdout, stderr = prefix.with_suffix(".stdout.json"), prefix.with_suffix(".stderr.txt")
    receipt = {**observation, "command": command, "stdout": str(stdout), "stderr": str(stderr),
               "stdout_sha256": file_hash(stdout), "stderr_sha256": file_hash(stderr)}
    if job_sha256 is not None:
        receipt["job_sha256"] = job_sha256
    write_json(prefix.with_suffix(".receipt.json"), receipt)
    return receipt


def worker(job, prefix, timeout, guard):
    job_path = prefix.with_suffix(".job.json")
    write_json(job_path, job)
    receipt = supervise([sys.executable, "-B", str(Path(__file__).with_name("worker.py")), "--job", str(job_path)],
                         prefix, timeout, guard, job_sha256=file_hash(job_path))
    return worker_outcome(receipt, Path(receipt["stdout"]).read_bytes()), receipt


def prepare(dataset, manifest, root, logs, args, guard):
    root.mkdir()
    sources, inputs, receipts = {}, [], []
    for table, record in manifest["tables"].items():
        sources[table] = []
        for index, member in enumerate(record["files"]):
            source = dataset / member["path"]
            destination = root / f"{table}-{index:05d}.vortex"
            prefix = logs / f"prepare-{table}-{index:05d}"
            print(json.dumps({"stage": "native_input_preparation", "table": table, "part": index,
                              "rows": member["rows"]}), flush=True)
            command = [str(args.binary), "vortex-prepare", str(source), str(destination),
                       "--input-format", "arrow-ipc", "--memory-gb", str(args.memory_gb),
                       "--max-parallelism", str(args.parallelism), "--format", "json"]
            receipt = supervise(command, prefix, args.preparation_timeout, guard)
            envelope = strict_json(Path(receipt["stdout"]).read_text()) if receipt["returncode"] == 0 else {}
            if receipt["guard_failures"]:
                raise RuntimeError(f"native preparation failed; retained receipt: {prefix}")
            preparation_fields(envelope)
            readback_job = {"kind": "readback", "source_paths": [str(source)], "native_path": str(destination),
                            "library": str(args.library), "memory_bytes": args.memory_gb << 30, "parallelism": args.parallelism}
            outcome, checked = worker(readback_job, logs / f"readback-{table}-{index:05d}", args.preparation_timeout, guard)
            if outcome["status"] != "passed" or outcome["checked_rows"] != member["rows"]:
                raise RuntimeError(f"native input readback failed; retained receipt: {checked['stdout']}")
            sources[table].append(str(destination))
            inputs.append(identity(destination))
            receipts.append({"prepare": str(prefix.with_suffix(".receipt.json")), "readback": checked["stdout"]})
            print(json.dumps({"stage": "native_input_readback_passed", "table": table, "part": index,
                              "rows": outcome["checked_rows"]}), flush=True)
    record = {"logical_identity": manifest["logical_identity"], "sources": sources, "inputs": inputs,
              "receipts": receipts, "complete_independent_readback": True}
    write_json(root / "prepared.json", record)
    return record


def observe(args):
    root = require_local_path(args.root, Path.home(), sys.platform)
    run_root = root / identifier(args.run_id)
    if run_root.exists():
        raise ValueError("run identity already exists; observations are immutable")
    for name in ("binary", "library"):
        path = getattr(args, name).resolve(strict=True)
        setattr(args, name, path)
    surfaces = tuple(args.surfaces.split(","))
    if not surfaces or len(set(surfaces)) != len(surfaces) or set(surfaces) - set(SURFACES):
        raise ValueError("select unique real public surfaces")
    with BenchmarkGuard(root, binary=args.binary, memory_gb=args.memory_gb) as guard:
        run_root.mkdir()
        args._owned_run_root = run_root
        logs = root / "logs" / args.run_id
        logs.mkdir(parents=True, exist_ok=False)
        frozen_runtime = [identity(args.binary), identity(args.library)]
        if args.dataset is None:
            spec = DatasetSpec(seed=args.seed, fact_rows=args.rows, entity_rows=max(1, args.rows // 10),
                product_rows=max(1, args.rows // 100), relationship_rows=args.rows // 5,
                calendar_rows=min(731, max(31, args.rows)), profile=args.profile)
            dataset = run_root / "dataset"
            write_dataset(dataset, spec, args.layout, guard=guard.check,
                          progress=lambda event: print(json.dumps(event), flush=True))
        else:
            dataset = require_local_path(args.dataset, Path.home(), sys.platform)
        spec, manifest, original_inputs = validate_dataset(dataset)
        if args.prepared is None:
            prepared = prepare(dataset, manifest, run_root / "prepared", logs, args, guard)
            prepared_path = run_root / "prepared" / "prepared.json"
        else:
            prepared_path = args.prepared.resolve(strict=True)
        prepared, preparation_evidence = validate_prepared(prepared_path, dataset, manifest)
        frozen = frozen_runtime + original_inputs + prepared["inputs"] + preparation_evidence
        cases = corpus(interactions=args.interactions)
        selected = [case for case in cases if case.relation is not None
                    and (args.partition == "all" or case.partition == args.partition)]
        if args.cases:
            requested = set(args.cases.split(","))
            if requested - {case.id for case in selected}:
                raise ValueError("requested case is absent, proposed or outside the selected partition")
            selected = [case for case in selected if case.id in requested]
        if not selected:
            raise ValueError("the selected cohort is empty")
        provenance = {"schema_version": 1, "run_id": args.run_id, "dataset": str(dataset),
            "prepared_manifest": str(prepared_path),
            "logical_identity": spec.identity, "dataset_spec": asdict(spec), "layout": manifest["layout"],
            "runtime_and_inputs": frozen, "python": sys.version, "platform": platform.platform(),
            "source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "harness_sources": [identity(path) for path in sorted(Path(__file__).parent.glob("*.py"))],
            "python_sources": [identity(path) for path in sorted((ROOT / "python" / "src" / "shardloom").rglob("*.py"))],
            "memory_bytes": args.memory_gb << 30, "parallelism": args.parallelism, "timeout": args.timeout,
            "cache_context": "input generation/preparation/prehash occurred; filesystem cache uncontrolled",
            "execution_mode": "first_query_fresh_session", "repeats": 1, "surfaces": list(surfaces),
            "selected_cases": [case.id for case in selected], "partition": args.partition,
            "comparison_engine_invoked": False,
            "aggregate_factor_coverage": coverage_record(aggregate_cells(),
                covering_selection(aggregate_cells()) if args.interactions == "pairwise" else aggregate_cells())}
        snapshot = run_root / "harness"
        snapshot.mkdir()
        for item in provenance["harness_sources"]:
            source = Path(item["path"])
            if generation(source) != item["generation"]:
                raise ValueError("harness source changed before its immutable snapshot")
            shutil.copyfile(source, snapshot / source.name)
            if file_hash(snapshot / source.name) != item["sha256"]:
                raise ValueError("harness snapshot differs from the executed source")
        provenance["harness_snapshot"] = str(snapshot)
        python_snapshot = run_root / "python-source"
        python_snapshot.mkdir()
        for item in provenance["python_sources"]:
            source = Path(item["path"])
            target = python_snapshot / source.relative_to(ROOT / "python" / "src")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
            if file_hash(target) != item["sha256"]:
                raise ValueError("Python source snapshot differs from the executed source")
        provenance["python_snapshot"] = str(python_snapshot)
        frozen.extend(provenance["harness_sources"] + provenance["python_sources"])
        write_json(run_root / "provenance.json", provenance)
        write_json(run_root / "corpus.json", [case.record() for case in cases])
        observations = []
        def check_generations():
            for item in frozen:
                if generation(Path(item["path"])) != item["generation"]:
                    raise ValueError(f"frozen source/runtime changed: {item['path']}")
        for index, case in enumerate(selected):
            check_generations()
            base = {"case_id": case.id, "case_identity": case.identity, "dataset": asdict(spec),
                    "memory_bytes": args.memory_gb << 30, "parallelism": args.parallelism,
                    "max_output_rows": args.max_output_rows, "timeout": args.timeout}
            reference_prefix = logs / f"{index:04d}-{case.id}-reference"
            print(json.dumps({"stage": "independent_expectation", "case_id": case.id}), flush=True)
            reference, reference_receipt = worker({**base, "kind": "reference"}, reference_prefix, args.reference_timeout, guard)
            for surface in surfaces:
                check_generations()
                if reference["status"] != "passed":
                    record = {"case_id": case.id, "surface": surface, "status": reference["status"],
                              "stage": "independent_expectation", "evidence": reference_receipt["stdout"],
                              "evidence_sha256": reference_receipt["stdout_sha256"],
                              "detail": reference.get("detail")}
                else:
                    print(json.dumps({"stage": "query_execution", "case_id": case.id, "surface": surface}), flush=True)
                    job = {**base, "kind": "execute", "sources": prepared["sources"], "surface": surface,
                           "library": str(args.library), "binary": str(args.binary),
                           "reference_path": reference_receipt["stdout"], "reference_sha256": reference_receipt["stdout_sha256"]}
                    outcome, receipt = worker(job, logs / f"{index:04d}-{case.id}-{surface}", args.timeout + 5, guard)
                    clocks = outcome.get("observation", {}).get("clocks", {})
                    record = {"case_id": case.id, "surface": surface, "status": outcome["status"],
                              "evidence": receipt["stdout"], "evidence_sha256": receipt["stdout_sha256"],
                              "engine_execution_seconds": clocks.get("engine_execution_seconds"),
                              "peak_rss_bytes": receipt.get("native_peak_rss_bytes"),
                              "diagnostics": outcome.get("observation", {}).get("diagnostics"),
                              "detail": outcome.get("detail")}
                check_generations()
                observations.append(record)
                with (run_root / "observations.jsonl").open("a") as stream:
                    stream.write(json.dumps(record, allow_nan=False) + "\n")
                # A storage/RSS refusal terminates the cohort. A native timeout
                # is recorded and the next fresh worker may still be evaluated.
                if record["status"] == "guard_failed":
                    raise RuntimeError("guard stopped the cohort; original evidence retained")
            print(json.dumps({"completed_cases": index + 1, "selected_cases": len(selected), "case_id": case.id,
                              "outcomes": [row["status"] for row in observations[-len(surfaces):]]}), flush=True)
        check_generations()
        for item in frozen:
            if file_hash(Path(item["path"])) != item["sha256"]:
                raise ValueError("frozen source/runtime digest changed after the cohort")
        summary = summarize(cases, observations, surfaces)
        summary.update(completed_selected_cohort=True, selected_cases=len(selected),
                       native_input_readback=True, source_runtime_posthash_verified=True)
        write_json(run_root / "summary.json", summary)
        (run_root / "report.md").write_text(markdown(summary))
        write_json(run_root / "resource-observations.json", guard.samples)
        failed = any(row["status"] != "passed" for row in observations)
        print(json.dumps({"report": str(run_root / "report.md"), "selected_observations": len(observations),
                          "all_selected_passed": not failed}), flush=True)
        return 1 if failed else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=20261026)
    parser.add_argument("--profile", choices=("balanced", "null_heavy", "skewed", "high_cardinality"), default="balanced")
    parser.add_argument("--layout", choices=LAYOUTS, default="sorted")
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--prepared", type=Path)
    parser.add_argument("--interactions", choices=("pairwise", "all"), default="pairwise")
    parser.add_argument("--partition", choices=("development", "held_out", "all"), default="development")
    parser.add_argument("--surfaces", default=",".join(SURFACES))
    parser.add_argument("--cases")
    parser.add_argument("--memory-gb", type=int, default=2)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--timeout", type=float, default=120)
    parser.add_argument("--reference-timeout", type=float, default=600)
    parser.add_argument("--preparation-timeout", type=float, default=600)
    parser.add_argument("--max-output-rows", type=int, default=100000)
    args = parser.parse_args()
    budgets = (args.memory_gb, args.parallelism, args.timeout, args.reference_timeout, args.preparation_timeout, args.max_output_rows)
    if any(not math.isfinite(value) or value <= 0 for value in budgets):
        parser.error("resource and complete-result budgets must be positive")
    try:
        return observe(args)
    except Exception as error:
        # Retain a classified incomplete report even if preparation or a guard
        # aborts before the first query. Never overwrite an existing run identity.
        candidate = getattr(args, "_owned_run_root", None)
        if candidate is not None and not (candidate / "summary.json").exists() and not (candidate / "failure.json").exists():
            observations = []
            observation_file = candidate / "observations.jsonl"
            if observation_file.exists():
                observations = [strict_json(line) for line in observation_file.read_text().splitlines()]
            summary = summarize(corpus(interactions=args.interactions), observations, tuple(args.surfaces.split(",")))
            summary.update(completed_selected_cohort=False, failure=str(error),
                           source_runtime_posthash_verified=False)
            write_json(candidate / "failure.json", {"exception": type(error).__name__, "detail": str(error)})
            write_json(candidate / "summary.json", summary)
            (candidate / "report.md").write_text(markdown(summary))
        raise


if __name__ == "__main__":
    raise SystemExit(main())
