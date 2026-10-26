# SPDX-License-Identifier: Apache-2.0
"""One external Polars observation per previously frozen native semantic case.

Reuses the native observation without rerunning it. Both complete workflow
clocks include declaration/planning, execution and full Python-row delivery,
including initialization triggered inside these APIs. Explicit pre-clock engine
imports are excluded.
ShardLoom engine clocks are retained separately and never divided by a Polars
API clock. This is an initial prioritization cohort, not a noise estimate.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import math
import os
from pathlib import Path
import platform
import shutil
import statistics
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "python" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from local_uat_storage import require_local_path
from native_workflow_protocol import strict_json
from resources import BenchmarkGuard
from query_surface.corpus import corpus
from query_surface.evidence import generation, identity, no_fallback, validate_dataset, validate_prepared
from query_surface.datasets import file_hash
from query_surface.run import worker, write_json
from query_surface.semantics import compare, result_from_record
from query_surface.specification import identifier


def _positive_clock(value):
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError("comparison requires a positive finite clock at the declared boundary")
    return value


def comparison_record(case_id, family, native_seconds, polars_seconds, *, correct):
    if not correct:
        return {"case_id": case_id, "family": family, "comparable": False,
                "native_over_polars": None, "native_minus_polars_seconds": None}
    native_seconds, polars_seconds = _positive_clock(native_seconds), _positive_clock(polars_seconds)
    return {"case_id": case_id, "family": family, "comparable": True,
        "clock": "complete_workflow_seconds", "shardloom_seconds": native_seconds, "polars_seconds": polars_seconds,
        "native_over_polars": native_seconds / polars_seconds,
        "native_minus_polars_seconds": native_seconds - polars_seconds}


def report(records, *, completed):
    comparable = [row for row in records if row["comparison"]["comparable"]]
    lines = ["# Initial Polars comparison", "",
        "One execution per selected case and engine. Native observations are reused unchanged. "
        "The comparison clock spans lazy declaration through complete Python-row output, excluding explicit pre-clock imports. "
        "Initialization triggered inside the APIs is included. "
        "ShardLoom reads prepared Vortex; Polars reads the identical logical records in original Arrow IPC. "
        "Preparation is excluded. The engines ran in separate sequential cohorts; cache and host drift are uncontrolled. "
        "These observations prioritize investigation and do not establish statistical improvements or superiority.", "",
        f"Cohort complete: {completed}. Comparable correct cases: {len(comparable)}/{len(records)}.", "",
        "Polars exposes logical types but no nonnullable-field contract. Values, names, order and logical types "
        "are checked; missing nullability metadata is explicitly excluded from schema parity.", "",
        "| Case | Status | ShardLoom API s | Polars API s | Ratio | Difference s |", "| --- | --- | ---: | ---: | ---: | ---: |"]
    for row in sorted(records, key=lambda row: -(row["comparison"].get("native_minus_polars_seconds") or 0)):
        pair = row["comparison"]
        metrics = (f"{pair['shardloom_seconds']:.6f} | {pair['polars_seconds']:.6f} | {pair['native_over_polars']:.3f} | "
                   f"{pair['native_minus_polars_seconds']:.6f}" if pair["comparable"] else "— | — | — | —")
        lines.append(f"| {row['case_id']} | {row['status']} | {metrics} |")
    return "\n".join(lines) + "\n"


def family_observations(records):
    """Keep families separate; numerous easy variants cannot dominate a score."""
    groups = {}
    for row in records:
        groups.setdefault(row["family"], []).append(row)
    return {name: {"selected_cases": len(rows), "correct_comparable_cases": len(pairs),
                   "median_case_ratio": statistics.median(p["native_over_polars"] for p in pairs) if pairs else None,
                   "summed_absolute_difference_seconds": sum(p["native_minus_polars_seconds"] for p in pairs) if pairs else None}
            for name, rows in sorted(groups.items())
            for pairs in [[row["comparison"] for row in rows if row["comparison"]["comparable"]]]}


def observe(args):
    native_root = require_local_path(args.native_run, Path.home(), sys.platform)
    root = require_local_path(args.root or native_root.parent, Path.home(), sys.platform)
    target = root / identifier(args.run_id)
    if target.exists():
        raise ValueError("comparison run identity exists; observations are immutable")
    provenance_path, summary_path = native_root / "provenance.json", native_root / "summary.json"
    original = strict_json(provenance_path.read_text())
    summary = strict_json(summary_path.read_text())
    if (summary.get("completed_selected_cohort") is not True or summary.get("source_runtime_posthash_verified") is not True
            or summary.get("native_input_readback") is not True or args.native_surface not in original["surfaces"]):
        raise ValueError("native cohort lacks completed input, result or runtime validation")
    binary = Path(original["runtime_and_inputs"][0]["path"])
    with BenchmarkGuard(root, binary=binary, memory_gb=original["memory_bytes"] >> 30) as guard:
        target.mkdir()
        args._owned_root = target
        logs = root / "logs" / args.run_id
        logs.mkdir(parents=True, exist_ok=False)
        dataset = Path(original["dataset"])
        spec, manifest, frozen = validate_dataset(dataset)
        if spec.identity != original["logical_identity"]:
            raise ValueError("comparison input differs from native input identity")
        sources = {table: [str(dataset / member["path"]) for member in record["files"]]
                   for table, record in manifest["tables"].items()}
        observations_path = native_root / "observations.jsonl"
        native_rows = [strict_json(line) for line in observations_path.read_text().splitlines()
                       if line.strip()]
        selected = {row["case_id"]: row for row in native_rows if row["surface"] == args.native_surface}
        if (len(selected) != sum(row["surface"] == args.native_surface for row in native_rows)
                or set(selected) != set(original["selected_cases"])):
            raise ValueError("native case/surface observation coverage differs")
        catalog = {case.id: case for case in corpus(interactions="all")}
        inputs = []
        for name in original["selected_cases"]:
            case, row = catalog[name], selected[name]
            actual_path = Path(row["evidence"])
            if file_hash(actual_path) != row["evidence_sha256"] or row["status"] != "passed":
                raise ValueError("initial comparison requires intact, correct native observations")
            result = strict_json(actual_path.read_text())
            if result["case_identity"] != case.identity:
                raise ValueError("native case semantic identity changed")
            no_fallback(result["observation"]["fields"])
            job_path = actual_path.with_name(actual_path.name.removesuffix(".stdout.json") + ".job.json")
            job = strict_json(job_path.read_text())
            reference_path = Path(job["reference_path"])
            if file_hash(reference_path) != job["reference_sha256"]:
                raise ValueError("original independent reference changed")
            reference = strict_json(reference_path.read_text())
            if reference["case_identity"] != case.identity or reference["status"] != "passed":
                raise ValueError("reference case identity differs")
            compare(result_from_record(reference["reference"]), result_from_record(result["actual"]), case.contract)
            frozen.extend(identity(path) for path in (actual_path, job_path, reference_path))
            inputs.append((case, result, job))
        # Recheck the persisted full-readback chain without executing native
        # queries. Legacy packets predate the explicit prepared_manifest field;
        # recover its location only from their exact recorded source paths.
        if "prepared_manifest" in original:
            prepared_path = Path(original["prepared_manifest"])
        else:
            parents = {Path(path).parent for paths in inputs[0][2]["sources"].values() for path in paths}
            if len(parents) != 1:
                raise ValueError("legacy native packet has no unambiguous prepared manifest")
            prepared_path = parents.pop() / "prepared.json"
        prepared, receipts = validate_prepared(prepared_path, dataset, manifest)
        if any(job["sources"] != prepared["sources"] for _, _, job in inputs):
            raise ValueError("native requests differ from the validated prepared source mapping")
        frozen.extend(receipts)
        frozen.extend(identity(path) for path in (provenance_path, summary_path, observations_path))
        snapshot = target / "harness"
        snapshot.mkdir()
        for source in sorted(Path(__file__).parent.glob("*.py")):
            item = identity(source)
            shutil.copyfile(source, snapshot / source.name)
            if file_hash(snapshot / source.name) != item["sha256"]:
                raise ValueError("comparison source snapshot differs")
            frozen.append(item)
        packages = {}
        for name in ("polars", "polars-runtime-32", "pyarrow"):
            distribution = importlib.metadata.distribution(name)
            packages[name] = distribution.version
            # Hash the installed package RECORD and native extension payloads.
            for member in distribution.files or ():
                if str(member).endswith((".so", ".dylib", ".dist-info/RECORD")):
                    frozen.append(identity(Path(distribution.locate_file(member)).resolve()))
        write_json(target / "provenance.json", {"schema_version": 1, "run_id": args.run_id,
            "native_run": str(native_root), "native_surface": args.native_surface, "dataset_spec": original["dataset_spec"],
            "logical_identity": spec.identity, "selected_cases": original["selected_cases"], "packages": packages,
            "python": sys.version, "platform": platform.platform(), "parallelism": original["parallelism"],
            "external_rss_guard_bytes": guard.rss_limit, "polars_internal_memory_limit": None,
            "input_format_pair": {"shardloom": "prepared_vortex", "polars": "original_arrow_ipc"},
            "clock": "complete_workflow_seconds", "native_engine_clock_comparable": False,
            "ordering": "native cohort previously completed; one sequential Polars cohort; no rerun or cache manipulation",
            "repeats": 1, "purpose": "initial prioritization; no noise estimate or superiority claim",
            "frozen_files": frozen, "comparison_engine_invoked": "polars_only"})
        def unchanged():
            for item in frozen:
                if generation(Path(item["path"])) != item["generation"]:
                    raise ValueError("comparison source/input/evidence changed during the cohort")
        records = []
        for index, (case, native, original_job) in enumerate(inputs):
            unchanged()
            job = {"kind": "polars", "case_id": case.id, "case_identity": case.identity,
                   "reference_path": original_job["reference_path"], "reference_sha256": original_job["reference_sha256"],
                   "sources": sources, "parallelism": original["parallelism"]}
            outcome, receipt = worker(job, logs / f"{index:04d}-{case.id}-polars", args.timeout, guard)
            pair = comparison_record(case.id, case.family, native["observation"]["clocks"]["complete_workflow_seconds"],
                outcome.get("observation", {}).get("clocks", {}).get("complete_workflow_seconds"), correct=outcome["status"] == "passed")
            row = {"case_id": case.id, "family": case.family, "status": outcome["status"],
                   "evidence": receipt["stdout"], "evidence_sha256": receipt["stdout_sha256"],
                   "comparison": pair, "detail": outcome.get("detail")}
            records.append(row)
            with (target / "observations.jsonl").open("a") as stream:
                stream.write(json.dumps(row, allow_nan=False) + "\n")
            unchanged()
            print(json.dumps({"completed_cases": index + 1, "selected_cases": len(inputs), "case_id": case.id,
                              "status": outcome["status"]}), flush=True)
            if row["status"] == "guard_failed":
                raise ValueError("resource guard stopped comparison; observations retained")
        for item in frozen:
            if file_hash(Path(item["path"])) != item["sha256"]:
                raise ValueError("comparison source/input/evidence digest changed")
        write_json(target / "summary.json", {"completed": True, "posthash_verified": True,
            "selected_cases": len(inputs), "all_passed": all(row["status"] == "passed" for row in records),
            "families": family_observations(records),
            "comparisons": [row["comparison"] for row in records], "comparison_engine_invoked": "polars_only"})
        write_json(target / "resource-observations.json", guard.samples)
        (target / "report.md").write_text(report(records, completed=True))
        return int(any(row["status"] != "passed" for row in records))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--native-run", type=Path, required=True)
    parser.add_argument("--root", type=Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--native-surface", default="python_sql")
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error("timeout must be finite and positive")
    try:
        return observe(args)
    except Exception as error:
        root = getattr(args, "_owned_root", None)
        if root is not None and not (root / "summary.json").exists():
            write_json(root / "summary.json", {"completed": False, "posthash_verified": False,
                       "failure": str(error), "exception": type(error).__name__})
        raise


if __name__ == "__main__":
    raise SystemExit(main())
