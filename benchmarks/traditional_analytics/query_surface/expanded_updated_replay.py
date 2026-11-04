# SPDX-License-Identifier: Apache-2.0
"""Replay the updated complete native cohort against the original portable references."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
import re
import sys
import zlib

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(ROOT / "benchmarks/traditional_analytics")]
from native_workflow_protocol import strict_json
from query_timing_report import query_phase_timings
from query_surface.corpus import corpus
from query_surface.expanded_catalog import build, spaces
from query_surface.expanded_membership_replay import contents
from query_surface.expanded_packet import CASES_PER_PART, PART_COUNT
from query_surface.expanded_replay import (
    bounded, lines, metadata, require, sha, source_json, verify_bindings, verify_transport,
)
from query_surface.expanded_report import raw_files, replay


def validate_capture(index, proofs, members, text, hashes):
    protocol, binding, complete = (proofs[name] for name in ("protocol.json", "binding.json", "complete.json"))
    updated = binding["contract"]["updated"]
    require(
        protocol["status"] == "admitted_before_execution"
        and protocol["expected_attempt_count"] == complete["total"] == complete["attempt_slots"] == 10000
        and protocol["runtime_binding"] == updated == complete["updated"]
        and protocol["design"]["sha256"] == hashes["design.json"]
        and protocol["source"]["sha256"] == hashes["inputs.json"]
        and updated["original_freeze"]["sha256"] == hashes["frozen.json"]
        and updated["prepared"]["sha256"] == hashes["native-preparation/complete.json"]
        and updated["cached_polars"]["sha256"] == hashes["observe-polars/complete.json"]
        and updated["previous_native"]["sha256"] == hashes["observe-native/complete.json"]
        and complete["status"] == "complete_observation" and complete["posthash_verified"]
        and complete["frozen"] == binding["contract"]["frozen"] == updated["original_freeze"]
        and complete["prerequisites"] == binding["contract"]["prerequisites"]
        == {"polars": updated["cached_polars"], "preparation": updated["prepared"]}
        and complete["attempts_per_case"] == updated["attempts_per_case"] == 1
        and complete["requested_repeats"] == updated["warmups"] == updated["alternate_attempts"] == 0
        and binding["contract"]["stage"] == complete["stage"] == "native"
        and complete["binding"]["sha256"] == members["binding.json"]["sha256"],
        "updated capture contract differs",
    )
    worker = proofs["worker-source-verification.json"]
    require(worker["published_frozen_sha256"] == hashes["frozen.json"]
            and worker["complete_harness_count"] == len(worker["harness_sha256"]) == 65
            and worker["harness_sha256"]["benchmarks/traditional_analytics/query_surface/expanded_worker.py"]
            == updated["original_worker"]["sha256"],
            "original worker source binding differs")
    for field, name in (("driver", "driver.py"), ("admission_helper", "admission-helper.py"),
                        ("manifest", "runtime.json"), ("build", "build.json"),
                        ("acceptance", "local-acceptance.json")):
        require(updated[field]["sha256"] == members[name]["sha256"], "unbound capture source " + name)
    guard, isolation = proofs["guard.json"], proofs["isolation.json"]
    require(
        guard["status"] == "passed" and guard["returncode"] == isolation["capture_returncode"] == 0
        and guard["process_group_drained"] and guard["direct_child_reaped"]
        and guard["query_workload"] and guard["working_tree"] == ""
        and guard["head"] == updated["head"] == index["head"]
        and guard["driver_sha256"] == protocol["supervisor"]["sha256"] == members["guard.py"]["sha256"]
        and guard["log_sha256"] == members["guard.log"]["sha256"]
        and isolation["driver_sha256"] == protocol["isolation_driver"]["sha256"] == members["isolation.py"]["sha256"]
        and [row["pid"] for row in isolation["held_task_observers"]] == isolation["resumed_pids"]
        and all(row["scopes"] == ["active_output"] for row in guard["storage_inventory_scopes"]
                if row["child_started"] and not row["forced"]),
        "capture supervision or isolation differs",
    )
    runtime, receipt, acceptance = (proofs[name] for name in ("runtime.json", "build.json", "local-acceptance.json"))
    require(
        runtime["source_revision"] == receipt["head"] == updated["build_source"] == index["build"]
        and runtime["profile"] == "release" and runtime["features"] == ["release-user-surfaces"]
        and runtime["build_receipt_sha256"] == members["build.json"]["sha256"]
        and runtime["source_overlay"] == {"bytes": 0, "sha256": sha(b"")}
        and not runtime["untracked_source_paths"] and not receipt["changed_source_inventory"]
        and receipt["status"] == acceptance["status"] == "passed" and receipt["process_group_drained"]
        and receipt["working_tree"] == "" and acceptance["head"] == index["head"]
        and acceptance["measured_candidate"] == index["build"] and acceptance["runtime_source_equality"]
        and receipt["log_sha256"] == members["build.log"]["sha256"]
        and all(f"Compiling {crate} v1.1.0 ({receipt['cwd']}/{crate})" in text["build.log"]
                for crate in ("shardloom-vortex", "shardloom-cli", "shardloom-python")),
        "native release build differs",
    )
    for kind, product in (("binary", "shardloom"), ("library", "_native.abi3.so")):
        file = updated["runtime"][kind]
        require(file["sha256"] == runtime["products"][product] == runtime["files"][product]["sha256"]
                and file["generation"]["bytes"] == runtime["files"][product]["bytes"],
                "bound native product differs")
    for field, name in (("build", "build.commit"), ("head", "head.commit")):
        raw = text[name].encode()
        commit = hashlib.sha1(f"commit {len(raw)}\0".encode() + raw, usedforsecurity=False).hexdigest()
        require(commit == index[field], "source commit object differs")
    match = proofs["source-match.json"]
    patch_paths = re.findall(r"^diff --git a/(.+?) b/(.+?)$", text["updated.patch"], flags=re.MULTILINE)
    allowed = ("docs/", "benchmarks/", "python/tests/", ".github/")
    production = [p for p in match["changed_paths"]
                  if not p.startswith(allowed) and p != match["test_only_changed_path"]]
    require(
        match["build_source"] == index["build"] and match["requested_head"] == index["head"]
        and match["patch_sha256"] == members["updated.patch"]["sha256"]
        and sorted(path for _, path in patch_paths) == match["changed_paths"]
        and all(old == new for old, new in patch_paths)
        and production == match["production_changed_paths"] == []
        and match["test_only_changed_path"] == updated["test_only_change"]
        == "shardloom-vortex/src/local_primitive_relational_provider_tests.rs"
        and match["public_client_unchanged_from"] == "0155355e7927e6f8a4747daae5a6daf3438e822b",
        "requested release source equivalence differs",
    )
    return updated


def summarize(rows):
    paired = [r for r in rows if r["original"]["status"] == r["updated"]["status"] == "passed"]
    fresh = [r for r in rows if r["updated"]["status"] == "passed"]
    def engine(values, role):
        times = [r[role]["engine_seconds"] for r in values]
        require(all(type(v) in (int, float) and math.isfinite(v) and v >= 0 for v in times),
                "missing or invalid engine clock")
        return math.fsum(times)
    before, after = engine(paired, "original"), engine(paired, "updated")
    directions = Counter(
        "faster" if r["updated"]["engine_seconds"] < r["original"]["engine_seconds"]
        else "slower" if r["updated"]["engine_seconds"] > r["original"]["engine_seconds"]
        else "unchanged" for r in paired
    )
    public_pairs = [r for r in fresh if r["polars"]["status"] == "passed"]
    families = {}
    for family in sorted({r["family"] for r in rows}):
        family_rows = [r for r in rows if r["family"] == family]
        family_pairs = [r for r in paired if r["family"] == family]
        family_fresh = [r for r in fresh if r["family"] == family]
        families[family] = dict(
            cases=len(family_rows), updated_correct=len(family_fresh), matched_cases=len(family_pairs),
            original_matched_engine_seconds=engine(family_pairs, "original"),
            updated_matched_engine_seconds=engine(family_pairs, "updated"),
            updated_engine_seconds=engine(family_fresh, "updated"),
        )
    return dict(
        status="passed_portable_updated_native_replay", query_executions=0, total_cases=len(rows),
        updated_statuses=dict(Counter(r["updated"]["status"] for r in rows)),
        original_statuses=dict(Counter(r["original"]["status"] for r in rows)),
        complete_engine_seconds=engine(fresh, "updated") if len(fresh) == len(rows) else None,
        common_correct=dict(cases=len(paired), original_engine_seconds=before, updated_engine_seconds=after,
                            reduction_percent=100 * (1 - after / before) if before > 0 else None,
                            directions=dict(directions)),
        recovered=[dict(id=r["id"], engine_seconds=r["updated"]["engine_seconds"])
                   for r in rows if r["original"]["status"] != "passed" and r["updated"]["status"] == "passed"],
        new_failures=[r["id"] for r in rows if r["original"]["status"] == "passed" and r["updated"]["status"] != "passed"],
        families=families,
        public_workflow=dict(cases=len(public_pairs),
                             native_seconds=math.fsum(r["updated"]["workflow_seconds"] for r in public_pairs),
                             polars_seconds=math.fsum(r["polars"]["workflow_seconds"] for r in public_pairs)),
    )


def verify_updated(archive, base):
    index = strict_json(bounded(archive / "index.json", 2 << 20))
    original_bytes = bounded(base / "index.json", 2 << 20)
    require(index["format"] == "shardloom.updated_native_10k.v1"
            and index["original_packet_sha256"] == sha(original_bytes)
            and index["counts"] == {"cases": 10000, "parts": PART_COUNT}
            and len(index["parts"]) == PART_COUNT
            and index["verifier_sha256"] == sha(bounded(Path(__file__), 1 << 20)), "packet identity differs")
    text = strict_json(contents(archive, "proofs.json.xz", index["files"]["proofs.json.xz"]))
    require(set(text) == set(index["members"]), "proof inventory differs")
    for name, source in text.items():
        raw = source.encode()
        require(index["members"][name] == {"bytes": len(raw), "sha256": sha(raw)}, "proof bytes differ")
    proofs = {k: strict_json(v) for k, v in text.items() if k.endswith(".json")}
    original = strict_json(original_bytes)
    docs, hashes = metadata(base, original)
    verify_bindings(original, docs, hashes)
    updated = validate_capture(index, proofs, index["members"], text, hashes)
    design, inputs, prepared = docs["design.json"], docs["inputs.json"], docs["native-preparation/complete.json"]
    require(proofs["protocol.json"]["execution"] == design["execution"], "resource or execution policy differs")
    require(len(design["cases"]) == len({r["id"] for r in design["cases"]})
            == len({r["semantic_sql_sha256"] for r in design["cases"]}) == 10000
            and sum("reference" in r for r in design["cases"]) == 347,
            "frozen distinct-request inventory differs")
    current_inputs = dict(inputs, runtime=updated["runtime"])
    originals = {c.id: c for c in corpus(interactions="all", include_pending=False)}
    definitions = {s.name: s for s in spaces()}
    rows = []
    for part in range(PART_COUNT):
        require(index["parts"][part]["rows"] == original["parts"][part]["rows"] == CASES_PER_PART,
                "incomplete case part")
        old_stream = lines(base, f"cases-{part:04d}.jsonl.xz", original["parts"][part], CASES_PER_PART)
        new_stream = lines(archive, f"cases-{part:04d}.jsonl.xz", index["parts"][part], CASES_PER_PART)
        for prior, current in zip(old_stream, new_stream, strict=True):
            seq = len(rows)
            spec = design["cases"][seq]
            require(current.keys() == {"seq", "id", "native"}
                    and prior["seq"] == current["seq"] == seq and prior["id"] == current["id"] == spec["id"],
                    "updated case order or request inventory differs")
            item = build(spec, definitions) if "cell" in spec else originals[spec["id"]]
            expected = prior["expected"]
            source_json(expected, "value")
            require(item.identity == spec["case_identity"] == expected["identity"], "independent request differs")
            observed = {}
            for role, entry, config, stage in (
                ("original", prior["native"], inputs, "native"),
                ("polars", prior["polars"], inputs, "polars"),
                ("updated", current["native"], current_inputs, "native"),
            ):
                raw = source_json(entry, "packet")
                verify_transport(entry["packet"], spec, stage)
                blob = zlib.compress(raw, 0) if raw is not None else None
                observed[role] = replay((seq, item.id, entry["status"], blob), spec, stage,
                                       expected["value"], item, config, prepared, design["execution"])
                if stage == "native" and observed[role]["engine_seconds"] is not None:
                    response = strict_json(raw_files(entry["packet"])["one.stdout.json"])
                    report = response["observation"]
                    clocks = query_phase_timings(report["fields"], report["clocks"]["complete_workflow_seconds"])
                    require(clocks["engine_execution_seconds"] == observed[role]["engine_seconds"]
                            and clocks["result_serialization_seconds"] == report["clocks"]["result_serialization_seconds"],
                            "engine clock differs from its native nanosecond counters")
            rows.append(dict(id=item.id, family=spec["family"], **observed))
    result = summarize(rows)
    local = proofs["local-replay.json"]
    require(result["updated_statuses"] == proofs["complete.json"]["outcomes"]
            == local["counts"]["updated_native_statuses"]
            and result["original_statuses"] == local["counts"]["old_native_statuses"]
            and result["total_cases"] == local["count"] == 10000
            and len(result["recovered"]) == local["counts"]["recovered_cases"]
            and len(result["new_failures"]) == local["counts"]["new_correctness_losses"]
            and result["common_correct"]["cases"] == local["counts"]["common_correct_engine_denominator"],
            "portable and local replay denominators differ")
    for actual, expected in (
        (result["complete_engine_seconds"], local["common_correct_engine_comparison"]["updated_all_10000_engine_seconds"]),
        (result["common_correct"]["original_engine_seconds"], local["common_correct_engine_comparison"]["original_engine_seconds"]),
        (result["common_correct"]["updated_engine_seconds"], local["common_correct_engine_comparison"]["updated_engine_seconds"]),
        (result["public_workflow"]["native_seconds"], local["updated_native_public_workflow_clock"]["sum_workflow_seconds"]),
        (result["public_workflow"]["polars_seconds"], local["polars_public_workflow_clock"]["sum_workflow_seconds"]),
    ):
        require(math.isclose(actual, expected, rel_tol=0, abs_tol=1e-7), "replayed clock totals differ")
    if "result_sha256" in index:
        raw = bounded(archive / "results.json", 2 << 20)
        require(sha(raw) == index["result_sha256"] and strict_json(raw) == result, "published results differ")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify_updated(args.archive.resolve(), args.baseline.resolve())
    if args.output is not None:
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
    else:
        require("result_sha256" in strict_json(bounded(args.archive / "index.json", 2 << 20)),
                "published result is not sealed")
    print(json.dumps({k: result[k] for k in ("status", "total_cases", "updated_statuses", "complete_engine_seconds")}))


if __name__ == "__main__":
    main()
