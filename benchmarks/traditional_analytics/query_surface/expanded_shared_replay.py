# SPDX-License-Identifier: Apache-2.0
"""Replay the shared-work 10k capture and its immediately preceding native baseline."""

import argparse
import json
from pathlib import Path
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
from query_surface.expanded_shared_proof import validate_capture
from query_surface.expanded_shared_investigations import validate_public_models, validate_reads
from query_surface.expanded_shared_analysis import validate_analysis
from query_surface.expanded_updated_replay import summarize, validate_capture as validate_previous


def read_proofs(archive, index):
    text = strict_json(contents(archive, "proofs.json.xz", index["files"]["proofs.json.xz"]))
    require(set(text) == set(index["members"]) and len(text) <= 512, "proof inventory differs")
    for name, source in text.items():
        raw = source.encode()
        require(index["members"][name] == {"bytes": len(raw), "sha256": sha(raw)}, "proof bytes differ")
    return {k: strict_json(v) for k, v in text.items() if k.endswith(".json")}, text


def intake(archive, base, previous):
    index = strict_json(bounded(archive / "index.json", 2 << 20))
    base_raw, previous_raw = (bounded(root / "index.json", 2 << 20) for root in (base, previous))
    require(index["format"] == "shardloom.shared_native_10k.v1"
            and index["original_packet_sha256"] == sha(base_raw)
            and index["previous_packet_sha256"] == sha(previous_raw)
            and index["counts"] == {"cases": 10000, "parts": PART_COUNT}
            and len(index["parts"]) == PART_COUNT and index["native_only"] is True,
            "shared packet identity differs")
    for name in ("expanded_shared_replay.py", "expanded_shared_proof.py",
                 "expanded_shared_investigations.py", "expanded_shared_trace.py", "expanded_shared_analysis.py"):
        require(index["verifiers"][name] == sha(bounded(Path(__file__).with_name(name), 1 << 20)),
                "shared verifier source differs")
    original, prior = strict_json(base_raw), strict_json(previous_raw)
    require(prior["format"] == "shardloom.updated_native_10k.v1"
            and prior["original_packet_sha256"] == sha(base_raw)
            and prior["counts"] == {"cases": 10000, "parts": PART_COUNT}
            and len(prior["parts"]) == PART_COUNT
            and prior["verifier_sha256"]
            == sha(bounded(Path(__file__).with_name("expanded_updated_replay.py"), 1 << 20)),
            "previous native packet identity differs")
    docs, hashes = metadata(base, original)
    verify_bindings(original, docs, hashes)
    previous_proofs, previous_text = read_proofs(previous, prior)
    previous_runtime = validate_previous(prior, previous_proofs, prior["members"], previous_text, hashes)
    proofs, text = read_proofs(archive, index)
    require(bounded(archive / "analysis.json", 2 << 20) == text["analysis.json"].encode(),
            "published request-shape analysis differs")
    runtime = validate_capture(index, proofs, index["members"], text, hashes, prior["members"])
    validate_public_models(proofs)
    previous_results_raw = bounded(previous / "results.json", 2 << 20)
    require(sha(previous_results_raw) == prior["result_sha256"], "previous published result differs")
    require(proofs["protocol.json"]["execution"] == docs["design.json"]["execution"],
            "resource or execution policy differs")
    return index, original, prior, docs, proofs, text, previous_runtime, runtime, strict_json(previous_results_raw)


def validate_result(result, local, previous):
    for field in ("total_cases", "updated_statuses", "original_statuses", "complete_engine_seconds",
                  "common_correct", "recovered", "new_failures", "families", "public_workflow"):
        require(result[field] == local[field], "portable and local shared replay differ: " + field)
    require(result["original_statuses"] == previous["updated_statuses"] == {"passed": 10000}
            and result["updated_statuses"] == {"passed": 10000}
            and result["total_cases"] == result["common_correct"]["cases"] == 10000
            and result["common_correct"]["original_engine_seconds"] == previous["complete_engine_seconds"],
            "previous native result or complete denominator differs")
    for family, values in result["families"].items():
        require(values["original_matched_engine_seconds"] == previous["families"][family]["updated_engine_seconds"],
                "previous native family totals differ")


def verify(archive, base, previous):
    index, original, prior_index, docs, proofs, text, previous_runtime, runtime, previous_result = intake(archive, base, previous)
    read_observations = validate_reads(base, original, docs, proofs, index["members"], text)
    design, inputs, prepared = (docs[k] for k in ("design.json", "inputs.json", "native-preparation/complete.json"))
    require(len(design["cases"]) == len({r["id"] for r in design["cases"]})
            == len({r["semantic_sql_sha256"] for r in design["cases"]}) == 10000
            and sum("reference" in r for r in design["cases"]) == 347,
            "frozen distinct-request inventory differs")
    previous_inputs = dict(inputs, runtime=previous_runtime["runtime"])
    current_inputs = dict(inputs, runtime=runtime["runtime"])
    originals = {c.id: c for c in corpus(interactions="all", include_pending=False)}
    definitions = {s.name: s for s in spaces()}
    rows = []
    for part in range(PART_COUNT):
        require(index["parts"][part]["rows"] == original["parts"][part]["rows"]
                == prior_index["parts"][part]["rows"] == CASES_PER_PART, "incomplete case part")
        streams = [lines(root, f"cases-{part:04d}.jsonl.xz", idx["parts"][part], CASES_PER_PART)
                   for root, idx in ((base, original), (previous, prior_index), (archive, index))]
        for reference, prior, current in zip(*streams, strict=True):
            seq = len(rows)
            spec = design["cases"][seq]
            require(current.keys() == prior.keys() == {"seq", "id", "native"}
                    and reference["seq"] == prior["seq"] == current["seq"] == seq
                    and reference["id"] == prior["id"] == current["id"] == spec["id"],
                    "shared case order or request inventory differs")
            item = build(spec, definitions) if "cell" in spec else originals[spec["id"]]
            expected = reference["expected"]
            source_json(expected, "value")
            require(item.identity == spec["case_identity"] == expected["identity"], "independent request differs")
            observed = {}
            for role, entry, config, stage in (
                ("original", prior["native"], previous_inputs, "native"),
                ("updated", current["native"], current_inputs, "native"),
                ("polars", reference["polars"], inputs, "polars"),
            ):
                raw = source_json(entry, "packet")
                verify_transport(entry["packet"], spec, stage)
                blob = zlib.compress(raw, 0) if raw is not None else None
                observed[role] = replay((seq, item.id, entry["status"], blob), spec, stage,
                                       expected["value"], item, config, prepared, design["execution"])
                require(observed[role]["status"] == "passed", "incomplete shared comparison: " + item.id)
                if stage == "native":
                    report = strict_json(raw_files(entry["packet"])["one.stdout.json"])["observation"]
                    clocks = query_phase_timings(report["fields"], report["clocks"]["complete_workflow_seconds"])
                    require(clocks["engine_execution_seconds"] == observed[role]["engine_seconds"]
                            and clocks["result_serialization_seconds"] == report["clocks"]["result_serialization_seconds"],
                            "engine clock differs from native nanosecond counters")
            rows.append(dict(id=item.id, family=spec["family"], **observed))
    result = summarize(rows)
    validate_result(result, proofs["local-replay.json"], previous_result)
    validate_analysis(proofs["analysis.json"], rows, design, inputs)
    result["status"] = "passed_portable_shared_native_replay"
    result["source_commit"] = index["head"]
    result["previous_source_commit"] = prior_index["build"]
    result["retained_combined_candidate"] = result["complete_engine_seconds"] < result["common_correct"]["original_engine_seconds"]
    result["read_ownership_observations"] = read_observations
    result["public_model_prototype"] = "dropped_after_two_observed_introspection_failures"
    result["request_shape_analysis_verified"] = True
    if "result_sha256" in index:
        raw = bounded(archive / "results.json", 2 << 20)
        require(sha(raw) == index["result_sha256"] and strict_json(raw) == result, "published shared results differ")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--previous", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify(args.archive.resolve(), args.baseline.resolve(), args.previous.resolve())
    if args.output is not None:
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2, allow_nan=False)
            stream.write("\n")
    else:
        require("result_sha256" in strict_json(bounded(args.archive / "index.json", 2 << 20)), "published result is not sealed")
    print(json.dumps({k: result[k] for k in ("status", "total_cases", "updated_statuses", "complete_engine_seconds")}))


if __name__ == "__main__":
    main()
