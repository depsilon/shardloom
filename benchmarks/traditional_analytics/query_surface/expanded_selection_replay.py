# SPDX-License-Identifier: Apache-2.0
"""Replay saved selection/retention requests against the original independent rows.

Archived host paths are identifiers only. No engine, native library, original
host input or packaged Python implementation is loaded or executed.
"""

import argparse
import json
import math
from pathlib import Path
import sys
import zlib

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(ROOT / "benchmarks/traditional_analytics")]
from native_workflow_protocol import strict_json
from shardloom.models import OutputEnvelope
from query_timing_report import query_phase_timings
from query_surface.corpus import corpus
from query_surface.evidence import no_fallback, worker_outcome
from query_surface.expanded_catalog import build, spaces
from query_surface.expanded_membership_replay import contents
from query_surface.expanded_packet import CASES_PER_PART, PART_COUNT
from query_surface.expanded_replay import bounded, lines, metadata, require, sha, source_json, verify_bindings, verify_transport
from query_surface.expanded_report import raw_files, replay
from query_surface.expanded_run import request
from query_surface.semantics import compare, result_from_record
from query_surface.expanded_selection_analysis import checked_clocks, summarize, work_counters
from query_surface.expanded_selection_proof import PROTOCOL, guard, linked, python_core_counts, validate_capture
from query_surface.expanded_shared_replay import read_proofs as previous_proofs

VERIFIERS = ("expanded_selection_replay.py", "expanded_selection_proof.py", "expanded_selection_analysis.py")


def digest(value):
    return sha(json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode())


def read_proofs(archive, index):
    names = index["proof_parts"]
    require(0 < len(names) <= 8 and names == [f"proofs-{part:04d}.json.xz" for part in range(len(names))],
            "invalid proof part inventory")
    text = {}
    for name in names:
        part = strict_json(contents(archive, name, index["files"][name]))
        require(not text.keys() & part.keys(), "duplicate proof member")
        text.update(part)
    require(set(text) == set(index["members"]) and len(text) <= 1024, "proof inventory differs")
    for name, source in text.items():
        require(type(source) is str, "proof source must be text")
        raw = source.encode()
        require(index["members"][name] == {"bytes": len(raw), "sha256": sha(raw)}, "proof bytes differ: " + name)
    fragments = index.get("fragmented_members", {})
    require(len(fragments) <= 8, "too many fragmented receipts")
    for name, record in fragments.items():
        names = record["parts"]
        require(name not in text and 1 < len(names) <= 4
                and names == [name + f".fragment-{part:04d}" for part in range(len(names))],
                "invalid fragmented receipt inventory")
        require(all(len(text[part].encode()) <= 16 << 20 for part in names), "oversized proof fragment")
        raw = "".join(text[part] for part in names).encode()
        require(len(raw) == record["bytes"] and sha(raw) == record["sha256"], "fragmented receipt differs")
        source = index["source_identities"][name]
        require(source["bytes"] == len(raw) and source["sha256"] == sha(raw), "fragmented source identity differs")
        text[name] = raw.decode()
    return {name: strict_json(source) for name, source in text.items() if name.endswith(".json")}, text


def intake(archive, base, previous):
    index = strict_json(bounded(archive / "index.json", 2 << 20))
    base_raw, previous_raw = (bounded(root / "index.json", 2 << 20) for root in (base, previous))
    original, prior = strict_json(base_raw), strict_json(previous_raw)
    require(index["format"] == "shardloom.selection_retention_10k.v1"
            and index["original_packet_sha256"] == prior["original_packet_sha256"] == sha(base_raw)
            and index["previous_packet_sha256"] == sha(previous_raw)
            and index["counts"] == prior["counts"] == {"cases": 10000, "parts": PART_COUNT}
            and len(index["parts"]) == len(prior["parts"]) == PART_COUNT
            and index["native_only"] is True and prior["format"] == "shardloom.shared_native_10k.v1",
            "selection packet or baseline identity differs")
    require(set(index["verifiers"]) == set(VERIFIERS), "verifier inventory differs")
    for name in VERIFIERS:
        require(index["verifiers"][name] == sha(bounded(Path(__file__).with_name(name), 1 << 20)),
                "selection verifier source differs")
    docs, hashes = metadata(base, original)
    verify_bindings(original, docs, hashes)
    proofs, text = read_proofs(archive, index)
    for name in VERIFIERS:
        require(index["members"]["benchmarks/traditional_analytics/query_surface/" + name]["sha256"]
                == index["verifiers"][name], "archived verifier source differs")
    protocol = validate_capture(index, proofs, index["members"], text, hashes)
    previous_docs, _ = previous_proofs(previous, prior)
    previous_runtime = previous_docs["protocol.json"]["runtime_binding"]["runtime"]
    require(protocol["execution"] == docs["design.json"]["execution"], "execution policy differs")
    return index, original, prior, docs, proofs, protocol, previous_runtime


def checked_case(seq, reference, prior, current, spec, item, inputs, prepared, policy, protocol, previous_runtime):
    frozen = protocol["cases"][seq]
    require(current.keys() == prior.keys() == {"seq", "id", "native"}
            and current["seq"] == prior["seq"] == reference["seq"] == frozen["sequence"] == seq
            and current["id"] == prior["id"] == reference["id"] == frozen["id"] == spec["id"],
            "case sequence or request identity differs")
    expected = reference["expected"]
    source_json(expected, "value")
    require(item.identity == spec["case_identity"] == expected["identity"] == frozen["case_identity"]
            and digest(expected["value"]) == frozen["expected_sha256"], "independent expected rows differ")
    row = {key: spec[key] for key in ("id", "family", "profile", "surface")}
    row["raw_file_count"] = 0
    for role, record, runtime in (("baseline", prior, previous_runtime), ("candidate", current, protocol["runtime"])):
        entry = record["native"]
        raw = source_json(entry, "packet")
        require(raw is not None, "native packet is absent")
        verify_transport(entry["packet"], spec, "native")
        observed = replay((seq, item.id, entry["status"], zlib.compress(raw, 0)), spec, "native",
                          expected["value"], item, dict(inputs, runtime=runtime), prepared, policy)
        require(observed["status"] == "passed", "saved result is not correct: " + item.id)
        files = raw_files(entry["packet"])
        report = strict_json(files["one.stdout.json"])["observation"]
        nanos, clocks = checked_clocks(report)
        derived = query_phase_timings(report["fields"], report["clocks"]["complete_workflow_seconds"])
        require(derived["engine_execution_seconds"] == observed["engine_seconds"]
                and derived["result_serialization_seconds"] == report["clocks"]["result_serialization_seconds"],
                "reported engine attribution differs")
        row[role] = {"engine_nanos": nanos, "clocks": clocks, "correct_rows": observed["actual_rows"]}
        if role == "baseline":
            require(nanos == frozen["baseline_engine_nanos"] and report["clocks"] == frozen["baseline_clocks"],
                    "frozen baseline clocks differ from original observations")
        else:
            require(digest(strict_json(files["one.job.json"])) == frozen["request_sha256"],
                    "captured request differs from predeclared request")
            fields = report["fields"]
            require(fields["execution_resource_admission_status"] == "admitted"
                    and int(fields["execution_resource_declared_memory_bytes"]) == policy["native_memory_bytes"]
                    and int(fields["execution_resource_declared_max_parallelism"]) == policy["threads"],
                    "declared execution resources differ")
            row["counters"] = work_counters(fields)
            row["raw_file_count"] = len(files)
    return row


def validate_diagnostics(archive, index, proofs, text, references, inputs, prepared, policy):
    result = {}
    for label, name, count, selected_key, guard_name in (
        ("selection-controls-r1", "selection-controls-r1-protocol.json", 13, "selected", "selection-controls-guard-r1"),
        ("combined-controls-r1", "combined-controls-r1-protocol.json", 24, "selected", "combined-controls-observe-r1"),
        ("combined-controls-r1-profile", "combined-controls-r1-protocol.json", 2, "profile_selected", "combined-collection-profile-r1"),
    ):
        protocol, complete = proofs[name], proofs[label + "/complete.json"]
        selected = protocol[selected_key]
        require(protocol["attempts_per_case"] == complete["attempts_per_case"] == 1
                and protocol["warmups"] == complete["warmups"] == 0
                and complete["cases"] == len(selected) == count and complete["outcomes"] == {"passed": count}
                and complete["posthash_verified"] is True and complete["complete_cohort_acceptance"] is False,
                "diagnostic scope differs")
        linked(complete["protocol"], name, index["members"])
        require(complete["ledger"]["sha256"] == index["ledger_identities"][label]["sha256"],
                "diagnostic ledger identity differs")
        receipt = guard(proofs, index["members"], guard_name)
        require(receipt["query_workload"] is True, "diagnostic workload guard missing")
        member = label + "/attempts.jsonl"
        raw = bounded(archive / member, 16 << 20)
        descriptor = index["files"][member]
        require(raw == text[member].encode() and len(raw) == descriptor["bytes"]
                and sha(raw) == descriptor["sha256"] and descriptor["rows"] == count,
                "diagnostic packet bytes differ")
        records = [strict_json(line) for line in raw.splitlines()]
        require(len(records) == count, "diagnostic attempt count differs")
        nanos = baseline = correct_rows = 0
        for seq, (entry, frozen) in enumerate(zip(records, selected, strict=True)):
            spec, item, expected, base_nanos = references[frozen["id"]]
            require(entry["seq"] == seq and entry["id"] == spec["id"] and entry["status"] == "passed",
                    "diagnostic sequence differs")
            encoded = source_json(entry, "packet")
            verify_transport(entry["packet"], spec, "native")
            config = dict(inputs, runtime=protocol["runtime"])
            if selected_key == "profile_selected":
                files = raw_files(entry["packet"])
                require(strict_json(files["one.job.json"]) == request(spec, "native", config, prepared, expected, policy),
                        "profile request differs")
                response = worker_outcome(entry["packet"]["supervision"], files["one.stdout.json"])
                profile = response["observation"]
                require(response["status"] == "passed" and response["id"] == item.id
                        and response["case_identity"] == item.identity and response["surface"] == spec["surface"]
                        and response["execution_engine"] == "native" and profile["complete_consumption"] is True
                        and response["query_executions"] == response["attempts"] == 1 and response["warmups"] == 0
                        and response["scored_observation"] is False and response["proxies_are_diagnostic_only"] is True
                        and response["probe_installation_excluded"] is True, "instrumented profile contract differs")
                actual = result_from_record(response["actual"])
                compare(result_from_record(expected), actual, item.contract)
                no_fallback(profile["fields"])
                envelope = OutputEnvelope.from_json(profile["raw_report"])
                require(envelope.field_map == profile["fields"] and not envelope.fallback.attempted,
                        "instrumented full report differs")
                attribution = response["attribution"]
                require(all(type(v) is int and v >= 0 for v in attribution["exclusive_category_ns"].values())
                        and sum(attribution["exclusive_category_ns"].values()) == attribution["exclusive_total_ns"],
                        "exclusive profile attribution differs")
                observed = {"status": "passed", "actual_rows": len(actual.rows)}
            else:
                observed = replay((seq, item.id, entry["status"], zlib.compress(encoded, 0)), spec, "native", expected,
                                  item, config, prepared, policy)
            require(observed["status"] == "passed" and frozen["baseline_engine_seconds"] == base_nanos / 1e9,
                    "diagnostic result or baseline differs")
            report = strict_json(raw_files(entry["packet"])["one.stdout.json"])["observation"]
            actual_nanos, _ = checked_clocks(report)
            nanos += actual_nanos
            baseline += base_nanos
            correct_rows += observed["actual_rows"]
        result[label] = dict(cases=count, correct_rows=correct_rows, engine_nanos=nanos,
                             baseline_engine_nanos=baseline, included_in_complete_cohort=False,
                             instrumented=selected_key == "profile_selected")
    return result


def verify(archive, base, previous):
    index, original, prior, docs, proofs, protocol, previous_runtime = intake(archive, base, previous)
    design, inputs, prepared = (docs[name] for name in ("design.json", "inputs.json", "native-preparation/complete.json"))
    require(len(design["cases"]) == len({r["id"] for r in design["cases"]}) == 10000, "incomplete distinct cohort")
    originals = {item.id: item for item in corpus(interactions="all", include_pending=False)}
    definitions = {item.name: item for item in spaces()}
    rows = []
    wanted = {row["id"] for name, key in (("selection-controls-r1-protocol.json", "selected"),
              ("combined-controls-r1-protocol.json", "selected"), ("combined-controls-r1-protocol.json", "profile_selected"))
              for row in proofs[name][key]}
    references = {}
    for part in range(PART_COUNT):
        require(all(idx["parts"][part]["rows"] == CASES_PER_PART for idx in (index, original, prior)),
                "incomplete case part")
        streams = [lines(root, f"cases-{part:04d}.jsonl.xz", idx["parts"][part], CASES_PER_PART)
                   for root, idx in ((base, original), (previous, prior), (archive, index))]
        for reference, old, current in zip(*streams, strict=True):
            seq = len(rows)
            spec = design["cases"][seq]
            item = build(spec, definitions) if "cell" in spec else originals[spec["id"]]
            rows.append(checked_case(seq, reference, old, current, spec, item, inputs, prepared,
                                     design["execution"], protocol, previous_runtime))
            if item.id in wanted:
                references[item.id] = (spec, item, reference["expected"]["value"], rows[-1]["baseline"]["engine_nanos"])
    result = summarize(rows)
    require(result["baseline"]["engine_nanos"] == protocol["baseline_engine_nanos"], "baseline total differs")
    result.update(status="passed_portable_selection_retention_replay", source_commit=index["head"],
                  previous_source_commit=prior["head"], outcomes={"passed": 10000}, new_engine_executions=0,
                  correct_rows=sum(row["candidate"]["correct_rows"] for row in rows),
                  raw_attempt_files=sum(row["raw_file_count"] for row in rows))
    local = proofs["combined-10k-replay-r1.json"]
    require(local["status"] == "passed" and local["cases"] == result["cases"]
            and local["totals"]["engine_execution_total_nanos"] == result["candidate"]["engine_nanos"]
            and local["totals"]["baseline_engine_total_nanos"] == result["baseline"]["engine_nanos"]
            and local["totals"]["complete_rows"] == result["correct_rows"]
            and local["raw_attempt_file_count_verified"] == result["raw_attempt_files"],
            "independent local replay differs")
    for role, key in (("baseline", "baseline_public_clocks_seconds"), ("candidate", "public_clocks_seconds")):
        for name, value in local["totals"][key].items():
            require(math.isclose(float(value), result[role][name], rel_tol=1e-12, abs_tol=1e-9),
                    "independent public clock totals differ")
    require(set(references) == wanted, "diagnostic independent references missing")
    _, text = read_proofs(archive, index)
    result["python_core_verification"] = python_core_counts(proofs, text)
    result["diagnostics"] = validate_diagnostics(archive, index, proofs, text, references, inputs, prepared, design["execution"])
    require(result["retained_combined_candidate"] is False and index["cohort_acceptance"]["disposition"] == "rejected",
            "published disposition contradicts complete observations")
    for role in ("baseline", "candidate"):
        require(index["cohort_acceptance"][role + "_engine_seconds"] == result[role]["engine_seconds"],
                "indexed engine total differs")
    if "result_sha256" in index:
        raw = bounded(archive / "results.json", 2 << 20)
        require(sha(raw) == index["result_sha256"] and strict_json(raw) == result, "published selection results differ")
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
        require("result_sha256" in strict_json(bounded(args.archive / "index.json", 2 << 20)), "results are unsealed")
    print(json.dumps({key: result[key] for key in ("status", "cases", "outcomes", "candidate", "retained_combined_candidate")}))


if __name__ == "__main__":
    main()
