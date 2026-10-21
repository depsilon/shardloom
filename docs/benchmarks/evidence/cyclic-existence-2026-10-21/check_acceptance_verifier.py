#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Check that the cyclic acceptance packet verifier rejects bounded corruption."""
from __future__ import annotations

import argparse
import base64
import copy
import gzip
import hashlib
import importlib.util
import json
import lzma
import sys
from pathlib import Path


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical(value) -> str:
    return digest(json.dumps(value, sort_keys=True, allow_nan=False).encode())


def require(value, detail: str) -> None:
    if not value:
        raise ValueError(detail)


def load_helpers(repository: Path, verifier_path: Path):
    spec = importlib.util.spec_from_file_location("cyclic_acceptance_packet_verifier_854", verifier_path)
    require(spec is not None and spec.loader is not None, "cannot load sibling packet verifier")
    verifier = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(verifier)
    sys.path[:0] = [str(repository / "scripts"), str(repository),
                    str(repository / "benchmarks/traditional_analytics")]
    from native_workflow_protocol import extract_result, report_fields
    from run_clickbench_query_uat import equivalent
    from benchmarks.traditional_analytics.workloads import WORKLOADS
    from benchmarks.traditional_analytics.comparison import round_float
    return verifier, (equivalent, extract_result, report_fields, WORKLOADS, round_float)


def rejected(label: str, action, labels: list[str]) -> None:
    try:
        action()
    except (ValueError, KeyError):
        labels.append(label)
        return
    raise AssertionError(f"corruption was accepted: {label}")


def change_leaf(value):
    """Return a copy with one JSON value changed, including empty containers."""
    if isinstance(value, dict):
        result = copy.deepcopy(value)
        if not result:
            result["__packet_corruption__"] = True
        else:
            key = next(iter(result))
            result[key] = change_leaf(result[key])
        return result
    if isinstance(value, list):
        result = copy.deepcopy(value)
        if not result:
            result.append({"__packet_corruption__": True})
        else:
            result[0] = change_leaf(result[0])
        return result
    if value is None:
        return "__packet_corruption__"
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, float):
        return value + 1.0
    if isinstance(value, str):
        return value + "__packet_corruption__"
    raise TypeError(f"unsupported JSON result leaf: {type(value).__name__}")


def native_rows(report: dict) -> list[dict]:
    return [row for row in report["records"] if row["engine"] == "shardloom"]


def set_member_raw(evidence, path: str, raw: bytes) -> dict:
    row = evidence.members[path]
    saved = dict(row)
    row["bytes"] = len(raw)
    row["sha256"] = digest(raw)
    row["data"] = raw.decode("utf-8") if row["encoding"] == "utf8" else base64.b64encode(raw).decode("ascii")
    return saved


def set_native_call_field(report: dict, row: dict, field_name: str, mutate) -> str:
    evidence_fields = row["evidence"]
    previous = evidence_fields["benchmark_native_calls"]
    calls = json.loads(previous)
    require(calls and field_name in calls[0], f"native call lacks {field_name}")
    mutate(calls[0])
    evidence_fields["benchmark_native_calls"] = json.dumps(calls)
    return previous


def mutate_envelope(evidence, report: dict, row: dict, mutation: str):
    previous_calls = row["evidence"]["benchmark_native_calls"]
    calls = json.loads(previous_calls)
    require(calls, "native call list is empty")
    path = calls[0]["stdout"]
    stored = evidence.raw(path)
    decoded = gzip.decompress(stored) if str(path).endswith(".gz") else stored
    envelope = json.loads(decoded)
    if mutation == "status":
        envelope["status"] = "error"
    elif mutation == "fallback":
        envelope["fallback"]["attempted"] = True
    else:
        raise ValueError(f"unknown envelope mutation {mutation}")
    changed = json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode()
    stored_changed = gzip.compress(changed, mtime=0) if str(path).endswith(".gz") else changed
    saved_member = set_member_raw(evidence, path, stored_changed)
    calls[0]["stdout_sha256"] = digest(stored_changed)
    row["evidence"]["benchmark_native_calls"] = json.dumps(calls)
    return previous_calls, path, saved_member


def restore_member(evidence, path: str, saved: dict) -> None:
    evidence.members[path].clear()
    evidence.members[path].update(saved)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    repository = args.repository.resolve(strict=True)
    manifest_path = args.manifest.resolve(strict=True)
    verifier_path = Path(__file__).with_name("verify_acceptance.py")
    if not verifier_path.is_file():
        verifier_path = Path(__file__).with_name("verify_cyclic_acceptance_packet_854.py")
    require(verifier_path.is_file(), "sibling packet verifier is missing")
    verifier, helpers = load_helpers(repository, verifier_path)
    manifest = json.loads(manifest_path.read_bytes())
    packet_ref = manifest["packet"]
    packet_path = manifest_path.parent / packet_ref["file"]
    packet_bytes = packet_path.read_bytes()
    require(len(packet_bytes) == packet_ref["bytes"] and digest(packet_bytes) == packet_ref["sha256"],
            "packet identity changed")
    packet = json.loads(lzma.decompress(packet_bytes))
    evidence = verifier.Evidence(packet)

    final_ref = manifest["final_acceptance"]
    final = evidence.read(evidence.check(final_ref))
    perf10_ref = final["perf10"]
    perf10 = evidence.read(evidence.check(perf10_ref))
    accepted_modular_ref = perf10["acceptance"]["modular"]
    accepted_modular = evidence.read(evidence.check(accepted_modular_ref))
    matrix_ref = accepted_modular["matrix"]
    matrix = evidence.read(evidence.check(matrix_ref))
    cohorts = matrix["cohorts"]
    require(cohorts, "modular matrix has no cohorts")
    first = cohorts[0]
    require(digest(evidence.raw(first["report"])) == first["report_sha256"], "first modular report identity changed")
    report = evidence.read(first["report"])
    build = evidence.read(final["build"]["path"])
    records, verified_calls = verifier.verify_record_values(report, evidence, build, helpers)
    require(records > 0 and verified_calls > 0, "positive modular cohort did not verify records and calls")

    labels: list[str] = []
    # Evidence's constructor must reject changed member bytes whose recorded identity is stale.
    member_path = next(name for name, row in packet["members"].items() if row["encoding"] == "utf8" and row["data"])
    altered_members = packet["members"].copy()
    altered_members[member_path] = dict(altered_members[member_path])
    altered_members[member_path]["data"] += "x"
    rejected("altered_member_without_digest_refresh",
             lambda: verifier.Evidence({**packet, "members": altered_members}), labels)

    first_member = evidence.members[first["report"]]
    report_ref = {"path": first["report"], "bytes": first_member["bytes"],
                  "sha256": first_member["sha256"]}
    substituted_ref = dict(report_ref, sha256=("0" if report_ref["sha256"][0] != "0" else "1")
                           + report_ref["sha256"][1:])
    rejected("substituted_file_reference_digest", lambda: evidence.check(substituted_ref), labels)

    original_rows = report["records"]
    report["records"] = original_rows[:-1]
    rejected("omitted_modular_row", lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
    report["records"] = original_rows

    report["records"] = original_rows + [original_rows[0]]
    rejected("duplicated_modular_identity", lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
    report["records"] = original_rows

    native = native_rows(report)[0]
    old_result, old_hash = native["result"], native["result_sha256"]
    native["result"] = change_leaf(old_result)
    native["result_sha256"] = canonical(native["result"])
    rejected("changed_native_complete_result_refreshed_hash",
             lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
    native["result"], native["result_sha256"] = old_result, old_hash

    by_identity = {(row["format"], row["scenario"], row["repeat"]): row
                   for row in report["records"] if row["engine"] == "pandas"}
    pair = next((row for row in native_rows(report)
                 if ("pandas", row["format"], row["scenario"], row["repeat"]) in
                 {(r["engine"], r["format"], r["scenario"], r["repeat"]) for r in report["records"]}), None)
    require(pair is not None, "first cohort has no native/reference row pair")
    refrow = by_identity[(pair["format"], pair["scenario"], pair["repeat"])]
    old_native, old_native_hash = pair["result"], pair["result_sha256"]
    old_ref, old_ref_hash = refrow["result"], refrow["result_sha256"]
    changed = change_leaf(old_ref)
    pair["result"] = copy.deepcopy(changed)
    refrow["result"] = copy.deepcopy(changed)
    pair["result_sha256"] = refrow["result_sha256"] = canonical(changed)
    rejected("changed_reference_and_native_but_original_reference_process_output",
             lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
    pair["result"], pair["result_sha256"] = old_native, old_native_hash
    refrow["result"], refrow["result_sha256"] = old_ref, old_ref_hash

    previous_calls = set_native_call_field(report, native, "binary_sha256",
                                           lambda call: call.__setitem__("binary_sha256", "0" * 64))
    rejected("changed_native_call_binary_hash",
             lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
    native["evidence"]["benchmark_native_calls"] = previous_calls

    for mutation in ("status", "fallback"):
        previous, path, saved_member = mutate_envelope(evidence, report, native, mutation)
        rejected(f"native_envelope_{mutation}_changed_with_refreshed_hashes",
                 lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
        native["evidence"]["benchmark_native_calls"] = previous
        restore_member(evidence, path, saved_member)

    writer = next((row for row in native_rows(report) if helpers[3][row["scenario"]].write_statement is not None), None)
    require(writer is not None, "first cohort has no writer workload for readback corruption")
    previous_readbacks = writer["evidence"]["benchmark_output_validation_calls"]
    readbacks = json.loads(previous_readbacks)
    require(readbacks, "writer workload has no readback to remove")
    writer["evidence"]["benchmark_output_validation_calls"] = json.dumps(readbacks[1:])
    rejected("missing_writer_output_readback",
             lambda: verifier.verify_record_values(report, evidence, build, helpers), labels)
    writer["evidence"]["benchmark_output_validation_calls"] = previous_readbacks

    expected = {
        "altered_member_without_digest_refresh", "substituted_file_reference_digest", "omitted_modular_row",
        "duplicated_modular_identity", "changed_native_complete_result_refreshed_hash",
        "changed_reference_and_native_but_original_reference_process_output",
        "changed_native_call_binary_hash", "native_envelope_status_changed_with_refreshed_hashes",
        "native_envelope_fallback_changed_with_refreshed_hashes", "missing_writer_output_readback",
    }
    require(set(labels) == expected and len(labels) == len(expected), "corruption rejection inventory incomplete")
    result = {"status": "passed", "corruptions_rejected": labels, "corruption_count": len(labels),
              "positive_first_cohort_records": records, "positive_first_cohort_payload_calls": verified_calls,
              "native_execution_performed": False, "engine_build_or_benchmark_performed": False,
              "packet_loaded_once": True, "cohort_deepcopy_count": 0,
              "scope": "one unmodified first modular cohort plus bounded independent corruptions"}
    if args.output:
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2, sort_keys=True)
            stream.write("\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
