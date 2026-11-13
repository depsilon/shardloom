#!/usr/bin/env python3
"""Verify and replay compact saved 347-case evidence without extracting it."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import re
import subprocess
import sys
import tarfile
from collections import Counter
from pathlib import Path


EXPECTED = {
    "complete_comparisons": 347,
    "complete_rows": 12565,
    "baseline_engine_seconds": 11.978234339,
    "candidate_engine_seconds": 12.065545164,
}
MAX_ARCHIVE_BYTES = 64 << 20
SAFE_MEMBER = re.compile(r"(?:files|provenance)/[0-9]{6}\Z")


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def need(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_bundle(bundle: Path) -> tuple[dict, dict[str, bytes], bytes, bytes]:
    manifest_path = bundle / "manifest.json"
    archive_path = bundle / "evidence.tar.gz"
    need(manifest_path.is_file() and not manifest_path.is_symlink(), "manifest is not a regular file")
    need(archive_path.is_file() and not archive_path.is_symlink(), "archive is not a regular file")
    manifest_raw = manifest_path.read_bytes()
    manifest = json.loads(manifest_raw)
    need(manifest.get("schema_version") == "shardloom.compact_347_saved_replay.v1", "manifest schema differs")
    archive = archive_path.read_bytes()
    need(len(archive) <= MAX_ARCHIVE_BYTES, "archive exceeds verifier bound")
    need(manifest.get("archive") == {
        "path": "evidence.tar.gz", "bytes": len(archive), "sha256": sha(archive),
    }, "archive identity differs from manifest")

    expected_entries = manifest.get("members")
    need(isinstance(expected_entries, list), "manifest member list is absent")
    expected_by_name = {}
    evidence_by_path = {}
    for entry in expected_entries:
        name = entry.get("member")
        need(isinstance(name, str) and SAFE_MEMBER.fullmatch(name) is not None, "unsafe manifest member name")
        need(name not in expected_by_name, "duplicate manifest member name")
        need(entry.get("kind") in {"consumed_evidence", "provenance"}, "unknown archive member kind")
        need(type(entry.get("bytes")) is int and entry["bytes"] >= 0, "invalid member length")
        need(isinstance(entry.get("sha256"), str) and re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]),
             "invalid member digest")
        original = entry.get("original_literal_path")
        need(isinstance(original, str) and original, "member lacks original literal path")
        expected_by_name[name] = entry
        if entry["kind"] == "consumed_evidence":
            need(original not in evidence_by_path, "duplicate consumed evidence path")
            evidence_by_path[original] = entry

    actual = {}
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as tar:
        for member in tar:
            need(member.isfile() and not member.issym() and not member.islnk(), "non-regular tar member")
            need(SAFE_MEMBER.fullmatch(member.name) is not None, "unsafe tar member name")
            need(member.name not in actual, "duplicate tar member")
            record = expected_by_name.get(member.name)
            need(record is not None, "tar contains an unlisted member")
            need(member.size == record["bytes"], "tar member byte count differs")
            stream = tar.extractfile(member)
            need(stream is not None, "cannot read tar member")
            data = stream.read(record["bytes"] + 1)
            need(len(data) == record["bytes"], "tar member was truncated or oversized")
            need(sha(data) == record["sha256"], "tar member digest differs")
            actual[member.name] = data
    need(set(actual) == set(expected_by_name), "archive member names/count differ from manifest")
    need(len(actual) == len(expected_entries), "archive member count differs")

    listed_paths = manifest.get("consumed_evidence_paths")
    expected_paths = {
        entry["original_literal_path"]: {
            "original_literal_path": entry["original_literal_path"],
            "member": entry["member"], "bytes": entry["bytes"],
            "sha256": entry["sha256"], "read_calls": entry["read_calls"],
        }
        for entry in expected_entries if entry["kind"] == "consumed_evidence"
    }
    need(isinstance(listed_paths, list), "consumed-path map is absent")
    listed_map = {item.get("original_literal_path"): item for item in listed_paths}
    need(listed_map == expected_paths, "consumed-path map differs from archive members")
    return manifest, actual, archive, manifest_raw


def verify_repo_sources(repo_root: Path, binding: dict) -> dict:
    need(repo_root.is_dir(), "repository root is not a directory")
    checkout_revision = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo_root, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    source_revision = binding.get("source_revision")
    need(isinstance(source_revision, str) and re.fullmatch(r"[0-9a-f]{40}", source_revision),
         "manifest source revision is invalid")
    source_results = []
    for entry in binding.get("files", []):
        relative = entry.get("repo_path")
        need(isinstance(relative, str) and relative, "invalid replay source path")
        path = repo_root / relative
        need(not path.is_symlink() and path.is_file(), "replay source is not a regular file: " + relative)
        raw = path.read_bytes()
        digest_matches = len(raw) == entry.get("bytes") and sha(raw) == entry.get("sha256")
        need(digest_matches, "replay source hash differs: " + relative)
        source_results.append(relative)
    need(bool(source_results), "manifest does not bind any replay sources")
    need(not binding.get("binding_gap"), "manifest records an unresolved replay-source binding gap")
    return {
        "source_revision": source_revision,
        "verification_checkout_revision": checkout_revision,
        "source_file_count": len(source_results),
        "bound_verifier_files_match": True,
        "whole_repository_equivalence_claimed": False,
        "binding_gap": False,
        "equivalence_scope": "Exact byte-length and SHA-256 match for the manifest-listed replay source files only; no whole-repository equivalence claim.",
    }

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, required=True,
                        help="repository root containing the source-bound replay verifier")
    parser.add_argument("--result-out", type=Path,
                        help="optional JSON result path inside this bundle directory")
    args = parser.parse_args()
    sys.dont_write_bytecode = True

    bundle = Path(__file__).resolve().parent
    manifest, members, archive, manifest_raw = read_bundle(bundle)
    source_result = verify_repo_sources(args.repo_root.resolve(), manifest["replay_source_binding"])

    provenance = {item["label"]: item for item in manifest.get("provenance_members", [])}
    for label in ("runtime-binding", "verified-summary-r1", "supervision-receipt",
                  "supervision-log", "replay-receipt", "run_347.py", "supervise_347.py"):
        need(label in provenance, "required provenance member is missing: " + label)
        need(provenance[label]["member"] in members, "required provenance bytes are missing: " + label)
    runtime_binding = json.loads(members[provenance["runtime-binding"]["member"]])
    need(runtime_binding.get("head") == source_result["source_revision"],
         "runtime identity receipt source revision differs")
    need(runtime_binding.get("locally_validated") is True
         and runtime_binding.get("published") is False, "runtime identity scope differs")
    runtime_ids = runtime_binding.get("runtime")
    need(isinstance(runtime_ids, dict) and {"binary", "library"} <= set(runtime_ids),
         "runtime identity receipt lacks binary/library identities")
    for identity in runtime_ids.values():
        need(isinstance(identity, dict) and isinstance(identity.get("path"), str)
             and type(identity.get("bytes")) is int and identity["bytes"] > 0
             and isinstance(identity.get("sha256"), str)
             and re.fullmatch(r"[0-9a-f]{64}", identity["sha256"]),
             "invalid runtime identity")
    summary = json.loads(members[provenance["verified-summary-r1"]["member"]])
    need(summary.get("query_executions") == 0, "verified summary reports query execution")
    need(summary.get("comparison_engine_invoked") is False, "verified summary reports a comparison engine")
    need(manifest.get("runtime_and_input_policy") == {
        "runtime_binding_included_as_identity_only": True,
        "runtime_binaries_copied": False,
        "source_data_copied": False,
        "query_executions": 0,
    }, "runtime/data copy policy differs")

    evidence = {
        path: members[item["member"]]
        for path, item in ((entry["original_literal_path"], entry)
                           for entry in manifest["members"]
                           if entry["kind"] == "consumed_evidence")
    }
    expected_calls = {item["original_literal_path"]: item["read_calls"]
                      for item in manifest["consumed_evidence_paths"]}
    observed_calls: Counter = Counter()

    def raw(literal_path: str) -> bytes:
        need(isinstance(literal_path, str), "replay requested a non-literal path")
        if literal_path not in evidence:
            raise ValueError("replay requested evidence absent from bundle: " + literal_path)
        observed_calls[literal_path] += 1
        return evidence[literal_path]

    repo = args.repo_root.resolve()
    sys.path[:0] = [str(repo / "scripts"), str(repo / "python/src"),
                    str(repo / "benchmarks/traditional_analytics")]
    from parquet_demand_completion_replay import replay

    result = replay(raw, manifest["candidate_root_literal"])
    need(set(observed_calls) == set(evidence), "replay did not consume every evidence member")
    need(dict(observed_calls) == expected_calls, "replay raw-read counts differ from recorded callback")
    need(result.get("status") == "passed", "saved-result replay did not pass")
    need(result.get("comparison_engine_invoked") is False, "saved-result replay invoked a comparison engine")
    for key, expected in EXPECTED.items():
        actual = result.get(key)
        if key.endswith("engine_seconds"):
            need(type(actual) in (int, float) and math.isclose(actual, expected, rel_tol=0, abs_tol=1e-9),
                 "saved-result replay clock differs: " + key)
        else:
            need(actual == expected, "saved-result replay count differs: " + key)

    first = manifest["first_recorded_offline_replay"]
    need(first.get("repeated_reads_verified_identical") is True, "pack callback did not verify repeated reads")
    need(first.get("query_executions") == 0, "pack callback reports query execution")
    verifier_raw = Path(__file__).read_bytes()
    output = {
        "verifier": {"file": "verify.py", "bytes": len(verifier_raw), "sha256": sha(verifier_raw)},
        "status": "passed",
        "verification_kind": "archive members and source-bound offline replay",
        "archive_bytes": len(archive),
        "archive_sha256": sha(archive),
        "manifest_sha256": sha(manifest_raw),
        "member_count": len(members),
        "consumed_evidence_member_count": len(evidence),
        "provenance_member_count": len(provenance),
        "raw_read_calls": sum(observed_calls.values()),
        "unique_requested_paths": len(observed_calls),
        "repeated_reads_verified_identical": True,
        "query_executions": 0,
        "comparison_engine_invoked": False,
        "source_binding": source_result,
        "complete_comparisons": result["complete_comparisons"],
        "complete_rows": result["complete_rows"],
        "baseline_engine_seconds": result["baseline_engine_seconds"],
        "candidate_engine_seconds": result["candidate_engine_seconds"],
    }
    if args.result_out:
        destination = args.result_out.resolve()
        need(destination.is_relative_to(bundle), "result output must stay inside the bundle")
        destination.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
