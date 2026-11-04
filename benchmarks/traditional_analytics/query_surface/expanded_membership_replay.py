# SPDX-License-Identifier: Apache-2.0
"""Compare the 392 membership follow-up requests to their frozen baseline observations."""

import argparse
from collections import Counter
from contextlib import closing
import hashlib
import json
import lzma
import math
from pathlib import Path
import sqlite3
import sys
import tempfile
import zlib

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from native_workflow_protocol import strict_json
from query_surface.expanded_catalog import build, spaces
from query_surface.expanded_packet import CASES_PER_PART, bounded_unpack
from query_surface.expanded_replay import (
    bounded,
    lines,
    metadata,
    require,
    sha,
    source_json,
    verify_bindings,
    verify_transport,
)
from query_surface.expanded_report import replay
from query_surface.run import write_json

MAX_STREAM = 32 << 20
SHAPES = {"subquery_membership", "subquery_null_aware_anti"}


def contents(root, name, spec):
    require(spec["path"] == name, "unexpected evidence filename")
    data = bounded(root / name, MAX_STREAM)
    require(len(data) == spec["compressed_bytes"] and sha(data) == spec["sha256"], "archive bytes differ")
    size = spec["raw_bytes"]
    require(type(size) is int and 0 < size <= MAX_STREAM, "invalid source size")
    decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=96 << 20)
    raw = decoder.decompress(data, max_length=size + 1)
    require(
        decoder.eof and not decoder.unused_data and len(raw) == size and sha(raw) == spec["raw_sha256"],
        "archived source does not match",
    )
    return raw


def verify_build(proofs, members, freeze):
    match = proofs["source-match.json"]
    source = freeze["runtime_provenance"]["binary_build_source"]
    candidate = match["candidate"]
    require(
        match["status"] == "passed"
        and source == match["baseline"]
        and match["patch_sha256"] == members["candidate.patch"]["sha256"],
        "candidate patch is not bound",
    )
    for label, manifest_key, build_key, commit in (
        ("baseline", "baseline-runtime.json", "baseline-build.json", source),
        ("candidate", "runtime.json", "build.json", candidate),
    ):
        manifest, receipt = proofs[manifest_key], proofs[build_key]
        require(
            manifest["source_revision"] == receipt["head"] == commit
            and receipt["command"] == match["command"]
            and receipt["environment"] == match["environment"]
            and receipt["status"] == "passed"
            and receipt["returncode"] == 0
            and receipt["process_group_drained"] is True
            and manifest["features"] == match["features"] == ["release-user-surfaces"]
            and manifest["profile"] == "release"
            and not manifest["untracked_source_paths"]
            and manifest["source_overlay"]["bytes"] == 0
            and members[manifest_key]["sha256"] == match[label + "_manifest_sha256"]
            and members[build_key]["sha256"]
            == manifest["build_receipt_sha256"]
            == match[label + "_build_sha256"],
            "runtime build provenance differs",
        )
    old, new = (proofs[k]["source_snapshot"] for k in ("baseline-runtime.json", "runtime.json"))
    require(
        set(new) <= old.keys()
        and all(k.startswith("docs/benchmarks/evidence/") for k in old.keys() - new.keys())
        and {k for k in new if new[k] != old[k]} == set(match["changed"]),
        "changed source inventory differs",
    )
    require(
        proofs["tests.json"]["log_sha256"] == members["tests.log"]["sha256"], "native tests are not bound"
    )
    for key in ("tests.json", "run.json", "replay.json"):
        require(
            proofs[key]["status"] == "passed"
            and proofs[key]["returncode"] == 0
            and proofs[key]["process_group_drained"] is True,
            "unsuccessful local verification: " + key,
        )
    return source, candidate


def verify(baseline, followup):
    idx_raw = bounded(baseline / "index.json", 2 << 20)
    idx = strict_json(idx_raw)
    index = strict_json(bounded(followup / "index.json", 2 << 20))
    require(
        index["schema"] == "shardloom.expanded_membership_comparison.v1"
        and index["baseline_index_sha256"] == sha(idx_raw),
        "follow-up uses a different baseline",
    )
    docs, hashes = metadata(baseline, idx)
    verify_bindings(idx, docs, hashes)
    text = strict_json(contents(followup, "proofs.json.xz", index["proofs"]))
    members = index["members"]
    require(members.keys() == text.keys() and len(text) <= 80, "proof inventory differs")
    for name, value in text.items():
        raw = value.encode()
        require(
            len(raw) == members[name]["bytes"] and sha(raw) == members[name]["sha256"],
            "proof differs: " + name,
        )
    proofs = {k: strict_json(v) for k, v in text.items() if k.endswith(".json")}
    source, candidate = verify_build(proofs, members, docs["frozen.json"])
    for name, revision in (("base-commit", source), ("candidate-commit", candidate)):
        raw = text[name].encode()
        require(
            hashlib.sha1(f"commit {len(raw)}\0".encode() + raw, usedforsecurity=False).hexdigest()
            == revision,
            "git commit identity differs",
        )
    require(
        "parent " + source in text["candidate-commit"].splitlines(),
        "candidate parent differs from release source",
    )
    plan, bound, complete, comparison = (
        proofs[n + ".json"] for n in ("protocol", "binding", "complete", "comparison")
    )
    require(
        plan["original"] == source
        and plan["candidate"] == candidate
        and plan["freeze"]["sha256"] == hashes["frozen.json"]
        and plan["primary"]["sha256"] == hashes["observe-native/complete.json"]
        and plan["design"]["sha256"] == hashes["design.json"]
        and plan["report"]["sha256"] == hashes["report/replay.json"],
        "comparison protocol differs from the baseline",
    )
    for key, name in (
        ("build", "runtime.json"),
        ("source_match", "source-match.json"),
        ("driver", "driver.py"),
        ("protocol", "protocol.json"),
    ):
        require(bound[key]["sha256"] == members[name]["sha256"], "follow-up binding differs: " + key)
    runtime = bound["runtime"]
    for key, product in (("binary", "shardloom"), ("library", "_native.abi3.so")):
        require(
            runtime[key]["sha256"] == proofs["runtime.json"]["products"][product]
            and docs["inputs.json"]["runtime"][key]["sha256"]
            == proofs["baseline-runtime.json"]["products"][product],
            "runtime product differs",
        )
    design, prepared, inputs = (
        docs[k] for k in ("design.json", "native-preparation/complete.json", "inputs.json")
    )
    policy = design["execution"]
    cases = design["cases"]
    selected = [r["id"] for r in cases if r["archetype"] in SHAPES]
    require(
        plan["policy"] == policy and len(selected) == 392 and selected == [r["id"] for r in plan["rows"]],
        "comparison did not select both complete frozen archetypes",
    )
    db_raw = contents(followup, "observations.sqlite.xz", index["observations"])
    require(
        complete["ledger"]["sha256"] == sha(db_raw)
        and complete["binding"]["sha256"] == members["binding.json"]["sha256"]
        and complete["total"] == len(selected)
        and complete["attempts_per_request"] == 1
        and complete["warmups"] == 0
        and complete["posthash_verified"] is True
        and comparison["complete"]["sha256"] == members["complete.json"]["sha256"],
        "candidate ledger is not bound",
    )
    counts, checked = Counter(), []
    declarations = {s.name: s for s in spaces()}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "attempts.sqlite"
        path.write_bytes(db_raw)
        with closing(sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)) as db:
            db.execute("PRAGMA trusted_schema=OFF")
            require(
                db.execute("SELECT type FROM sqlite_master WHERE name='attempt'").fetchall() == [("table",)],
                "missing native attempt table",
            )
            attempts = iter(db.execute("SELECT seq,id,status,packet FROM attempt ORDER BY seq"))
            for number, part in enumerate(idx["parts"]):
                start = number * CASES_PER_PART
                if not any(r["archetype"] in SHAPES for r in cases[start : start + CASES_PER_PART]):
                    continue
                for offset, record in enumerate(
                    lines(baseline, f"cases-{number:04d}.jsonl.xz", part, CASES_PER_PART)
                ):
                    row = cases[start + offset]
                    if row["archetype"] not in SHAPES:
                        continue
                    seq = len(checked)
                    require(
                        row["id"] == record["id"] == selected[seq] and record["seq"] == start + offset,
                        "baseline case order differs",
                    )
                    item = build(row, declarations)
                    require(
                        item.identity == record["expected"]["identity"] == row["case_identity"],
                        "intent differs",
                    )
                    source_json(record["expected"], "value")
                    expected = record["expected"]["value"]
                    old = record["native"]
                    raw = source_json(old, "packet")
                    verify_transport(old["packet"], row, "native")
                    old_result = replay(
                        (seq, row["id"], old["status"], zlib.compress(raw, 0)),
                        row,
                        "native",
                        expected,
                        item,
                        inputs,
                        prepared,
                        policy,
                    )
                    attempt = next(attempts)
                    require(attempt[:2] == (seq, row["id"]), "out-of-order follow-up case")
                    followup_packet, _ = bounded_unpack(attempt[3], row["id"])
                    verify_transport(followup_packet, row, "native")
                    result = replay(
                        attempt,
                        row,
                        "native",
                        expected,
                        item,
                        dict(inputs, runtime=runtime),
                        prepared,
                        policy,
                    )
                    reserved = result.pop("work", {}).get(
                        "execution_resource_observed_native_peak_reserved_bytes", "unavailable"
                    )
                    result["reported_peak_reserved_bytes"] = (
                        None if reserved == "unavailable" else int(reserved)
                    )
                    previous = dict(
                        id=row["id"],
                        original_status=old_result["status"],
                        engine_seconds=old_result["engine_seconds"],
                    )
                    require(previous == plan["rows"][seq], "comparison changed the original outcome or clock")
                    checked.append(dict(**previous, candidate=result))
                    counts[result["status"]] += 1
            require(next(attempts, None) is None, "extra candidate attempt")
    paired = [r for r in checked if r["original_status"] == "passed"]
    recovered = [
        r for r in checked if r["original_status"] != "passed" and r["candidate"]["status"] == "passed"
    ]
    old_sum = math.fsum(r["engine_seconds"] for r in paired)
    new_sum = math.fsum(r["candidate"]["engine_seconds"] for r in paired)
    require(
        len(checked) == 392
        and len(paired) == 387
        and len(recovered) == 5
        and counts == {"passed": 392}
        and old_sum == plan["original_passing_sum"] == comparison["original_passing_sum"]
        and new_sum == comparison["candidate_paired_sum"]
        and new_sum < old_sum
        and checked == comparison["rows"]
        and recovered == comparison["candidate_recovered"]
        and comparison["status"] == "passed",
        "comparison acceptance differs",
    )
    require(
        all(
            0 <= r["candidate"]["reported_peak_reserved_bytes"] <= policy["native_memory_bytes"]
            and 0 < r["candidate"]["peak_process_rss_bytes"] <= policy["process_group_rss_ceiling_bytes"]
            for r in checked
        ),
        "observed memory outside the protocol",
    )
    result = dict(
        original_passing=len(paired),
        candidate_passing=len(checked),
        paired=len(paired),
        original_paired_engine_seconds=old_sum,
        candidate_paired_engine_seconds=new_sum,
        recovered=len(recovered),
    )
    require(result == index["result"], "published outcome differs")
    return dict(
        status="passed_portable_membership_comparison",
        baseline_index_sha256=sha(idx_raw),
        followup_index_sha256=sha((followup / "index.json").read_bytes()),
        query_executions=0,
        **result,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = verify(args.baseline, args.candidate)
    if args.output:
        if args.output.exists():
            parser.error("output already exists")
        write_json(args.output, report)
    print(json.dumps(report))
