# SPDX-License-Identifier: Apache-2.0
"""Verify a portable 10,000-case packet without the original machine or query engines."""

from __future__ import annotations

import argparse
import base64
from collections import Counter
import hashlib
import lzma
from pathlib import Path, PurePosixPath
import re
import sys
import zlib

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from native_workflow_protocol import strict_json
from query_surface.adapters import relation_sql
from query_surface.corpus import corpus
from query_surface.datasets import SCHEMAS
from query_surface.evidence import preparation_fields
from query_surface.expanded_catalog import build, digest, spaces
from query_surface.expanded_metrics import summarize
from query_surface.expanded_packet import (
    CASES_PER_PART,
    PART_COUNT,
    MAX_PART_BYTES,
    MAX_RECORD_BYTES,
    METADATA_NAMES,
    canonical_blob_bytes,
)
from query_surface.expanded_report import replay
from query_surface.report import STATUSES
from query_surface.run import write_json


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def bounded(path, maximum):
    require(
        path.is_file() and not path.is_symlink() and path.stat().st_size <= maximum,
        "missing or oversized packet file: " + path.name,
    )
    with path.open("rb") as stream:
        data = stream.read(maximum + 1)
    require(len(data) <= maximum, "packet file grew: " + path.name)
    return data


def lines(root, name, info, count):
    # Only caller-generated fixed filenames are opened. Archived host paths
    # are compared as identifiers and never resolved or executed.
    require(info["path"] == name, "unexpected packet path")
    data = bounded(root / name, MAX_PART_BYTES)
    require(
        len(data) == info["compressed_bytes"] and sha(data) == info["sha256"],
        "compressed file changed: " + name,
    )
    limit = info["raw_bytes"]
    require(type(limit) is int and 0 < limit <= MAX_PART_BYTES, "invalid uncompressed size")
    decoder = lzma.LZMADecompressor(format=lzma.FORMAT_XZ, memlimit=96 << 20)
    raw = decoder.decompress(data, max_length=limit + 1)
    require(
        decoder.eof and not decoder.unused_data and len(raw) == limit and raw.endswith(b"\n"),
        "truncated, concatenated or oversized packet stream",
    )
    require(raw.count(b"\n") == count, "wrong record count")
    for line in raw.splitlines():
        require(len(line) <= MAX_RECORD_BYTES, "packet line exceeds its limit")
        yield strict_json(line)


def metadata(root, index):
    info = index["metadata"]
    descriptors = info["records"]
    require(0 < len(descriptors) <= 1024, "unexpected metadata inventory size")
    docs, hashes = {}, {}
    for item, descriptor in zip(
        lines(root, "metadata.jsonl.xz", info, len(descriptors)), descriptors, strict=True
    ):
        name = item["name"]
        require(
            name not in docs and name == descriptor["name"] and item["encoding"] == "base64",
            "ambiguous metadata entry",
        )
        raw = base64.b64decode(item["data"], validate=True)
        hashes[name] = sha(raw)
        require(
            len(raw) == item["bytes"] == descriptor["source_bytes"]
            and hashes[name] == item["sha256"] == descriptor["source_sha256"],
            "metadata content changed: " + name,
        )
        docs[name] = strict_json(raw) if name.endswith(".json") else raw
    require(set(METADATA_NAMES) <= docs.keys(), "missing required metadata")
    return docs, hashes


def verify_bindings(index, docs, hashes):
    design, frozen, inputs = (docs[k + ".json"] for k in ("design", "frozen", "inputs"))
    base = PurePosixPath(frozen["design"]["path"]).parent

    def linked(ref, name):
        require(ref["path"] == str(base / name) and ref["sha256"] == hashes[name], "broken link to " + name)

    source = index["source"]
    for k in ("design", "frozen", "inputs"):
        require(source[k + "_sha256"] == hashes[k + ".json"], "packet source differs")
    require(
        index["stage_order"] == design["execution"]["order"]
        and frozen["status"] == "frozen_before_comparison"
        and source["source_revision"] == frozen["source_revision"]
        and source["harness"] == frozen["harness"],
        "frozen harness or execution order differs",
    )
    linked(frozen["design"], "design.json")
    linked(inputs["design"], "design.json")
    linked(frozen["inputs"], "inputs.json")
    prepared = docs["native-preparation/complete.json"]
    require(prepared["status"] == "passed", "preparation failed")
    linked(prepared["frozen"], "frozen.json")
    linked(prepared["polars_complete"], "observe-polars/complete.json")
    archived = set(METADATA_NAMES)
    for profile in prepared["profiles"].values():
        for ref in profile["receipts"]:
            path = PurePosixPath(ref["path"])
            if path.is_relative_to(base / "native-preparation") and path.suffix in (".txt", ".json"):
                name = str(path.relative_to(base))
                linked(ref, name)
                archived.add(name)
    require(set(docs) == archived, "unreferenced metadata")
    stages = {"observe-polars", "native-preparation", "observe-native", "observe-alternate"}
    profiles = {"reference-" + p for p in inputs["profiles"]}
    require(
        source["stage_receipts"].keys() == stages | profiles
        and index["expected_stage_counts"].keys() == (stages - {"native-preparation"}) | profiles,
        "stage inventory differs",
    )
    for name, hash_ in source["stage_receipts"].items():
        require(hash_ == hashes[name + "/complete.json"], "stage receipt differs")
    for profile, db in source["expected_databases"].items():
        name = f"reference-{profile}/complete.json"
        linked(next(ref for ref in frozen["references"] if ref["path"] == str(base / name)), name)
        require(
            docs[name]["expectations"] == db
            and db in frozen["references"]
            and docs[name]["count"]
            == index["expected_stage_counts"]["reference-" + profile]
            == sum(row["profile"] == profile for row in design["cases"]),
            "expectation identity or count differs",
        )
    require(set(source["expected_databases"]) == set(inputs["profiles"]), "missing reference database")
    for stage in ("polars", "native", "alternate"):
        name = "observe-" + stage
        result = docs[name + "/complete.json"]
        linked(result["frozen"], "frozen.json")
        linked(result["binding"], name + "/binding.json")
        contract = docs[name + "/binding.json"]["contract"]
        require(
            result["stage"] == stage
            and result["total"] == index["expected_stage_counts"][name]
            and set(contract) == {"stage", "prerequisites", "frozen"}
            and all(contract[k] == result[k] for k in contract)
            and result["status"] == "complete_observation"
            and result["posthash_verified"] is True
            and result["attempts_per_case"] == 1
            and result["requested_repeats"] == 0
            and result["ledger"] == source["observed_ledgers"][stage],
            "stage binding differs: " + stage,
        )
        prerequisites = (
            {} if stage == "polars" else {"polars": "observe-polars", "preparation": "native-preparation"}
        )
        if stage == "alternate":
            prerequisites["native"] = "observe-native"
        require(result["prerequisites"].keys() == prerequisites.keys(), "stage sequence differs")
        for k, target in prerequisites.items():
            linked(result["prerequisites"][k], target + "/complete.json")
    local = docs["report/replay.json"]
    require(
        local["status"] == "passed_offline_result_replay"
        and source["report_replay_sha256"] == hashes["report/replay.json"],
        "source replay differs",
    )
    for k, name in (
        ("metrics", "report/metrics.json"),
        ("surrogate_rows", "report/surrogates.json"),
        ("frozen", "frozen.json"),
    ):
        linked(local[k], name)
    # Reconcile the published preparation table to the actual CLI receipt and
    # the independently checked IPC rows for every persisted part.
    parts = docs["report/preparation.json"]["parts"]
    seen = set()
    for part in parts:
        name = str(PurePosixPath(part["receipt"]["path"]).relative_to(base))
        require(name not in seen and name.endswith(".receipt.json"), "duplicate preparation")
        seen.add(name)
        linked(part["receipt"], name)
        receipt = docs[name]
        stdout = str(PurePosixPath(receipt["stdout"]).relative_to(base))
        fields = preparation_fields(docs[stdout])
        readback = docs[name.replace("/prepare-", "/readback-").replace(".receipt.json", ".stdout.json")]
        require(
            receipt["stdout_sha256"] == hashes[stdout]
            and receipt["returncode"] == 0
            and not receipt["guard_failures"]
            and receipt["seconds"] == part["preparation_process_seconds"]
            and readback["status"] == "passed"
            and readback["source_rows"]
            == readback["checked_rows"]
            == part["reopened_rows"]
            == int(fields["reopen_row_count"])
            and all(fields[k] == v for k, v in part["observed_work"].items()),
            "preparation observation differs",
        )
    require(
        len(seen)
        == sum(len(paths) for p in prepared["profiles"].values() for paths in p["sources"].values()),
        "preparation count differs",
    )


def source_json(entry, key):
    value = entry[key]
    if value is None:
        require(entry["json_sha256"] is None and entry["blob_sha256"] is None, "missing original blob")
        return None
    require(re.fullmatch("[0-9a-f]{64}", entry["blob_sha256"]) is not None, "invalid source blob digest")
    raw = canonical_blob_bytes(value)
    require(sha(raw) == entry["json_sha256"], "original SQLite JSON content differs")
    return raw


def verify_transport(packet, row, stage):
    if packet is None:
        return
    supervision = packet["supervision"]
    timing = packet["files"].get("one.timing.json")
    if timing is None:
        require("native timing evidence is missing" in supervision["guard_failures"], "missing child timing")
    else:
        child = strict_json(timing["utf8"])
        for key in (
            "seconds",
            "user_cpu_seconds",
            "system_cpu_seconds",
            "peak_rss_bytes",
            "minor_page_faults",
            "major_page_faults",
            "input_block_operations",
            "output_block_operations",
            "os_counter_scope",
        ):
            if key in child:
                summary_key = "native_peak_rss_bytes" if key == "peak_rss_bytes" else key
                require(supervision.get(summary_key) == child[key], "child observation differs: " + key)
        require(
            child["returncode"] == 0 or "profiled native command failed" in supervision["guard_failures"],
            "unsuccessful child omitted from guard failures",
        )
    if packet["classified_status"] == "passed" or supervision["returncode"] or supervision["guard_failures"]:
        return
    # A supervisor may classify absent or invalid stdout. A classified worker
    # response, including a deadline exception, must name its own request.
    try:
        worker = strict_json(packet["files"]["one.stdout.json"].get("utf8", ""))
    except ValueError:
        return
    if isinstance(worker, dict) and worker.get("status") in STATUSES:
        engine = "polars" if stage == "polars" else "native"
        require(
            worker.get("id") == row["id"]
            and worker.get("case_identity") == row["case_identity"]
            and worker.get("execution_engine") == engine,
            "worker failure belongs to another request",
        )


def verify_packet(root, reports=None):
    index = strict_json(bounded(root / "index.json", 2 << 20))
    require(
        index["format"] == "shardloom-expanded-portable-evidence"
        and index["schema_version"] == 1
        and len(index["parts"]) == PART_COUNT
        and index["counts"] == {"cases": 10000, "alternate_attempts": 12, "parts": PART_COUNT},
        "unexpected packet format or cohort",
    )
    docs, hashes = metadata(root, index)
    verify_bindings(index, docs, hashes)
    if reports is not None:
        for name in ("metrics.json", "preparation.json", "surrogates.json"):
            require(
                sha(bounded(reports / name, MAX_RECORD_BYTES)) == hashes["report/" + name],
                "published report differs: " + name,
            )
        require(
            sha(bounded(reports / "cases.jsonl.gz", MAX_PART_BYTES))
            == docs["report/replay.json"]["case_rows"]["sha256"],
            "published case table differs",
        )
    design, inputs, prepared = (
        docs["design.json"],
        docs["inputs.json"],
        docs["native-preparation/complete.json"],
    )
    cases = design["cases"]
    require(
        design["execution"]["order"]
        == ["polars_once", "explicit_preparation", "native_once", "twelve_alternate_source_routes"]
        and design["execution"]["attempts_per_case_and_route"] == 1
        and design["execution"]["warmups"] == 0,
        "one-pass comparison contract differs",
    )
    require(
        len(cases) == len({r["id"] for r in cases}) == len({r["semantic_sql_sha256"] for r in cases}) == 10000
        and sum("reference" in r for r in cases) == 347
        and index["surrogate_order"] == design["alternate_route_cases"],
        "invalid case inventory",
    )
    originals = {c.id: c for c in corpus(interactions="all", include_pending=False)}
    declarations = {s.name: s for s in spaces()}
    symbolic = {name: [name + ".parquet"] for name in SCHEMAS}
    rows, extra = [], []
    stages = {stage: Counter() for stage in ("polars", "native", "alternate")}
    for n, part in enumerate(index["parts"]):
        require(part["rows"] == CASES_PER_PART, "incomplete part")
        for value in lines(root, f"cases-{n:04d}.jsonl.xz", part, CASES_PER_PART):
            seq = len(rows)
            row = cases[seq]
            require(
                value.keys()
                == {"seq", "id", "expected", "polars", "native"}
                | ({"alternate"} if row["surrogate"] else set()),
                "unexpected or missing stage in case record",
            )
            require(value["seq"] == seq and value["id"] == row["id"], "case order differs")
            item = build(row, declarations) if "cell" in row else originals[row["id"]]
            wanted = value["expected"]
            require(
                item.identity == row["case_identity"] == wanted["identity"]
                and digest(relation_sql(item.relation, symbolic)) == row["semantic_sql_sha256"],
                "logical request differs",
            )
            source_json(wanted, "value")
            observed = {}
            for stage in stages:
                if stage == "alternate" and not row["surrogate"]:
                    require(stage not in value, "undeclared repeat")
                    continue
                entry = value[stage]
                raw = source_json(entry, "packet")
                verify_transport(entry["packet"], row, stage)
                # Feed the existing ledger comparator without depending on the
                # producing machine's choice of equivalent DEFLATE bitstream.
                blob = zlib.compress(raw, 0) if raw is not None else None
                actual = replay(
                    (seq, item.id, entry["status"], blob),
                    row,
                    stage,
                    wanted["value"],
                    item,
                    inputs,
                    prepared,
                    design["execution"],
                )
                stages[stage][actual["status"]] += 1
                observed[stage] = actual
            if row["surrogate"]:
                extra.append(
                    dict(
                        id=item.id,
                        label=row["surrogate"],
                        cell=row["cell"],
                        polars=observed["polars"],
                        native_parquet=observed["native"],
                        native_prepared_vortex=observed["alternate"],
                    )
                )
            rows.append(
                {
                    **row,
                    "polars": observed["polars"],
                    "native": {k: v for k, v in observed["native"].items() if k != "work"},
                }
            )
    require(
        extra == docs["report/surrogates.json"]
        and [r["id"] for r in extra] == design["alternate_route_cases"],
        "alternate observations differ from the report",
    )
    for stage, counts in stages.items():
        done = docs[f"observe-{stage}/complete.json"]
        require(
            dict(counts) == done["outcomes"]
            and sum(counts.values()) == done["attempt_slots"] == done["total"],
            "stage outcome counts differ: " + stage,
        )
    metrics = summarize(rows)
    for stage in ("native", "polars"):
        members = [row[stage] for row in rows]
        metrics[stage + "_accounting"] = dict(
            generated=len(rows),
            attempt_slots=len(members),
            returned_complete_typed_result=sum(v["actual_rows"] is not None for v in members),
            correct=stages[stage]["passed"],
            statuses=dict(stages[stage]),
            interrupted_or_unexecuted=sum(stages[stage][k] for k in ("incomplete", "unexecuted")),
        )
    require(metrics == docs["report/metrics.json"], "recomputed metrics differ")
    return dict(
        status="passed_portable_replay",
        index_sha256=sha((root / "index.json").read_bytes()),
        cases=len(rows),
        outcomes=stages,
        preparation_parts=len(docs["report/preparation.json"]["parts"]),
        query_executions=0,
        external_inputs_reopened=False,
    )


if __name__ == "__main__":
    import json

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--reports", type=Path, help="also verify the published case and summary tables")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = verify_packet(args.packet, args.reports)
    if args.output:
        if args.output.exists():
            parser.error("output already exists")
        write_json(args.output, result)
    print(json.dumps(result))
