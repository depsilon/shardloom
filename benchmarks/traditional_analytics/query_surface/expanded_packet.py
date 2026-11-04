# SPDX-License-Identifier: Apache-2.0
"""Write a portable, lossless packet of an already completed expanded run."""

from __future__ import annotations

import argparse
import base64
from contextlib import ExitStack
import hashlib
import json
import lzma
from pathlib import Path
import sys
import zlib

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "scripts"), str(ROOT / "python/src"), str(Path(__file__).resolve().parents[1])]
from query_surface.expanded_freeze import read
from query_surface.expanded_run import admission, closing_db, stage_complete, verify

CASES_PER_PART = 250
PART_COUNT = 40
MAX_RECORD_BYTES = 16 * 1024 * 1024
MAX_PART_BYTES = 128 * 1024 * 1024
METADATA_NAMES = (
    "design.json",
    "frozen.json",
    "inputs.json",
    "native-preparation/complete.json",
    "reference-holdout/complete.json",
    "reference-primary/complete.json",
    "reference-scale/complete.json",
    "reference-smoke/complete.json",
    "observe-alternate/binding.json",
    "observe-alternate/complete.json",
    "observe-native/binding.json",
    "observe-native/complete.json",
    "observe-polars/binding.json",
    "observe-polars/complete.json",
    "report/metrics.json",
    "report/preparation.json",
    "report/replay.json",
    "report/surrogates.json",
)


def canonical_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def canonical_blob_bytes(value):
    """The byte representation passed to zlib by the original SQLite writer."""
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def bounded_unpack(blob, label):
    decoder = zlib.decompressobj()
    decoded = decoder.decompress(blob, MAX_RECORD_BYTES + 1)
    if len(decoded) > MAX_RECORD_BYTES or decoder.unconsumed_tail:
        raise ValueError(f"unpacked source record exceeds {MAX_RECORD_BYTES} bytes: {label}")
    decoded += decoder.flush(MAX_RECORD_BYTES - len(decoded) + 1)
    if len(decoded) > MAX_RECORD_BYTES or not decoder.eof or decoder.unused_data:
        raise ValueError(f"invalid or oversized packed source record: {label}")
    value = json.loads(decoded)
    if canonical_blob_bytes(value) != decoded:
        raise ValueError(f"source record is not canonical JSON: {label}")
    return value, {
        "blob_sha256": hashlib.sha256(blob).hexdigest(),
        "json_sha256": hashlib.sha256(decoded).hexdigest(),
    }


def digest_file(path):
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
            size += len(block)
    return size, digest.hexdigest()


def source_record(root, name):
    path = root / name
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_RECORD_BYTES:
        raise ValueError(f"metadata record exceeds {MAX_RECORD_BYTES} bytes: {name}")
    data = path.read_bytes()
    return {
        "name": name,
        "encoding": "base64",
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "data": base64.b64encode(data).decode("ascii"),
    }


def attempt_stream(stack, root, stage, complete):
    path = root / f"observe-{stage}/attempts.sqlite"
    if str(path) != complete["ledger"]["path"]:
        raise ValueError(f"unexpected attempt ledger path: {stage}")
    db = stack.enter_context(closing_db(path, readonly=True))
    return iter(db.execute("SELECT seq,id,status,packet FROM attempt ORDER BY seq"))


def next_packet(stream, seq, case_id, stage):
    row = next(stream, None)
    if row is None or row[0] != seq or row[1] != case_id:
        raise ValueError(f"{stage} attempt order differs at sequence {seq}")
    status, blob = row[2], row[3]
    if blob is None:
        if status != "incomplete":
            raise ValueError(f"{stage} terminal observation lacks its original packet: {case_id}")
        return {"status": status, "packet": None, "blob_sha256": None, "json_sha256": None}
    blob = bytes(blob)
    if len(blob) > MAX_RECORD_BYTES:
        raise ValueError(f"{stage} source packet exceeds {MAX_RECORD_BYTES} bytes: {case_id}")
    packet, hashes = bounded_unpack(blob, f"{stage}:{case_id}")
    if packet.get("classified_status") != status:
        raise ValueError(f"{stage} packet status differs from its ledger: {case_id}")
    return {"status": status, "packet": packet, **hashes}


def write_part(output, part_index, cases):
    name = f"cases-{part_index:04d}.jsonl.xz"
    path = output / name
    row_count = 0
    raw_bytes = 0
    with path.open("xb") as raw:
        with lzma.LZMAFile(raw, "wb", format=lzma.FORMAT_XZ, preset=3) as compressed:
            for record in cases:
                line = canonical_json(record) + b"\n"
                if len(line) > MAX_RECORD_BYTES or raw_bytes + len(line) > MAX_PART_BYTES:
                    raise ValueError(f"case record exceeds {MAX_RECORD_BYTES} bytes: {record['id']}")
                compressed.write(line)
                row_count += 1
                raw_bytes += len(line)
    stored_bytes, sha256 = digest_file(path)
    return {
        "path": name,
        "rows": row_count,
        "raw_bytes": raw_bytes,
        "compressed_bytes": stored_bytes,
        "sha256": sha256,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    output = args.output.absolute()

    # The report replay receipt is the gate: never start a packet directory
    # unless all four report artifacts and the completed offline replay exist.
    report_names = ("metrics.json", "preparation.json", "surrogates.json", "replay.json")
    if any(not (root / "report" / name).is_file() for name in report_names):
        raise FileNotFoundError("completed report including replay.json is required")
    replay = read(root / "report/replay.json")
    if replay.get("status") != "passed_offline_result_replay" or replay.get("unique_cases") != 10_000:
        raise ValueError("report replay receipt does not certify the complete frozen cohort")
    if output.exists():
        raise FileExistsError(output)
    if not output.parent.is_dir():
        raise FileNotFoundError(output.parent)

    design, _, _ = admission(root)
    prepared = read(root / "native-preparation/complete.json")
    if prepared.get("status") != "passed":
        raise ValueError("native preparation did not complete")
    cases = design["cases"]
    if len(cases) != CASES_PER_PART * PART_COUNT:
        raise ValueError("wire format requires exactly 10,000 frozen cases")
    references = {}
    stage_records = {}
    for profile in ("primary", "holdout", "scale", "smoke"):
        complete = read(root / f"reference-{profile}/complete.json")
        if complete.get("status") != "passed":
            raise ValueError(f"reference cohort did not complete: {profile}")
        expected_db = complete["expectations"]
        if expected_db["path"] != str(root / f"reference-{profile}/expectations.sqlite"):
            raise ValueError(f"unexpected expectation database path: {profile}")
        verify([expected_db])
        stage_records[f"reference-{profile}"] = complete
    for stage in ("polars", "native", "alternate"):
        expected_count = len(design["alternate_route_cases"]) if stage == "alternate" else len(cases)
        stage_records[f"observe-{stage}"] = stage_complete(root, stage, expected_count)
    verify([replay["metrics"], replay["surrogate_rows"], replay["case_rows"], replay["frozen"]])

    output.mkdir(mode=0o700, parents=False, exist_ok=False)
    parts = []
    with ExitStack() as stack:
        for profile in ("primary", "holdout", "scale", "smoke"):
            db = stack.enter_context(
                closing_db(root / f"reference-{profile}/expectations.sqlite", readonly=True)
            )
            references[profile] = db
        streams = {
            stage: attempt_stream(stack, root, stage, stage_records[f"observe-{stage}"])
            for stage in ("polars", "native", "alternate")
        }
        for part_index in range(PART_COUNT):
            begin = part_index * CASES_PER_PART
            group = cases[begin : begin + CASES_PER_PART]

            def records():
                for offset, spec in enumerate(group):
                    seq = begin + offset
                    identity, expected_blob = (
                        references[spec["profile"]]
                        .execute("SELECT identity,value FROM expected WHERE id=?", (spec["id"],))
                        .fetchone()
                    )
                    expected_blob = bytes(expected_blob)
                    if identity != spec["case_identity"]:
                        raise ValueError(f"expected identity differs from frozen case: {spec['id']}")
                    expected_value, hashes = bounded_unpack(expected_blob, f"expected:{spec['id']}")
                    record = {
                        "seq": seq,
                        "id": spec["id"],
                        "expected": {
                            "identity": identity,
                            "value": expected_value,
                            **hashes,
                        },
                        "polars": next_packet(streams["polars"], seq, spec["id"], "polars"),
                        "native": next_packet(streams["native"], seq, spec["id"], "native"),
                    }
                    if spec.get("surrogate"):
                        alt_seq = design["alternate_route_cases"].index(spec["id"])
                        record["alternate"] = next_packet(
                            streams["alternate"], alt_seq, spec["id"], "alternate"
                        )
                    yield record

            parts.append(write_part(output, part_index, records()))
        if any(next(stream, None) is not None for stream in streams.values()):
            raise ValueError("an attempt ledger contains extra rows")

    receipts = [
        item
        for profile in prepared["profiles"].values()
        for item in profile["receipts"]
        if Path(item["path"]).is_relative_to(root / "native-preparation")
        and Path(item["path"]).suffix in (".json", ".txt")
    ]
    verify(receipts)
    metadata_names = sorted(
        set(METADATA_NAMES) | {Path(item["path"]).relative_to(root).as_posix() for item in receipts}
    )
    metadata = []
    with (output / "metadata.jsonl.xz").open("xb") as raw:
        with lzma.LZMAFile(raw, "wb", format=lzma.FORMAT_XZ, preset=3) as compressed:
            for name in metadata_names:
                source = source_record(root, name)
                record = canonical_json(source) + b"\n"
                compressed.write(record)
                metadata.append(
                    {
                        "name": name,
                        "source_bytes": source["bytes"],
                        "source_sha256": source["sha256"],
                        "record_bytes": len(record),
                    }
                )
    metadata_bytes, metadata_sha256 = digest_file(output / "metadata.jsonl.xz")

    frozen = read(root / "frozen.json")
    index = {
        "format": "shardloom-expanded-portable-evidence",
        "schema_version": 1,
        "case_schema": {
            "encoding": "jsonl-xz",
            "hashes": "blob_sha256 binds the original SQLite bytes; json_sha256 binds their exact decompressed contents",
            "ordering": "frozen design case order; seq is zero-based",
            "cases_per_part": CASES_PER_PART,
        },
        "counts": {
            "cases": len(cases),
            "parts": PART_COUNT,
            "alternate_attempts": len(design["alternate_route_cases"]),
        },
        "stage_order": design["execution"]["order"],
        "expected_stage_counts": {
            "reference-primary": stage_records["reference-primary"]["count"],
            "reference-holdout": stage_records["reference-holdout"]["count"],
            "reference-scale": stage_records["reference-scale"]["count"],
            "reference-smoke": stage_records["reference-smoke"]["count"],
            "observe-polars": len(cases),
            "observe-native": len(cases),
            "observe-alternate": len(design["alternate_route_cases"]),
        },
        "surrogate_order": design["alternate_route_cases"],
        "metadata": {
            "path": "metadata.jsonl.xz",
            "encoding": "jsonl-xz; each data field is base64 of exact source bytes",
            "records": metadata,
            "raw_bytes": sum(record["record_bytes"] for record in metadata),
            "compressed_bytes": metadata_bytes,
            "sha256": metadata_sha256,
        },
        "parts": parts,
        "source": {
            "frozen_sha256": hashlib.sha256((root / "frozen.json").read_bytes()).hexdigest(),
            "design_sha256": hashlib.sha256((root / "design.json").read_bytes()).hexdigest(),
            "inputs_sha256": hashlib.sha256((root / "inputs.json").read_bytes()).hexdigest(),
            "stage_receipts": {
                name: hashlib.sha256((root / name / "complete.json").read_bytes()).hexdigest()
                for name in (
                    "native-preparation",
                    "reference-primary",
                    "reference-holdout",
                    "reference-scale",
                    "reference-smoke",
                    "observe-polars",
                    "observe-native",
                    "observe-alternate",
                )
            },
            "expected_databases": {
                profile: stage_records[f"reference-{profile}"]["expectations"]
                for profile in ("primary", "holdout", "scale", "smoke")
            },
            "observed_ledgers": {
                stage: stage_records[f"observe-{stage}"]["ledger"]
                for stage in ("polars", "native", "alternate")
            },
            "report_replay_sha256": hashlib.sha256((root / "report/replay.json").read_bytes()).hexdigest(),
            "source_revision": frozen.get("source_revision"),
            "harness": frozen.get("harness"),
        },
    }
    with (output / "index.json").open("x", encoding="utf-8") as target:
        json.dump(index, target, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False)
        target.write("\n")


if __name__ == "__main__":
    main()
