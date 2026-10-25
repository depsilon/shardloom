#!/usr/bin/env python3
"""Offline integrity and correctness checks for the native workflow drop packet."""
from __future__ import annotations

import hashlib
import io
import json
import lzma
import math
from pathlib import PurePosixPath
import re
import sys
import tarfile
import zlib

MAX_FILE = 16 * 1024 * 1024
MAX_EXPANDED = 96 * 1024 * 1024
MAX_TAR = MAX_EXPANDED + 4 * 1024 * 1024
MAX_OBJECTS = 2048
EXPECTED_HEAD = "167ac248ba662ae7967a7801995e9b160b17bae5"
EXPECTED_REFERENCE_SHA256 = "fc5780da415dc33bd06ed0879db07fc040db5dd419e3c37ecde7cd40a315696b"
EXPECTED_SECONDS = {"candidate": 52.873938411, "restored": 47.176619540,
                    "main": 47.889862794, "prior": 46.973255495}
EXPECTED_BINARY = {
    "candidate": "9bb5871feedce81ad866303a30fafce6c7ba09ac8b0b8445e08d8389bb14a3d9",
    "restored": "bbdb78fd982253b9428ceb76fc52af5d2b163fe9230c57e8e1b13f179a428988",
    "main": "65b899c70fa2fbe9572f6b490b66092595646e855ebaed14c7c4e25147081ade",
}
INPUT_SHA256 = "95a393d153a8c2e1ffeb5084b2656ac61465c6ef9dad435d032365844f1ac057"
QUERIES_SHA256 = "4afa04814edf3a4c52ff26fd87ea3b5dd92c7264b2d8d69ee718709f3df6f09b"
PREFIX = "/Users/dylan/LocalData/shardloom/workflow-work-elimination-20261025/"
REFERENCE = "/Users/dylan/LocalData/shardloom/native-prerequisites-20261022/independent-full43-948/reference.json"
RUNS = {
    "candidate": "checkpoint-full43-r1",
    "restored": "restored-full43-r1",
    "main": "restored-main-full43-r1",
}


def need(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def strict_json(raw: bytes | str):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            need(key not in out, f"duplicate JSON key: {key}")
            out[key] = value
        return out
    def constant(value):
        raise ValueError(f"nonfinite JSON constant: {value}")
    def finite(value):
        number = float(value)
        need(math.isfinite(number), "nonfinite JSON number")
        return number
    return json.loads(raw, object_pairs_hook=unique, parse_constant=constant, parse_float=finite)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def equivalent(actual, expected) -> bool:
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(equivalent(actual[k], expected[k]) for k in actual)
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(equivalent(a, b) for a, b in zip(actual, expected))
    if isinstance(actual, float):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(actual, expected, rel_tol=1e-12, abs_tol=1e-12)
    return actual == expected


def unsigned(value) -> int:
    need(type(value) is int or (type(value) is str and re.fullmatch(r"[0-9]+", value)), "invalid unsigned integer")
    number = int(value)
    need(number >= 0, "negative unsigned integer")
    return number


def bounded_gunzip(raw: bytes) -> bytes:
    decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
    decoded = decoder.decompress(raw, MAX_FILE + 1)
    need(len(decoded) <= MAX_FILE and not decoder.unconsumed_tail, "gzip expands beyond per-file limit")
    decoded += decoder.flush()
    need(len(decoded) <= MAX_FILE and decoder.eof and not decoder.unused_data, "invalid or oversized gzip stream")
    return decoded


def normalized(path: str) -> str:
    return path[len(PREFIX):] if path.startswith(PREFIX) else path


def object_files(manifest_raw: bytes, archive_raw: bytes, verifier_raw: bytes) -> dict[str, bytes]:
    manifest = strict_json(manifest_raw)
    need(manifest.get("schema_version") == "shardloom.workflow_drop.packet.v1", "manifest schema mismatch")
    need(manifest.get("verifier") == {"path": "verify.py", "sha256": sha(verifier_raw)}, "verifier identity mismatch")
    meta = manifest.get("archive")
    need(isinstance(meta, dict) and meta.get("path") == "packet.tar.xz", "archive descriptor malformed")
    need(meta.get("bytes") == len(archive_raw) and meta.get("sha256") == sha(archive_raw), "archive identity mismatch")
    try:
        need(len(archive_raw) <= MAX_TAR, "compressed archive too large")
        decoder = lzma.LZMADecompressor(memlimit=128 * 1024 * 1024)
        tar_bytes = decoder.decompress(archive_raw, max_length=MAX_TAR + 1)
        need(len(tar_bytes) <= MAX_TAR and decoder.eof and not decoder.unused_data, "invalid or oversized xz stream")
        with tarfile.open(fileobj=io.BytesIO(tar_bytes), mode="r:") as tar:
            payloads = {}
            for entry in tar:
                need(len(payloads) < MAX_OBJECTS, "too many archive objects")
                name = entry.name
                parts = PurePosixPath(name).parts
                need(not entry.issym() and not entry.islnk() and entry.isfile(), f"non-regular archive member: {name}")
                need(not name.startswith("/") and ".." not in parts and name == f"objects/sha256/{parts[-1]}", f"unsafe archive member: {name}")
                need(re.fullmatch(r"[0-9a-f]{64}", parts[-1]) is not None, f"bad object name: {name}")
                need(name not in payloads and 0 <= entry.size <= MAX_FILE, f"duplicate or oversized object: {name}")
                stream = tar.extractfile(entry)
                need(stream is not None, f"unreadable archive member: {name}")
                raw = stream.read(MAX_FILE + 1)
                need(len(raw) == entry.size and sha(raw) == parts[-1], f"object content mismatch: {name}")
                payloads[name] = raw
    except (tarfile.TarError, lzma.LZMAError) as exc:
        raise ValueError(f"invalid tar.xz archive: {exc}") from exc
    files = manifest.get("files")
    need(isinstance(files, list) and 0 < len(files) <= MAX_OBJECTS, "manifest file inventory absent or oversized")
    out = {}
    object_refs = set()
    expanded = 0
    for record in files:
        need(isinstance(record, dict), "malformed manifest file record")
        path, size, digest, member = (record.get(k) for k in ("path", "bytes", "sha256", "member"))
        need(isinstance(path, str) and path not in out, "duplicate or invalid manifest path")
        need(type(size) is int and 0 <= size <= MAX_FILE, f"invalid file size: {path}")
        need(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), f"invalid file hash: {path}")
        need(member == f"objects/sha256/{digest}" and member in payloads, f"missing or misaddressed object: {path}")
        raw = payloads[member]
        need(len(raw) == size and sha(raw) == digest, f"file content mismatch: {path}")
        out[path] = raw
        object_refs.add(member)
        expanded += size
    need(set(payloads) == object_refs, "unreferenced or missing archive objects")
    need(expanded <= MAX_EXPANDED and manifest.get("expanded_file_bytes") == expanded, "expanded-size accounting mismatch")
    need(manifest.get("unique_objects") == len(object_refs), "unique-object count mismatch")
    return out


def read_json(files: dict[str, bytes], path: str):
    key = normalized(path)
    need(key in files, f"required evidence missing: {key}")
    return strict_json(files[key])


def bound_ref(files: dict[str, bytes], record: dict) -> bytes:
    key = normalized(record["path"])
    need(key in files, f"referenced file missing: {key}")
    raw = files[key]
    need(type(record["bytes"]) is int and record["bytes"] == len(raw) and record["sha256"] == sha(raw), f"reference identity mismatch: {key}")
    return raw


def report_fields(envelope):
    need(isinstance(envelope, dict) and envelope.get("status") == "success", "query output did not succeed")
    fb = envelope.get("fallback")
    need(isinstance(fb, dict) and fb.get("attempted") is False and fb.get("allowed") is False and fb.get("engine") is None, "fallback evidence invalid")
    entries = envelope.get("fields")
    need(isinstance(entries, list), "query output report fields missing")
    result = {}
    for item in entries:
        need(isinstance(item, dict) and isinstance(item.get("key"), str) and item["key"] not in result, "duplicate or malformed report field")
        result[item["key"]] = item.get("value")
    need(result.get("public_workflow_external_engine_invoked") == "false" and result.get("public_workflow_fallback_attempted") == "false", "external engine/fallback evidence invalid")
    need(all(v in (False, "false") for k, v in result.items() if k.endswith(("fallback_attempted", "external_engine_invoked"))), "unexpected fallback field")
    keys = [k for k in ("result_jsonl", "result_values_json") if k in result]
    need(len(keys) == 1 and result.get("result_payload_complete") in (True, "true"), "complete structured result absent")
    rows_n = result.get("output_row_count")
    rows_n = unsigned(rows_n)
    payload = result[keys[0]]
    need(isinstance(payload, str), "result payload must be text")
    rows = [strict_json(line) for line in payload.splitlines()] if keys[0] == "result_jsonl" else strict_json(payload)
    need(isinstance(rows, list) and len(rows) == int(rows_n) and all(isinstance(row, dict) for row in rows), "invalid result rows")
    return rows, strict_json(result.get("result_schema_json", "null")), unsigned(result.get("native_query_execution_nanos")) / 1e9


def verify_evidence(files: dict[str, bytes]) -> dict:
    need(REFERENCE in files and sha(files[REFERENCE]) == EXPECTED_REFERENCE_SHA256, "independent reference hash mismatch")
    reference = strict_json(files[REFERENCE])
    ref_records = reference.get("records")
    need(isinstance(ref_records, list) and len(ref_records) == 43, "reference must contain 43 query records")
    expected_values = [row.get("values") for row in ref_records]
    need(sum(map(len, expected_values)) == 381 and [r.get("query_id") for r in ref_records] == list(range(1, 44)), "reference query/row inventory mismatch")
    base = read_json(files, "checkpoint-full43-r1-baseline-schemas.json")
    need(base.get("status") == "passed" and len(base.get("records", [])) == 43, "baseline schemas receipt invalid")
    need(math.isclose(sum(r["engine_execution_seconds"] for r in base["records"]), EXPECTED_SECONDS["prior"], rel_tol=0, abs_tol=1e-9), "prior engine-time total mismatch")
    schemas = {r["query"]: r["schema"] for r in base["records"]}
    need(set(schemas) == set(range(1, 44)), "baseline query schema inventory invalid")
    total_observations = {}
    for label, stem in RUNS.items():
        driver = read_json(files, f"{stem}.driver.json")
        acceptance = read_json(files, f"{stem}-acceptance.json")
        bound_ref(files, acceptance["outer"])
        bound_ref(files, acceptance["summary"])
        need(normalized(acceptance["outer"]["path"]) == f"{stem}.driver.json", f"{label}: acceptance bound to different driver")
        need(driver.get("status") == "passed" and driver.get("returncode") == 0 and driver.get("process_group_drained") is True and driver.get("native_process_cleanup_verified") is True, f"{label}: driver/cleanup receipt invalid")
        need(driver.get("input_before") == driver.get("input_after"), f"{label}: input changed during execution")
        need(driver["input_before"].get("sha256") == INPUT_SHA256 and driver["input_before"].get("bytes") == 15715228660, f"{label}: wrong input identity")
        summaries = driver.get("summaries")
        need(isinstance(summaries, list) and len(summaries) == 1, f"{label}: expected one summary")
        summary_record = summaries[0]
        summary = strict_json(bound_ref(files, summary_record))
        need(acceptance["summary"] == summary_record, f"{label}: acceptance summary mismatch")
        need(all(summary.get(k) is True for k in ("complete", "completed_identity_check", "reference_packet_identity_verified", "harness_identity_verified", "engine_timing_required", "timing_available")), f"{label}: incomplete identity/timing checks")
        need(summary.get("timing_metric") == "engine_execution_seconds" and summary.get("runs_completed") == 43 and summary.get("runs_passed") == 43, f"{label}: wrong timing/run contract")
        need(summary.get("source_bytes") == 15715228660 and summary.get("source") == driver["input_before"]["path"], f"{label}: summary input differs")
        need(summary.get("queries_sha256") == QUERIES_SHA256 and summary.get("reference_packet") == {"path": REFERENCE, "sha256": EXPECTED_REFERENCE_SHA256, "reference_kind": "independent_reference", "queries_sha256": QUERIES_SHA256}, f"{label}: reference binding invalid")
        need(len(summary["records"]) == 43 and summary.get("repeats") == 1 and summary.get("full_result_validation") is True, f"{label}: not a complete single Full43")
        need(summary.get("selected_query_ids") == list(range(1, 44)), f"{label}: query inventory mismatch")
        need(summary.get("binary_sha256") == driver["binary"].get("sha256") == EXPECTED_BINARY[label], f"{label}: binary identity mismatch")
        total = 0.0
        for idx, record in enumerate(summary["records"], 1):
            need(record.get("query") == idx and record.get("run") == 1 and record.get("passed") is True and record.get("returncode") == 0 and not record.get("guard_failures"), f"{label}: query {idx} incomplete or failed")
            need(record.get("validation") == "complete_values", f"{label}: query {idx} lacks full-value validation")
            archives = record.get("completed_log_archives")
            need(isinstance(archives, list) and len(archives) == 1, f"{label}: query {idx} output archive absent")
            arc = archives[0]
            raw_path = normalized(arc["path"])
            need(raw_path in files, f"{label}: query {idx} gzip missing")
            gz = files[raw_path]
            need(sha(gz) == arc["gzip_sha256"] and len(gz) == arc["gzip_bytes"], f"{label}: query {idx} gzip identity mismatch")
            raw = bounded_gunzip(gz)
            need(sha(raw) == arc["raw_sha256"] and len(raw) == arc["raw_bytes"], f"{label}: query {idx} raw identity mismatch")
            rows, schema, seconds = report_fields(strict_json(raw))
            need(equivalent(rows, expected_values[idx-1]), f"{label}: query {idx} result differs from independent reference")
            need(schema == schemas[idx], f"{label}: query {idx} schema differs from baseline")
            need(record.get("engine_timing_reported") is True and type(record["engine_execution_seconds"]) in (int, float) and seconds == record["engine_execution_seconds"], f"{label}: query {idx} engine time mismatch")
            need(sha(json.dumps(rows, sort_keys=True, allow_nan=False).encode()) == record.get("result_sha256"), f"{label}: query {idx} result hash mismatch")
            total += seconds
        need(math.isclose(total, summary["query_total_seconds"], rel_tol=0, abs_tol=1e-9), f"{label}: engine total does not sum")
        need(math.isclose(total, EXPECTED_SECONDS[label], rel_tol=0, abs_tol=1e-9), f"{label}: engine-time total differs from recorded observation")
        need(math.isclose(acceptance["engine_seconds"], total, rel_tol=0, abs_tol=1e-9), f"{label}: acceptance total mismatch")
        schema_gate = acceptance.get("exact_schemas", acceptance.get("exact_schema_continuity"))
        need(acceptance["complete_results"] == 43 and acceptance["complete_rows"] == 381 and schema_gate in (True, 43), f"{label}: acceptance correctness gate invalid")
        total_observations[label] = total
    candidate = read_json(files, "checkpoint-full43-r1-acceptance.json")
    bound_ref(files, candidate["reference"])
    bound_ref(files, candidate["baseline_schemas"])
    need(candidate["previous_engine_seconds"] == EXPECTED_SECONDS["prior"] and total_observations["candidate"] > EXPECTED_SECONDS["prior"] * 1.1, "candidate regression threshold mismatch")
    need(candidate["status"] == "failed_regression_gate" and candidate["more_than_10_percent_regression"] is True and candidate["performance_claim"] is False, "candidate regression gate should remain failed")
    restored = read_json(files, "restored-full43-r1-acceptance.json")
    main = read_json(files, "restored-main-full43-r1-acceptance.json")
    need(restored.get("exact_prior_accepted_executable") is True and restored.get("dropped_batch_remains_removed") is True, "restored prior binary identity evidence invalid")
    need(main.get("fresh_latest_main_executable") is True and main.get("dropped_batch_remains_removed") is True, "fresh main identity evidence invalid")
    receipt = read_json(files, "runtime-restoration-r1.json")
    need(receipt.get("status") == "restored_current_main" and receipt.get("head") == EXPECTED_HEAD and receipt.get("no_candidate_runtime_changes_remain") is True, "runtime restoration receipt invalid")
    tracked = receipt.get("tracked_runtime_restored")
    removed = receipt.get("new_runtime_removed")
    need(isinstance(tracked, list) and len(tracked) == 37 and all(x.get("equals_main") is True for x in tracked), "tracked runtime restoration evidence invalid")
    need(isinstance(removed, list) and len(removed) == 11 and receipt.get("runtime_source_files_equal_restored_main_inventory") == 2908, "runtime removal/inventory evidence invalid")
    return {"status": "passed", "queries_per_run": 43, "runs": 3, "rows_per_run": 381,
            "engine_totals_seconds": total_observations,
            "reference_sha256": EXPECTED_REFERENCE_SHA256, "fallback_observed": False}


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: verify.py MANIFEST.json PACKET.tar.xz", file=sys.stderr)
        return 2
    try:
        with open(argv[1], "rb") as stream:
            manifest = stream.read(MAX_FILE + 1)
        with open(argv[2], "rb") as stream:
            archive = stream.read(MAX_TAR + 1)
        with open(__file__, "rb") as stream:
            verifier = stream.read(MAX_FILE + 1)
        need(len(manifest) <= MAX_FILE and len(verifier) <= MAX_FILE, "metadata too large")
        files = object_files(manifest, archive, verifier)
        print(json.dumps(verify_evidence(files), sort_keys=True))
    except (OSError, ValueError, KeyError, TypeError, IndexError, EOFError, zlib.error, UnicodeError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, sort_keys=True), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
