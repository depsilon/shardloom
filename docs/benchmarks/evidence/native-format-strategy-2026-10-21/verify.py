#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify retained format-strategy evidence without the original machine or runtime."""
from __future__ import annotations

import gzip
import hashlib
import json
import math
from pathlib import Path
import re
import tarfile


def equivalent(actual, expected):
    if type(actual) is not type(expected):
        return False
    if isinstance(actual, dict):
        return actual.keys() == expected.keys() and all(
            equivalent(actual[key], expected[key]) for key in actual
        )
    if isinstance(actual, list):
        return len(actual) == len(expected) and all(
            equivalent(left, right) for left, right in zip(actual, expected)
        )
    if isinstance(actual, float):
        return math.isfinite(actual) and math.isfinite(expected) and math.isclose(
            actual, expected, rel_tol=1e-12, abs_tol=1e-12
        )
    return actual == expected


def verify(directory: Path):
    index = json.loads((directory / "index.json").read_bytes())
    archive = (directory / "records.tar.xz").read_bytes()
    assert len(archive) == index["archive"]["bytes"]
    assert hashlib.sha256(archive).hexdigest() == index["archive"]["sha256"]
    members = index["members"]
    by_member = {entry["archive_member"]: entry for entry in members}
    assert len(by_member) == len(members) == index["archive"]["member_count"]
    blobs = {}
    with tarfile.open(directory / "records.tar.xz", "r:xz") as stream:
        entries = stream.getmembers()
        assert len(entries) == len(by_member)
        assert {entry.name for entry in entries} == set(by_member)
        for entry in entries:
            assert entry.isfile() and not entry.issym() and not entry.islnk()
            expected = by_member[entry.name]
            payload = stream.extractfile(entry).read()
            assert len(payload) == expected["bytes"]
            assert hashlib.sha256(payload).hexdigest() == expected["sha256"]
            assert expected["original_path"] not in blobs
            blobs[expected["original_path"]] = payload
    assert sum(map(len, blobs.values())) == index["archive"]["uncompressed_member_bytes"]

    def named(name):
        found = [data for path, data in blobs.items() if Path(path).name == name]
        assert len(found) == 1, (name, len(found))
        return found[0]

    def read(name):
        return json.loads(named(name))

    def report_fields(report):
        assert report["status"] == "success"
        fields = {item["key"]: item["value"] for item in report["fields"]}
        assert len(fields) == len(report["fields"])
        assert fields["result_payload_complete"] == "true"
        for key in ("public_workflow_fallback_attempted", "public_workflow_external_engine_invoked"):
            assert fields[key] == "false"
        values = ([json.loads(line) for line in fields["result_jsonl"].splitlines()]
                  if "result_jsonl" in fields else json.loads(fields["result_values_json"]))
        assert isinstance(values, list) and all(isinstance(row, dict) for row in values)
        return fields, values

    tests = {}
    for name, expected in (
        ("format-final-workspace-tests-905", (3238, 0, 0)),
        ("format-native-tests-902", (3161, 0, 31)),
        ("format-alias-cli-905", (1099, 0, 0)),
    ):
        receipt = read(name + ".json")
        log = named(name + ".log")
        assert receipt["status"] == "passed" and receipt["returncode"] == 0
        assert receipt["process_group_drained"] and receipt["direct_child_reaped"]
        assert hashlib.sha256(log).hexdigest() == receipt["log_sha256"]
        counts = [tuple(map(int, match)) for match in re.findall(
            rb"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", log)]
        actual = tuple(sum(row[column] for row in counts) for column in range(3))
        assert actual == expected, (name, actual)
        tests[name] = dict(passed=actual[0], failed=actual[1], ignored=actual[2])
    build = read("format-strategy-build-907.json")
    source = read("format-strategy-source-907.json")
    assert build["status"] == "passed" and build["source_commit"] == source["head"]
    wheel = read("report.json")
    assert wheel["status"] == wheel["uninstall_status"] == "passed"
    assert wheel["integration"] == dict(tests=121, failures=0, errors=0, skipped=0, status="passed")
    assert not wheel["publication_attempted"] and wheel["installed_load"]["isolated_install"]
    assert wheel["integrity"]["cli_sha256"] == build["binary"]["sha256"]
    assert wheel["integrity"]["native_library_sha256"] == build["library"]["sha256"]

    proof = read("format-strategy-full43-independent-verification-908.json")
    summary = json.loads(blobs[proof["summary"]["path"]])
    assert summary["complete"] and summary["full_result_validation"] and summary["timing_available"]
    assert summary["build_commit"] == source["head"]
    assert summary["binary_sha256"] == build["binary"]["sha256"]
    assert summary["repeats"] == 1 and summary["timing_metric"] == "engine_execution_seconds"
    reference = read("clickbench-reference-values-2.json")
    assert reference["queries_sha256"] == summary["queries_sha256"]
    assert hashlib.sha256(named("queries.sql")).hexdigest() == summary["queries_sha256"]
    references = {record["query_id"]: record["values"] for record in reference["records"]}
    assert set(references) == set(range(1, 44))
    assert [record["query"] for record in summary["records"]] == list(range(1, 44))
    row_count, engine = 0, 0.0
    for record in summary["records"]:
        assert record["run"] == 1 and record["passed"] and record["returncode"] == 0
        assert record["validation"] == "complete_values" and not record["guard_failures"]
        assert len(record["completed_log_archives"]) == 1
        log = record["completed_log_archives"][0]
        encoded = blobs[log["path"]]
        assert hashlib.sha256(encoded).hexdigest() == log["gzip_sha256"]
        raw = gzip.decompress(encoded)
        assert len(raw) == log["raw_bytes"] and hashlib.sha256(raw).hexdigest() == log["raw_sha256"]
        fields, values = report_fields(json.loads(raw))
        assert equivalent(values, references[record["query"]]), record["query"]
        assert hashlib.sha256(json.dumps(values, sort_keys=True, allow_nan=False).encode()).hexdigest() == record["result_sha256"]
        seconds = int(fields["native_query_execution_nanos"]) / 1_000_000_000
        assert seconds >= 0 and math.isclose(seconds, record["engine_execution_seconds"], abs_tol=1e-12)
        engine += seconds
        row_count += len(values)
    assert row_count == 381 and math.isclose(engine, 48.754238077, abs_tol=1e-12)
    assert math.isclose(engine, summary["query_total_seconds"], abs_tol=1e-12)
    run = read("format-strategy-full43-run-908.json")
    assert run["status"] == "passed"
    for name in ("process_group_drained", "direct_child_reaped", "uat_lock_released", "source_unchanged",
                 "pre_input_hash_verified", "post_input_hash_verified"):
        assert run[name] is True

    reopen = read("format-original-readback-907.json")
    assert reopen["status"] == "passed" and len(reopen["cases"]) == 2
    assert sum(case["rows"] for case in reopen["cases"]) == 12
    for case in reopen["cases"]:
        assert case["status"] == "passed" and case["input_unchanged"]
        _, actual = report_fields(json.loads(blobs[case["output"]["path"]]))
        assert equivalent(actual, case["expected"]) and equivalent(actual, case["actual"])
    original = read("full43-file-route-results-898.json")
    assert len(original["routes"]) == 3
    for route in original["routes"]:
        assert route["queries"] == 43 and route["rows"] == 381
        assert [record["query"] for record in route["values"]] == list(range(1, 44))
        assert sum(len(record["actual_values"]) for record in route["values"]) == 381
        assert all(record["engine_execution_seconds"] is None for record in route["values"])
    audit = index["prior_audit_reference_hashes"]
    assert audit["unique_path_sha_pairs"] == len(audit["verification_results"]) == 141
    for item in audit["verification_results"]:
        assert hashlib.sha256(blobs[item["original_path"]]).hexdigest() == item["expected_sha256"]
    return dict(status="passed", archive_members=len(members), tests=tests, installed_native_tests=121,
                full43_queries=43, full43_rows=row_count, full43_engine_seconds=engine,
                original_readback_fixtures=2, preserved_prior_writer_queries=129)


if __name__ == "__main__":
    print(json.dumps(verify(Path(__file__).resolve().parent), indent=2, sort_keys=True))
