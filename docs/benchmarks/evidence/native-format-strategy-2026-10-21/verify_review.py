#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Verify the separate filtered-count review packet without its original host."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import tarfile


def verify(directory: Path):
    index = json.loads((directory / "review-index.json").read_bytes())
    archive = (directory / "review-records.tar.xz").read_bytes()
    assert len(archive) == index["archive"]["bytes"]
    assert hashlib.sha256(archive).hexdigest() == index["archive"]["sha256"]
    members = {entry["archive_member"]: entry for entry in index["members"]}
    assert len(members) == len(index["members"]) == index["archive"]["member_count"]
    blobs = {}
    with tarfile.open(directory / "review-records.tar.xz", "r:xz") as stream:
        entries = stream.getmembers()
        assert len(entries) == len(members)
        assert {entry.name for entry in entries} == set(members)
        for entry in entries:
            assert entry.isfile() and not entry.issym() and not entry.islnk()
            expected = members[entry.name]
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

    def reference(ref):
        data = blobs[ref["path"]]
        assert len(data) == ref["bytes"]
        assert hashlib.sha256(data).hexdigest() == ref["sha256"]
        return data

    def counts(log):
        rows = re.findall(rb"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", log)
        assert rows
        return tuple(sum(int(row[column]) for row in rows) for column in range(3))

    acceptance = read("format-strategy-review-acceptance-910.json")
    assert acceptance["status"] == "passed" and acceptance["full43_rerun"] is False
    source = json.loads(reference(acceptance["source"]))
    assert source["head"] == "ca5a13fbfcf1fd7be4108f18d1af2c4159a9a554"
    assert source["tree"] == "97d30e633c8c97a63a71565645763c2b3eced686"
    assert len(source["source_hashes"]) == 2593
    for key in ("helper", "inventory_helper", "guard"):
        reference(source[key])
    assert source["head"].encode() in named("format-strategy-review-fix-ca5.patch")

    def receipt(ref, log_ref):
        row, log = json.loads(reference(ref)), reference(log_ref)
        assert row["process_group_drained"] and row["direct_child_reaped"]
        assert hashlib.sha256(log).hexdigest() == row["log_sha256"]
        for key, name in (("runner_sha256", "run_native_breadth_guard_871.py"),
                          ("guard_sha256", "native_workload_guard_816.py"),
                          ("supervisor_sha256", "bounded_build_process_resume_20260912.py")):
            assert hashlib.sha256(named(name)).hexdigest() == row[key]
        return row, log

    steps = {step["name"]: step for step in acceptance["steps"]}
    assert len(steps) == len(acceptance["steps"]) == 7
    assert set(steps) == {"fmt", "native-clippy", "extension", "python-native",
                          "workspace-clippy", "workspace-tests", "native-tests"}
    totals = {}
    for name, step in steps.items():
        row, log = receipt(step["receipt"], step["log"])
        assert row["status"] == "passed" and row["returncode"] == 0
        assert row["head"] == source["head"] and row["working_tree"] == ""
        command = row["command"]
        if name.endswith("tests"):
            totals[name] = counts(log)
            assert "test" in command and "--locked" in command
        if name in ("native-tests", "native-clippy"):
            assert "release-user-surfaces" in command
        if name in ("workspace-tests", "workspace-clippy"):
            assert "--workspace" in command and "--all-targets" in command
        if name.endswith("clippy"):
            assert command[-3:] == ["--", "-D", "warnings"]
        if name == "fmt":
            assert command[-4:] == ["fmt", "--all", "--", "--check"]
    assert totals == {"workspace-tests": (3238, 0, 0), "native-tests": (3162, 0, 31)}
    python_log = named("format-strategy-review-python-native-910.log")
    assert steps["python-native"]["tests"] == 18
    assert re.search(rb"Ran 18 tests", python_log) and python_log.rstrip().endswith(b"OK")
    assert b"skipped" not in python_log
    library = steps["extension"]["library"]
    assert library["sha256"] == "c3498a809425639b5905623a8350c2ac564895ec59a010e8674d8458c1d3a501"
    python_receipt = json.loads(reference(steps["python-native"]["receipt"]))
    assert "SHARDLOOM_NATIVE_TEST_LIBRARY=" + library["path"] in python_receipt["command"]

    attempts = acceptance["earlier_attempts"]
    assert set(attempts) == {"format-strategy-review-red-909", "format-strategy-review-red-909b",
                             "format-strategy-review-green-910"}
    for name, attempt in attempts.items():
        row, log = receipt(attempt["receipt"], attempt["log"])
        assert row["status"] == attempt["status"] and row["returncode"] == attempt["returncode"]
        if name.endswith("red-909"):
            assert row["returncode"] == 101 and b"error[E0432]: unresolved import" in log
            assert b"could not compile" in log and b"running 1 test" not in log
        elif name.endswith("red-909b"):
            assert row["returncode"] == 101
            assert b"assertion failed: report.physical_policy.writer_coalescing_enabled" in log
            assert b"0 passed; 1 failed; 0 ignored" in log
        else:
            assert row["returncode"] == 0 and counts(log) == (17, 0, 0)
    regression = b"filtered_count_writer_preserves_sink_policy_and_producer_evidence ... ok"
    assert regression in named("format-strategy-review-green-910.log")
    assert regression in named("format-strategy-review-native-tests-910.log")
    return {"status": "passed", "implementation_commit": source["head"],
            "archive_members": len(members), "source_inputs": len(source["source_hashes"]),
            "tests": totals, "native_python_tests": 18, "focused_tests": 17,
            "full43_rerun": False}


if __name__ == "__main__":
    print(json.dumps(verify(Path(__file__).resolve().parent), indent=2, sort_keys=True))
