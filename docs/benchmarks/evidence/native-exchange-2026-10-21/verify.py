# SPDX-License-Identifier: Apache-2.0
"""Replay the rejected exchange decision from compact records; runs no engine."""
from __future__ import annotations

import hashlib
import json
import lzma
from pathlib import Path
import re
import subprocess

from check_native_exchange_report_verifier import run_negative_matrix
from native_exchange_cost_gates import IDENTITY_KEYS, score_cohort
from native_exchange_result_checks import verify_workflow

HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[3]
CANDIDATE = "6e3419bc5f36d0ec41d381420d255c77ef533409"
BASELINE = "02aa39f3"
COHORTS = ("exchange-scored-report-one-846-a1.json", "exchange-scored-report-two-846-a1.json")
PREFLIGHT = (
    ("exchange-final-release-two-process-846-a1-received_native_values_execute_after_sender_exit.json", 13),
    ("exchange-final-release-two-process-846-a1-matched_arrow_flight_and_vortex_ipc_controls.json", 52),
    ("exchange-final-release-interop-846-a1-ordinary_flight_sql_and_explicit_native_selection.json", 2),
)
REPLAY_SOURCES = (
    "native_exchange_result_checks.py", "native_exchange_cost_gates.py",
    "check_native_exchange_report_verifier.py", "test_native_exchange_cost_gates.py",
)


def identity(data):
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def verify_git_sources(repository, revision, sources):
    """Check all frozen source bytes against the retained Git object, in one read."""
    request = "".join(f"{revision}:{row['file']}\n" for row in sources).encode()
    output = subprocess.check_output(["git", "cat-file", "--batch"], input=request, cwd=repository)
    position = 0
    for row in sources:
        newline = output.index(b"\n", position)
        header = output[position:newline].split()
        require(len(header) == 3 and header[1] == b"blob", f"missing Git source: {row['file']}")
        length = int(header[2])
        data = output[newline + 1:newline + 1 + length]
        require(identity(data) == {key: row[key] for key in ("bytes", "sha256")},
                f"Git source differs from frozen input: {row['file']}")
        position = newline + 1 + length
        require(output[position:position + 1] == b"\n", "malformed Git batch boundary")
        position += 1
    require(position == len(output), "unexpected extra Git source output")


def replay(members, *, verify_git=True):
    def raw(name):
        entry = members[name]
        data = entry["text"].encode()
        require(identity(data) == {key: entry[key] for key in ("bytes", "sha256")},
                f"changed packet member: {name}")
        return data

    def read(name):
        return json.loads(raw(name))

    for name in members:
        raw(name)
    manifest = read("fixture-manifest.json")
    freeze = read("protocol-freeze.json")
    build = read("exchange-validated-build-freeze-846-a1.json")
    require(build["head"] == CANDIDATE and build["status"] == "validated_before_scored_cohorts",
            "candidate was not accepted before the scored cohorts")
    require(build["scored_workflows_started"] == 0 and len(build["sources"]) == 2003,
            "unexpected pre-cohort build freeze")
    if verify_git:
        verify_git_sources(REPOSITORY, CANDIDATE, build["sources"])
    source_ids = {row["file"]: row for row in build["sources"]}
    for name in REPLAY_SOURCES:
        require(identity((HERE / name).read_bytes()) == {
            key: source_ids["scripts/" + name][key] for key in ("bytes", "sha256")
        }, f"replay source differs from scored implementation: {name}")

    checks = []
    for name in sorted(members):
        if not name.endswith(".json"):
            continue
        receipt = read(name)
        if not isinstance(receipt, dict) or not {"command", "returncode", "log_sha256"} <= receipt.keys():
            continue
        log_name = name[:-5] + ".log"
        require(identity(raw(log_name))["sha256"] == receipt["log_sha256"], f"changed log: {log_name}")
        checks.append({"name": name[:-5], "status": receipt["status"], "returncode": receipt["returncode"]})
    for check in build["guarded_checks"]:
        name = Path(check["receipt"]["path"]).name
        require(identity(raw(name)) == {key: check["receipt"][key] for key in ("bytes", "sha256")}, name)
        receipt = read(name)
        require(receipt["status"] == "passed" and receipt["returncode"] == 0
                and receipt["direct_child_reaped"] and receipt["process_group_drained"], name)
    for role in ("one", "two"):
        name = f"exchange-scored-cohort-{role}-846-a1.json"
        receipt = read(name)
        require(receipt["status"] == "passed" and receipt["returncode"] == 0
                and receipt["direct_child_reaped"] and receipt["process_group_drained"], name)

    rows = []
    for name, count in PREFLIGHT:
        report = read(name)
        require(report["status"] == "passed" and len(report["workflows"]) == count, name)
        require(report["executable"]["sha256"] == build["executable"]["sha256"], name)
        require(len(report["key_retirement"]["deleted_key_files"]) == 8, name)
        for row in report["workflows"]:
            verify_workflow(row, manifest, freeze)
        rows.extend(report["workflows"])
    negative = run_negative_matrix(rows, manifest, freeze)
    require(len(negative) == 24 and all(row["rejected"] and row["specific_check_matched"] for row in negative),
            "one or more report corruptions were not refused at their specific checks")

    cost = read("exchange-frozen-cost-report-846-a1.json")
    reports = [read(name) for name in COHORTS]
    rescored = [score_cohort(report, manifest, freeze) for report in reports]
    require(rescored == cost["cohorts"] and cost["cost_gates_passed"] is False,
            "cost scores do not reproduce the recorded rejection")
    require(cost["scorer_sha256"] == identity((HERE / "native_exchange_cost_gates.py").read_bytes())["sha256"],
            "scorer identity differs")
    require(cost["result_verifier_sha256"] == identity((HERE / "native_exchange_result_checks.py").read_bytes())["sha256"],
            "result verifier identity differs")
    expected_ids = {
        "fixture_manifest": identity(raw("fixture-manifest.json"))["sha256"],
        "protocol_freeze": identity(raw("protocol-freeze.json"))["sha256"],
        "build_freeze": identity(raw("exchange-validated-build-freeze-846-a1.json"))["sha256"],
        "executable": build["executable"]["sha256"],
        "runner": source_ids["scripts/run_native_exchange_experiment.py"]["sha256"],
        "result_verifier": source_ids["scripts/native_exchange_result_checks.py"]["sha256"],
    }
    require(set(expected_ids) == set(IDENTITY_KEYS), "incomplete cohort identity set")
    for report, result in zip(reports, rescored):
        require(result["identity_sha256"] == expected_ids, "cohort input identities differ")
        require(len(report["key_retirement"]["deleted_key_files"]) == 8, "scored private keys not retired")
    for item, name in zip(cost["inputs"], (*COHORTS, "fixture-manifest.json", "protocol-freeze.json")):
        require(identity(raw(name)) == {key: item[key] for key in ("bytes", "sha256")}, name)

    protocol = read("exchange-final-protocol-checks-and-key-retirement-846-a1.json")
    require(protocol["status"] == "passed" and protocol["protocol_tests"] == 41
            and protocol["process_failure_workflows"] == 4, "incomplete protocol proof")
    require(len(protocol["retirement"]["deleted_key_files"]) == 8, "unit private keys not retired")
    names = set()
    unit_identities = set()
    for selector in ("framing_limits_cancellation_replay_and_cleanup",
                     "mutual_tls_per_request_authorization_and_isolation",
                     "negotiation_identity_and_strict_native_refusal"):
        report = read(f"exchange-final-protocol-846-a1-{selector}.json")
        require(report["status"] == "passed", selector)
        discovered = set()
        for run in report["discovery_and_runs"]:
            found = re.findall(r"^(.+): test$", run["discovery_stdout"], re.MULTILINE)
            passed = re.findall(r"^test (.+) \.\.\. ok$", run["stdout"], re.MULTILINE)
            require(len(found) == len(passed) == run["expected_count"] > 0
                    and sorted(found) == sorted(passed) == sorted(run["test_names"]),
                    f"protocol discovery/execution mismatch: {run['selector']}")
            require(f"{len(passed)} passed; 0 failed; 0 ignored;" in run["stdout"], run["selector"])
            discovered.update(found)
        require(discovered == set(report["test_names"]), selector)
        names.update(discovered)
        unit_identities.add(report["unit_test_executable"]["sha256"])
    require(len(names) == 41 and len(unit_identities) == 1, "protocol test coverage or build identity changed")
    faults = read("exchange-final-protocol-846-a1-framing_limits_cancellation_replay_and_cleanup-process-failures.json")
    require(faults["status"] == "passed" and len(faults["workflows"]) == 4, "process-failure coverage differs")
    require({row["failure_kind"] for row in faults["workflows"]} == {
        "sender_death", "receiver_death", "restart_rejects_old_epoch", "full_fresh_transfer_after_restart"
    }, "process-failure cases differ")
    for row in faults["workflows"]:
        require(row["status"] == "passed" and row["owned_processes_drained"], row["failure_kind"])
        if "partial_transfer_barrier" in row:
            barrier = row["partial_transfer_barrier"]
            require(barrier["completed_executions"] == 0 and barrier["published_sources"] == 0
                    and barrier["retained_native_bytes"] > 0, "process death did not interrupt a partial source")
        if row["failure_kind"] == "full_fresh_transfer_after_restart":
            verify_workflow(row, manifest, freeze)
    totals = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;",
                        raw("exchange-workspace-tests-846-a1.log").decode())
    require((len(totals), *(sum(int(row[i]) for row in totals) for i in range(3))) == (107, 3235, 0, 0),
            "workspace acceptance totals differ")
    require("Ran 7 tests" in raw("exchange-cost-gates-unit-846-a1.log").decode(), "gate arithmetic tests missing")
    post = read("exchange-post-scoring-identities-846-a1.json")
    require(post["status"] == "passed" and post["scored_workflows"] == 650
            and post["external_inputs_unchanged"] == len(build["external_inputs"]) == 126
            and post["private_keys_deleted"] == 16, "post-scoring identity/cleanup check failed")
    require(identity(raw("exchange-frozen-cost-report-846-a1.json")) == {
        key: post["cost_report"][key] for key in ("bytes", "sha256")
    }, "post-scoring cost-report identity changed")
    return {
        "status": "verified", "decision": "rejected_frozen_mechanism",
        "candidate_commit": CANDIDATE, "source_inputs_verified": len(build["sources"]),
        "scored_workflows": sum(result["workflow_count"] for result in rescored),
        "preflight_workflows": len(rows), "report_corruption_refusals": len(negative),
        "protocol_tests": 41, "process_failure_workflows": 4,
        "cost_gates_passed": False, "guard_receipts": len(checks),
        "failure_history": [row for row in checks if row["status"] != "passed" or row["returncode"] != 0],
        "cohort_gates": [{"cohort": result["cohort"],
                          "gates": [{"gate": gate["gate"], "passed": gate["passed"]} for gate in result["gates"]]}
                         for result in rescored],
        "runtime_retention_claimed": False, "completed_broad_substeps": [],
    }


def main():
    record = json.loads((HERE / "decision.json").read_bytes())
    compressed = (HERE / "results.json.xz").read_bytes()
    require(identity(compressed) == {key: record["packet"][key] for key in ("bytes", "sha256")}, "packet identity changed")
    packet = json.loads(lzma.decompress(compressed))
    require(len(packet["members"]) == record["packet"]["members"], "packet member count changed")
    result = replay(packet["members"])
    require(result == record["replay"], "replayed decision differs from recorded decision")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
