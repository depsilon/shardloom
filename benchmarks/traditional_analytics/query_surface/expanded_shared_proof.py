# SPDX-License-Identifier: Apache-2.0
"""Bound source, local gates and isolated capture proofs for the shared-work cohort."""

import hashlib
import json
import re

from query_surface.expanded_replay import require, sha


def validate_gates(proofs, members, text, runtime, acceptance):
    gates = proofs["source-gates.json"]
    supplemental = proofs["source-match.json"]["supplemental_gate_sources"]
    require(set(supplemental) == {"python/THIRD_PARTY_LICENSES/manifest.json", "vendor/vortex-file/upstream-provenance.json"},
            "supplemental gate source inventory differs")
    tested_sources = dict(runtime["source_snapshot"], **supplemental)
    require(gates["status"] == "passed" and gates["new_ignored_tests"] == 0,
            "source gates are incomplete")
    for field, name in (("source_gates", "source-gates.json"),
                        ("integration_source", "integration-source.json"),
                        ("installed_proof", "installed.json"),
                        ("installed_guard", "installed-guard.json"),
                        ("runtime_manifest", "runtime.json")):
        require(acceptance[field] == members[name], "unbound local acceptance " + name)
    for name, evidence in gates["receipts"].items():
        receipt = proofs[name + ".json"]
        require(evidence["sha256"] == members[name + ".json"]["sha256"]
                and receipt["status"] == "passed" and receipt["returncode"] == 0
                and receipt["process_group_drained"] and receipt["direct_child_reaped"]
                and receipt["log_sha256"] == members[name + ".log"]["sha256"],
                "failed or unbound source gate " + name)
        for path in evidence["source_files_checked"]:
            require(receipt["changed_source_inventory"][path] == tested_sources[path],
                    "tested source differs from measured source")
        counts = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;",
                            text[name + ".log"])
        actual = dict(binaries=len(counts), passed=sum(int(row[0]) for row in counts),
                      failed=sum(int(row[1]) for row in counts), ignored=sum(int(row[2]) for row in counts))
        require(actual == evidence["tests"] == acceptance["tests"][name], "source test counts differ")
    for path, digest in gates["untracked_source"].items():
        if not path.startswith("docs/"):
            require(runtime["source_snapshot"][path]["sha256"] == digest, "new source differs")
    require(len(gates["receipts"]) == 7
            and acceptance["tests"]["mechanism-native-workspace-tests-r2"]["passed"] == 6901
            and acceptance["tests"]["mechanism-default-workspace-tests-r1"]["passed"] == 3319
            and acceptance["tests"]["mechanism-file-provider-tests-r6"]["passed"] == 167,
            "required native, default or provider tests missing")
    integration = proofs["integration-source.json"]
    require(integration["status"] == "passed" and integration["head"] == runtime["source_revision"]
            and integration["code_and_test_sources_unchanged"]
            and integration["source_gates_sha256"] == members["source-gates.json"]["sha256"]
            and len(integration["merged_paths"]) == 6
            and all(path.startswith("docs/release/") for path in integration["merged_paths"]),
            "release documentation integration changed measured code")
    package, guard = proofs["installed.json"], proofs["installed-guard.json"]
    require(package["status"] == guard["status"] == "passed"
            and guard["returncode"] == 0 and guard["process_group_drained"] and guard["direct_child_reaped"]
            and guard["head"] == runtime["source_revision"] and guard["working_tree"] == ""
            and guard["log_sha256"] == members["installed-guard.log"]["sha256"]
            and package["installation_performed"] and package["uninstall_status"] == "passed"
            and package["publication_attempted"] is False
            and package["integration"] == acceptance["installed_tests"]
            == dict(status="passed", tests=303, failures=0, errors=0, skipped=0)
            and package["integrity"]["native_library_sha256"] == runtime["products"]["_native.abi3.so"]
            and package["integrity"]["cli_sha256"] == runtime["products"]["shardloom"],
            "installed package differs from measured release")
    for step in package["steps"]:
        require(step["returncode"] == 0
                and step["log_sha256"] == members["installed/" + step["log"]]["sha256"],
                "failed or unbound installed package step")


def validate_capture(index, proofs, members, text, hashes, previous_members):
    protocol, binding, complete = (proofs[k] for k in ("protocol.json", "binding.json", "complete.json"))
    updated = binding["contract"]["updated"]
    require(protocol["status"] == "admitted_before_execution"
            and protocol["expected_attempt_count"] == complete["total"] == complete["attempt_slots"] == 10000
            and protocol["runtime_binding"] == updated == complete["updated"]
            and protocol["design"]["sha256"] == hashes["design.json"]
            and protocol["source"]["sha256"] == hashes["inputs.json"]
            and protocol["frozen"] == updated["original_freeze"]
            and updated["original_freeze"]["sha256"] == hashes["frozen.json"]
            and updated["prepared"]["sha256"] == hashes["native-preparation/complete.json"]
            and updated["cached_polars"]["sha256"] == hashes["observe-polars/complete.json"]
            and updated["legacy_native"]["sha256"] == hashes["observe-native/complete.json"]
            and updated["previous_native"]["sha256"] == previous_members["complete.json"]["sha256"]
            and complete["status"] == "complete_observation" and complete["posthash_verified"]
            and complete["outcomes"] == {"passed": 10000}
            and complete["frozen"] == binding["contract"]["frozen"] == updated["original_freeze"]
            and complete["prerequisites"] == binding["contract"]["prerequisites"]
            == {"polars": updated["cached_polars"], "preparation": updated["prepared"]}
            and complete["attempts_per_case"] == updated["attempts_per_case"] == protocol["attempts_per_case"] == 1
            and complete["requested_repeats"] == updated["warmups"] == updated["alternate_attempts"]
            == protocol["warmups"] == 0
            and binding["contract"]["stage"] == complete["stage"] == "native"
            and complete["binding"]["sha256"] == members["binding.json"]["sha256"],
            "shared capture contract differs")
    worker = proofs["worker-source-verification.json"]
    require(worker["published_frozen_sha256"] == hashes["frozen.json"]
            and worker["complete_harness_count"] == len(worker["harness_sha256"]) == 65
            and worker["harness_sha256"]["benchmarks/traditional_analytics/query_surface/expanded_worker.py"]
            == updated["original_worker"]["sha256"], "original worker source binding differs")
    for field, name in (("driver", "driver.py"), ("admission_helper", "admission-helper.py"),
                        ("manifest", "runtime.json"), ("build", "build.json"),
                        ("acceptance", "local-acceptance.json"), ("admission", "admission.json"),
                        ("supervisor", "guard.py"), ("isolation_driver", "isolation.py")):
        require(updated[field]["sha256"] == members[name]["sha256"], "unbound capture source " + name)
    guard, isolation = proofs["guard.json"], proofs["isolation.json"]
    require(guard["status"] == "passed" and guard["returncode"] == isolation["capture_returncode"] == 0
            and guard["process_group_drained"] and guard["direct_child_reaped"]
            and guard["query_workload"] and guard["working_tree"] == ""
            and guard["head"] == updated["head"] == index["head"] == index["build"]
            and guard["driver_sha256"] == protocol["supervisor"]["sha256"] == members["guard.py"]["sha256"]
            and guard["log_sha256"] == members["guard.log"]["sha256"]
            and isolation["driver_sha256"] == protocol["isolation_driver"]["sha256"]
            == members["isolation.py"]["sha256"]
            and [row["pid"] for row in isolation["held_task_observers"]] == isolation["resumed_pids"]
            and all(row["scopes"] == ["active_output"] for row in guard["storage_inventory_scopes"]
                    if row["child_started"] and not row["forced"]), "capture supervision or isolation differs")
    runtime, receipt, acceptance = (proofs[k] for k in ("runtime.json", "build.json", "local-acceptance.json"))
    require(runtime["source_revision"] == receipt["head"] == updated["build_source"] == index["build"]
            and runtime["profile"] == "release" and runtime["features"] == ["release-user-surfaces"]
            and runtime["build_receipt_sha256"] == members["build.json"]["sha256"]
            and runtime["source_overlay"] == {"bytes": 0, "sha256": sha(b"")}
            and not runtime["untracked_source_paths"] and not receipt["changed_source_inventory"]
            and receipt["status"] == acceptance["status"] == "passed" and receipt["returncode"] == 0
            and receipt["process_group_drained"] and receipt["direct_child_reaped"]
            and receipt["working_tree"] == "" and acceptance["head"] == index["head"]
            and acceptance["measured_candidate"] == index["build"] and acceptance["runtime_source_equality"]
            and receipt["log_sha256"] == members["build.log"]["sha256"]
            and receipt["command"] == ["cargo", "build", "--locked", "--offline", "--release",
                                       "--features", "release-user-surfaces", "-p", "shardloom-cli", "-p", "shardloom-python"]
            and all(f"Compiling {crate} v1.1.0 ({receipt['cwd']}/{crate})" in text["build.log"]
                    for crate in ("shardloom-vortex", "shardloom-cli", "shardloom-python")),
            "native release build differs")
    for kind, product in (("binary", "shardloom"), ("library", "_native.abi3.so")):
        file = updated["runtime"][kind]
        require(file["sha256"] == runtime["products"][product] == runtime["files"][product]["sha256"]
                and file["generation"]["bytes"] == runtime["files"][product]["bytes"], "bound native product differs")
    raw = text["source.commit"].encode()
    commit = hashlib.sha1(f"commit {len(raw)}\0".encode() + raw, usedforsecurity=False).hexdigest()
    match = proofs["source-match.json"]
    snapshot = json.dumps(runtime["source_snapshot"], sort_keys=True, separators=(",", ":")).encode()
    require(commit == index["head"] == match["head"] == match["build_source"]
            and match["source_snapshot_sha256"] == sha(snapshot)
            and match["source_files"] == acceptance["source_files"] == len(runtime["source_snapshot"])
            and match["public_client_unchanged_from"] == "0155355e7927e6f8a4747daae5a6daf3438e822b",
            "exact release source proof differs")
    admission = proofs["admission.json"]
    require(admission["head"] == admission["build_source"] == index["head"]
            and admission["source_root"] == updated["source_root"]
            and admission["runtime_directory"] == updated["runtime_directory"], "admission differs")
    validate_gates(proofs, members, text, runtime, acceptance)
    return updated
