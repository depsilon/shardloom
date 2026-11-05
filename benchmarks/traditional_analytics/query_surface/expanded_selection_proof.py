# SPDX-License-Identifier: Apache-2.0
"""Bind the combined runtime, compatible Python facade and single-pass evidence."""

import hashlib
import json
from pathlib import PurePosixPath
import re

from query_surface.expanded_replay import require, sha

PROTOCOL = "combined-10k-protocol-r1.json"
GATES = ("selection-combined-format-r2", "selection-combined-native-clippy-r2",
         "selection-combined-native-workspace-r1", "selection-combined-default-clippy-r1",
         "selection-combined-default-workspace-r1")


def linked(reference, name, members):
    require(reference["sha256"] == members[name]["sha256"], "unbound proof: " + name)
    size = reference.get("bytes", reference.get("generation", {}).get("bytes"))
    if size is not None:
        require(size == members[name]["bytes"], "proof size differs: " + name)


def guard(proofs, members, name):
    receipt = proofs[name + ".json"]
    require(receipt["status"] == "passed" and receipt["returncode"] == 0
            and receipt["process_group_drained"] is True and receipt["direct_child_reaped"] is True
            and receipt["timed_out"] is False and receipt["interrupted_signal"] is None
            and receipt["group_absence_unproved_at_cleanup"] is False
            and receipt["log_sha256"] == members[name + ".log"]["sha256"],
            "failed or unbound guard: " + name)
    return receipt


def python_core_counts(proofs, text):
    """Keep the original planned-count misreport distinct from executed evidence."""
    report = proofs["python-core-r2/core.json"]
    old = proofs["combined-public-acceptance-r1.json"]["python_core"]
    log = text["selection-combined-python-core-r2.log"]
    executed = re.findall(r"^Ran (\d+) tests in [0-9.]+s$", log, re.M)
    failures = re.findall(r"^FAILED \(failures=(\d+), skipped=(\d+)\)$", log, re.M)
    require(executed == ["817"] and failures == [("2", "57")]
            and report["test_count"] == old["tests"] == 1145
            and report["failure_count"] == old["failed_initially"] == 2
            and report["error_count"] == 0 and report["skipped_count"] == old["skipped"] == 57
            and old["passed_initially"] == 1086 and old["unchanged_code_followup_passed"] == 2,
            "planned and executed Python test counts differ from original evidence")
    return dict(planned_test_count=report["test_count"], reported_tests_run=int(executed[0]),
                failures=report["failure_count"], errors=report["error_count"],
                skipped_entries=report["skipped_count"], followup_failed_tests_passed=2,
                successful_test_count=None, historical_inferred_pass_count=old["passed_initially"],
                historical_pass_count_is_not_executed_count=True,
                count_limit="Class-level skips can be recorded without incrementing testsRun; no successful count is inferred.")


def validate_public(proofs, members, runtime, text):
    public = proofs["combined-public-acceptance-r1.json"]
    require(public["status"] == "passed" and public["runtime_source_equality"] is True
            and public["source_revision"] == runtime["source_revision"]
            and public["canonical_classes"] == 76 and public["canonical_declarations_unchanged"] is True,
            "public compatibility is incomplete")
    for path, descriptor in public["source_inventory"].items():
        require(runtime["source_snapshot"][path] == descriptor, "public source differs from runtime")
    for key, name in (("runtime", "combined-runtime-r1/manifest.json"),
                      ("installed", "installed-combined-r1/report.json"),
                      ("baseline", "public-compatibility-baseline-r2/baseline-r2-final.json"),
                      ("facade_extraction", "facade-extraction-audit-r2/audit.json"),
                      ("collection_probe_controls", "collection-probe-controls-r1.json")):
        linked(public[key], name, members)
    for check in public["checks"]:
        name = PurePosixPath(check["path"]).name
        linked(check, name, members)
        guard(proofs, members, name.removesuffix(".json"))
    core = public["python_core"]
    require(core["original_status"] == "failed" and core["tests"] == 1145
            and core["passed_initially"] == 1086 and core["failed_initially"] == 2
            and core["unchanged_code_followup_passed"] == 2 and core["skipped"] == 57,
            "original Python failures or follow-up disposition lost")
    linked(core["guard"], "selection-combined-python-core-r2.json", members)
    linked(core["report"], "python-core-r2/core.json", members)
    python_core_counts(proofs, text)
    scope = proofs["facade-scope-primary-r1.json"]
    require(scope["status"] == "passed" and scope["each_reference_matches_original_scope"] is True
            and scope["coverage"] == {"functions": 403, "qualified_global_references": 1515,
                                      "unqualified_local_or_annotation_references": 235}
            and len(scope["functions"]) == 403, "lexical scope coverage differs")
    for row in scope["functions"]:
        require(runtime["source_snapshot"]["python/src/shardloom/" + row["helper"]]["sha256"]
                == row["helper_sha256"], "scope audit helper source differs")
    require(scope["verification_helper_sha256"] == members["tools/verify_facade_scope_primary.py"]["sha256"],
            "scope verifier source differs")
    audit = proofs["facade-extraction-audit-r2/audit.json"]
    require(not audit["discrepancies"], "facade forwarding discrepancies remain")


def validate_capture(index, proofs, members, text, hashes):
    protocol = proofs[PROTOCOL]
    complete, binding = (proofs["combined-10k-r1/" + name + ".json"] for name in ("complete", "binding"))
    require(protocol["status"] == "admitted_before_execution"
            and protocol["head"] == index["head"] == index["build"]
            and protocol["expected_attempts"] == len(protocol["cases"]) == 10000
            and protocol["baseline_engine_nanos"] == 1513095098453
            and complete["status"] == "complete_observation" and complete["stage"] == "native"
            and complete["total"] == complete["attempt_slots"] == 10000
            and complete["outcomes"] == {"passed": 10000} and complete["posthash_verified"] is True
            and protocol["posthash_required"] is True
            and protocol["attempts_per_case"] == complete["attempts_per_case"] == 1
            and protocol["warmups"] == complete["warmups"] == protocol["requested_repeats"]
            == complete["requested_repeats"] == protocol["alternate_attempts"]
            == protocol["external_engine_attempts"] == 0
            and protocol["candidate_python_change_explicit"] is True
            and protocol["benchmark_code_and_clocks_byte_identical"] is True,
            "single-pass capture contract differs")
    linked(complete["protocol"], PROTOCOL, members)
    linked(binding["protocol"], PROTOCOL, members)
    linked(complete["binding"], "combined-10k-r1/binding.json", members)
    require(protocol["hash_policy"] == "full hashes before and after; request generations only during queries",
            "query hashing policy differs")
    frozen = {row["path"]: row for row in protocol["frozen_files"]}
    require(len(frozen) == len(protocol["frozen_files"]), "duplicate frozen source")
    for name in ("design.json", "inputs.json", "frozen.json", "native-preparation/complete.json"):
        expected = [row for path, row in frozen.items() if path.endswith("/expanded-query-2026-11-03/" + name)]
        require(len(expected) == 1 and expected[0]["sha256"] == hashes[name], "original source binding differs")
    observation = guard(proofs, members, "combined-10k-guard-r1")
    require(observation["head"] == index["head"] and observation["working_tree"] == ""
            and observation["query_workload"] is True and not observation["changed_source_inventory"]
            and observation["build_inventory_policy"] == observation["evidence_inventory_policy"] == "before_and_after_only"
            and all(row["scopes"] == ["active_output"] for row in observation["storage_inventory_scopes"]
                    if row["child_started"] and not row["forced"])
            and observation["command"][-3:] == ["observe", "--protocol-sha256", members[PROTOCOL]["sha256"]]
            and observation["driver_sha256"] == members["tools/guard_selection_retention.py"]["sha256"],
            "query supervision or isolation differs")
    runtime = proofs["combined-runtime-r1/manifest.json"]
    build = guard(proofs, members, "combined-release-build-r1")
    require(runtime["source_revision"] == build["head"] == index["build"]
            and runtime["features"] == ["release-user-surfaces"] and runtime["profile"] == "release"
            and runtime["build_receipt_sha256"] == members["combined-release-build-r1.json"]["sha256"]
            and runtime["source_overlay"] == {"bytes": 0, "sha256": sha(b"")}
            and not runtime["untracked_source_paths"] and build["working_tree"] == ""
            and not build["changed_source_inventory"]
            and build["command"] == ["cargo", "build", "--locked", "--offline", "--release", "--features",
                                      "release-user-surfaces", "-p", "shardloom-cli", "-p", "shardloom-python"],
            "native release build differs")
    for key, product in (("binary", "shardloom"), ("library", "_native.abi3.so")):
        descriptor = protocol["runtime"][key]
        require(descriptor["sha256"] == runtime["files"][product]["sha256"] == runtime["products"][product]
                and descriptor["generation"]["bytes"] == runtime["files"][product]["bytes"], "native product differs")
    commit = text["source.commit"].encode()
    require(hashlib.sha1(f"commit {len(commit)}\0".encode() + commit, usedforsecurity=False).hexdigest()
            == index["build"], "source commit object differs")
    match = proofs["source-match.json"]
    require(match["head"] == index["build"] and match["source_files"] == len(runtime["source_snapshot"])
            and match["source_snapshot_sha256"] == sha(json.dumps(runtime["source_snapshot"], sort_keys=True,
                                                                separators=(",", ":")).encode()),
            "runtime source snapshot differs")
    linked(match["candidate_diff"], "candidate.patch", members)
    restored = proofs["combined-source-restoration-r1.json"]
    require(restored["status"] == "passed" and restored["disposition"] == "combined_candidate_rejected"
            and restored["candidate"] == match["candidate_diff"]["to_commit"] == index["build"]
            and restored["accepted_baseline"] == match["candidate_diff"]["from_commit"]
            == "ac20b82118a4626bc5f47b530842318ed006f0d7"
            and restored["product_and_tests_exactly_match_accepted_source"] is True
            and len(restored["restored_paths"]) == len(set(restored["restored_paths"])) == 98,
            "rejected source restoration differs")
    gates = proofs["selection-combined-rust-acceptance-r1.json"]
    require(gates["status"] == "passed" and [row["name"] for row in gates["checks"]] == list(GATES),
            "required source gates missing")
    for path, descriptor in gates["source_inventory"].items():
        require(runtime["source_snapshot"][path] == descriptor, "tested source differs from runtime")
    for gate in gates["checks"]:
        name = gate["name"]
        receipt = guard(proofs, members, name)
        require(gate["receipt_sha256"] == members[name + ".json"]["sha256"]
                and gate["command"] == receipt["command"] and gate["status"] == "passed",
                "source gate command or identity differs")
        for path, descriptor in receipt["changed_source_inventory"].items():
            if path.endswith((".rs", ".toml", ".lock")):
                require(runtime["source_snapshot"][path] == descriptor, "gate source bytes differ")
        if "passed" in gate:
            counts = re.findall(r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;", text[name + ".log"], re.M)
            require(len(counts) == gate["targets"]
                    and tuple(sum(int(row[i]) for row in counts) for i in range(3))
                    == (gate["passed"], 0, gate["ignored"]), "source test totals differ")
    require(gates["checks"][2]["passed"] == 6926 and gates["checks"][4]["passed"] == 3319,
            "required test coverage differs")
    package = proofs["installed-combined-r1/report.json"]
    installed = guard(proofs, members, "combined-installed-wheel-r1")
    require(package["status"] == "passed" and package["installation_performed"] is True
            and package["publication_attempted"] is False and package["uninstall_status"] == "passed"
            and package["integration"] == dict(tests=314, failures=0, errors=0, skipped=0, status="passed")
            and installed["head"] == index["build"] and installed["working_tree"] == ""
            and package["integrity"]["native_library_sha256"] == runtime["products"]["_native.abi3.so"]
            and package["integrity"]["cli_sha256"] == runtime["products"]["shardloom"],
            "installed package differs from measured runtime")
    for step in package["steps"]:
        require(step["returncode"] == 0 and step["log_sha256"]
                == members["installed-combined-r1/" + step["log"]]["sha256"], "installed log differs")
    validate_public(proofs, members, runtime, text)
    harness = proofs["combined-harness-r1/manifest.json"]
    original = proofs["original-harness-source.json"]
    require(harness["status"] == "frozen" and harness["source_revision"] == index["build"]
            and harness["unchanged_harness_files"] == 35 and harness["candidate_python_files"] == 58,
            "public product harness differs")
    for field, name in (("runtime_manifest", "combined-runtime-r1/manifest.json"),
                        ("package_proof", "installed-combined-r1/report.json"),
                        ("compatibility", "combined-public-acceptance-r1.json"),
                        ("original_manifest", "original-harness-source.json")):
        linked(harness[field], name, members)
    counts = {"unchanged_harness": 0, "candidate_product_python": 0}
    for path, row in harness["files"].items():
        require(row["role"] in counts, "unknown harness role")
        counts[row["role"]] += 1
        descriptor = {key: row[key] for key in ("bytes", "sha256")}
        require(members["combined-harness-r1/" + path] == descriptor, "harness source bytes differ")
        if row["role"] == "unchanged_harness":
            require(row["sha256"] == original["harness_sha256"][path], "benchmark code or clocks changed")
        else:
            require(descriptor == runtime["source_snapshot"][path], "measured Python source differs")
    require(counts == {"unchanged_harness": 35, "candidate_product_python": 58}, "harness source counts differ")
    # Bind each included capture prerequisite that the protocol actually froze.
    for name, source in index["source_identities"].items():
        if source["path"] in frozen:
            linked(frozen[source["path"]], name, members)
    return protocol
