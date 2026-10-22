# SPDX-License-Identifier: Apache-2.0
"""Record/replay finite cyclic-existence intake; replay runs no native engine."""
import argparse
import hashlib
import json
import lzma
import math
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
REPOSITORY = HERE.parents[3]
CHECKS = {
    "cyclic-existence-prerequisite-tests-849-a1": "failed",
    "cyclic-existence-prerequisite-tests-849-a2": "passed",
    "cyclic-existence-prerequisite-clippy-849-a1": "failed",
    "cyclic-existence-prerequisite-clippy-849-a2": "passed",
    "cyclic-existence-public-prerequisite-849-a1": "failed",
    "cyclic-existence-public-prerequisite-849-a2": "passed",
    "cyclic-existence-final-prerequisite-tests-849-a1": "passed",
    "cyclic-existence-final-public-prerequisite-849-a1": "passed",
    "cyclic-existence-baseline-public-build-850-a1": "passed",
    "cyclic-existence-workload-reference-tests-850-a1": "passed",
    "cyclic-existence-fresh-public-prerequisite-850-a1": "passed",
    "cyclic-existence-fmt-850-a1": "passed",
    "cyclic-existence-clock-parser-tests-850-a1": "passed",
    "cyclic-existence-native-intake-850-a1": "failed",
    "cyclic-existence-native-intake-850-a2": "failed",
    "cyclic-existence-native-intake-850-a3": "passed",
}
FILES = (
    "cyclic-existence-prerequisite-freeze-849.json",
    "multiway-restored-source-823.json", "multiway-restored-public-build-823.json",
    "cyclic-existence-baseline-build-input-850.json", "cyclic-existence-baseline-build-850.json",
    "cyclic-existence-intake-protocol-850.json", "cyclic-existence-intake-protocol-850-v2.json",
    "cyclic-existence-intake-protocol-850-v3.json",
    "cyclic-existence-initial-probe-850.py", "cyclic-existence-initial-design-850.md",
    "cyclic-existence-second-probe-850.py", "cyclic-existence-second-design-850.md",
    "cyclic-existence-native-intake-results-850-a1.json",
    "cyclic-existence-native-intake-results-850-a2.json",
    "cyclic-existence-native-intake-results-850-a3.json", "run_guarded_observed_757.py",
)
TEST_OVERRIDES = (
    "python/tests/test_native_multiway_binding.py",
    "shardloom-vortex/src/local_primitive_relational_multiway_tests.rs",
)


def require(value, detail):
    if not value:
        raise ValueError(detail)


def identity(data):
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def stored_identity(row):
    return {key: row[key] for key in ("bytes", "sha256")}


def verify_sources(revision, overrides, sources):
    require(set(overrides) == set(TEST_OVERRIDES), "unexpected source override scope")
    request = "".join(overrides.get(row["file"], f"{revision}:{row['file']}") + "\n"
                      for row in sources).encode()
    output = subprocess.check_output(["git", "cat-file", "--batch"], input=request, cwd=REPOSITORY)
    position = 0
    for row in sources:
        newline = output.index(b"\n", position)
        header = output[position:newline].split()
        require(len(header) == 3 and header[1] == b"blob", f"missing Git input: {row['file']}")
        end = newline + 1 + int(header[2])
        require(identity(output[newline + 1:end]) == stored_identity(row), row["file"])
        require(output[end:end + 1] == b"\n", "malformed Git source boundary")
        position = end + 1
    require(position == len(output), "extra Git source output")


def verify_call(call, expected, schema, prepared_credits):
    require(call["result"] == {key: expected[key] for key in ("rows", "sha256")},
            "ordered result identity differs from independent reference")
    require(call["ordered_values_equal"] is True and call["schema"] == schema and call["delivery"]["complete"] is True,
            "incomplete values or schema")
    require(call["after_result_release"]["reserved_bytes"] == prepared_credits, "retained execution credits")
    report = call["report"]
    require(report["external_engine_invoked"] == "false" and report["fallback_attempted"] == "false"
            and report["resident_relational_declaration_reused"] == "true", "native prepared contract")
    require(report["native_query_timing_reported"] == "true"
            and report["native_query_timing_clock"] == "monotonic_wall_not_cpu"
            and report["native_query_timing_boundary"] ==
            "prepared_native_execution_including_io_generation_validation_and_certification_excluding_result_serialization",
            "unexpected engine clock")
    for key in ("native_query_execution_nanos", "native_result_serialization_nanos"):
        value = report[key]
        require(isinstance(value, str) and value.isascii() and value.isdecimal()
                and len(value) <= 20 and int(value) < (1 << 64), "invalid native clock")
    engine = int(report["native_query_execution_nanos"]) / 1e9
    require(math.isfinite(call["collect_seconds"]) and call["collect_seconds"] >= 0
            and call["engine_execution_seconds"] == engine
            and engine + int(report["native_result_serialization_nanos"]) / 1e9 <= call["collect_seconds"] + 1e-6,
            "engine clock exceeds the actual enclosing collect call")


def verify_comparison(protocol):
    names = ("abundant", "rare", "absent", "skew_wide", "null_duplicates", "semi_small",
             "empty_inner", "binary_semi", "binary_anti", "star_control", "binary_control",
             "cyclic_dense", "cyclic_small")
    require(tuple(cell["case"] for cell in protocol["workload"]["cells"]) == names, "changed fixed case set")
    require([cell["target"] for cell in protocol["workload"]["cells"]] == [True] * 4 + [False] * 9,
            "changed target/control roles")
    require(protocol["opportunity_gate"] == {"minimum_qualifying_targets": 2,
            "minimum_actual_child_rows": 65536, "minimum_child_per_outer_row": 16,
            "outer_denominator_floor": 1}, "changed opportunity gate")
    schedule = []
    for cohort in range(2):
        for case in names if cohort == 0 else reversed(names):
            for calls in (1, 4) if cohort == 0 else (4, 1):
                for sample in range(5):
                    parity = (names.index(case) + (0 if calls == 1 else 1) + sample + cohort) % 2
                    roles = ("ordinary", "candidate") if parity == 0 else ("candidate", "ordinary")
                    for role in roles:
                        schedule.append(dict(cohort="AB"[cohort], case=case, calls=calls, sample=sample, role=role))
    require(protocol["comparison"] == {"schedule": schedule, "workflow_count": 520,
            "engine_call_count": 1300, "median_samples": 5, "target_engine_ratio_max": 0.98,
            "control_engine_ratio_max": 1.05, "all_complete_workflow_ratio_max": 1.05,
            "cohorts_required": ["A", "B"], "absolute_noise_allowance": 0,
            "candidate_observers_in_scored_build": False, "candidate_build_binding_required_before_scoring": True},
            "changed prospective comparison schedule or gates")


def replay(record, packet):
    members = packet["members"]
    for name, entry in members.items():
        require(identity(entry["text"].encode()) == stored_identity(entry), f"changed member: {name}")

    def read(name):
        return json.loads(members[name]["text"])

    for name, status in CHECKS.items():
        receipt, log = read(name + ".json"), members[name + ".log"]["text"]
        require(receipt["status"] == status and (receipt["returncode"] == 0) == (status == "passed")
                and receipt["direct_child_reaped"] and receipt["process_group_drained"], name)
        require(identity(log.encode())["sha256"] == receipt["log_sha256"], name)
    native_log = members["cyclic-existence-final-prerequisite-tests-849-a1.log"]["text"]
    require("test result: ok. 3 passed; 0 failed; 0 ignored;" in native_log, "native test coverage")
    for name, count in (("cyclic-existence-final-public-prerequisite-849-a1", 3),
                        ("cyclic-existence-fresh-public-prerequisite-850-a1", 3),
                        ("cyclic-existence-workload-reference-tests-850-a1", 4),
                        ("cyclic-existence-clock-parser-tests-850-a1", 5)):
        log = members[name + ".log"]["text"]
        require(re.search(rf"Ran {count} tests in .*\n\nOK\s*$", log), name)

    native_freeze = read("cyclic-existence-prerequisite-freeze-849.json")
    inputs = read("cyclic-existence-baseline-build-input-850.json")
    build = read("cyclic-existence-baseline-build-850.json")
    require(native_freeze["head"] == inputs["head"] == record["base_commit"], "source base differs")
    require(len(native_freeze["native_inputs"]) == 1969 and len(inputs["source_inputs"]) == 2063,
            "source inventory coverage changed")
    by_name = {row["file"]: row for row in inputs["source_inputs"]}
    require(len(by_name) == len(inputs["source_inputs"]), "duplicate source input")
    for row in native_freeze["native_inputs"]:
        require(stored_identity(row) == stored_identity(by_name[row["file"]]), "prebuild native input changed")
    verify_sources(record["base_commit"], record["source_overrides"], inputs["source_inputs"])
    require(build["status"] == "passed" and build["source_unchanged"], "build source check failed")
    require(build["source_inventory"] == stored_identity(members["cyclic-existence-baseline-build-input-850.json"])
            and build["native_prebuild_freeze"] == stored_identity(members["cyclic-existence-prerequisite-freeze-849.json"]),
            "build input linkage changed")
    require({row["target"]["name"] for row in build["artifacts"]} == {"_native", "shardloom_vortex", "shardloom_runtime"}
            and all(not row["fresh"] and row["profile"]["opt_level"] == "3" for row in build["artifacts"]),
            "ordinary runtime was not freshly compiled")
    old_public = read("multiway-restored-public-build-823.json")
    require(native_freeze["public_reference_library"] == old_public["native_library"]
            and native_freeze["public_reference_source_manifest"]["sha256"] ==
            members["multiway-restored-source-823.json"]["sha256"], "historical public scope changed")

    first = read("cyclic-existence-intake-protocol-850.json")
    second = read("cyclic-existence-intake-protocol-850-v2.json")
    protocol = read("cyclic-existence-intake-protocol-850-v3.json")
    for key in ("workload", "comparison", "opportunity_gate", "library", "build_record"):
        require(first[key] == second[key] == protocol[key], f"harness correction changed frozen {key}")
    require(protocol["candidate_implemented"] is False and protocol["scored_workflows_started"] == 0,
            "unexpected candidate timing")
    verify_comparison(protocol)
    require(protocol["library"] == build["library"], "observed library differs from fresh build")
    require(protocol["build_record"] == stored_identity(members["cyclic-existence-baseline-build-850.json"]),
            "protocol build receipt differs")
    for name, expected in protocol["prerequisite_checks"].items():
        require(stored_identity(members[name + ".json"]) == expected and CHECKS[name] == "passed", name)
    require(stored_identity(members["cyclic-existence-initial-probe-850.py"]) == first["drivers"]["native_cyclic_existence_probe.py"]
            and stored_identity(members["cyclic-existence-initial-design-850.md"]) == stored_identity(first["design"]),
            "initial failed protocol source missing")
    require(stored_identity(members["cyclic-existence-second-probe-850.py"]) == second["drivers"]["native_cyclic_existence_probe.py"]
            and stored_identity(members["cyclic-existence-second-design-850.md"]) == stored_identity(second["design"]),
            "second failed protocol source missing")
    for name, expected in protocol["drivers"].items():
        require(stored_identity(members["drivers/" + name]) == expected, name)
    require(stored_identity(members["design.md"]) == stored_identity(protocol["design"]), "frozen design changed")
    failed = read("cyclic-existence-native-intake-results-850-a1.json")
    require(failed["status"] == "failed" and failed["cells"] == []
            and failed["error"] == "KeyError: 'engine_execution_seconds'", "initial failure changed")
    failed = read("cyclic-existence-native-intake-results-850-a2.json")
    require(failed["status"] == "failed" and failed["cells"] == []
            and failed["error"] == "ValueError: execution credits retained: abundant", "second failure changed")

    result = read("cyclic-existence-native-intake-results-850-a3.json")
    require(result["status"] == "passed" and result["protocol"] ==
            stored_identity(members["cyclic-existence-intake-protocol-850-v3.json"])
            and result["library"] == stored_identity(build["library"]), "intake lineage differs")
    cases = protocol["workload"]["cells"]
    require(len(cases) == 13 and [cell["case"] for cell in result["cells"]] == [cell["case"] for cell in cases],
            "incomplete native workload")
    qualifying, calls = [], 0
    for observed, wanted in zip(result["cells"], cases):
        outer = observed["outer"]
        require(outer["case"] == wanted["case"] and outer["inner"] is False
                and outer["final"]["reserved_bytes"] == 0 and len(outer["calls"]) == 2
                and outer["after_prepared_release"]["reserved_bytes"] == outer["source_credits"], "outer owner/call coverage")
        for number, call in enumerate(outer["calls"]):
            require(call["call"] == number, "repeated call order")
            verify_call(call, wanted["expected"], wanted["expected"]["schema"], outer["prepared_credits"])
            calls += 1
        inner = observed["inner"]
        if wanted["independent_inner"] is None:
            require(inner is None, "unexpected child observation")
            continue
        require(inner["case"] == wanted["case"] and inner["inner"] is True
                and inner["final"]["reserved_bytes"] == 0 and len(inner["calls"]) == 1
                and inner["after_prepared_release"]["reserved_bytes"] == inner["source_credits"], "child owner/call coverage")
        schema = {"Struct": [{"names": ["witness_b", "witness_a"],
                              "dtypes": [{"Primitive": ["i64", True]}] * 2}, False]}
        verify_call(inner["calls"][0], wanted["independent_inner"], schema, inner["prepared_credits"])
        calls += 1
        child_rows, outer_rows = inner["calls"][0]["result"]["rows"], outer["calls"][0]["result"]["rows"]
        if wanted["target"] and child_rows >= 65536 and child_rows >= 16 * max(outer_rows, 1):
            qualifying.append(wanted["case"])
    require(calls == 33 and result["opportunity"] == {
        "qualifying_targets": qualifying, "admitted": len(qualifying) >= 2, "performance_claim": False},
        "opportunity arithmetic differs")
    require(len(qualifying) == 4, "finite opportunity gate did not pass")
    return {"status": "verified", "native_sources": 1969, "build_and_python_sources": 2063,
            "native_cases": 13, "native_calls": calls, "qualifying_targets": qualifying,
            "passed_commands": list(CHECKS.values()).count("passed"),
            "preserved_failed_commands": list(CHECKS.values()).count("failed"),
            "candidate_implemented": False, "scored_workflows": 0}


def record(evidence):
    require(not (HERE / "intake.json").exists() and not (HERE / "intake.json.xz").exists(), "packet already exists")
    members = {}

    def add(path, name):
        data = path.read_bytes()
        members[name] = {**identity(data), "text": data.decode()}

    for name in FILES:
        add(evidence / name, name)
    for name in CHECKS:
        for suffix in (".json", ".log"):
            add(evidence / (name + suffix), name + suffix)
    protocol = json.loads(members["cyclic-existence-intake-protocol-850-v3.json"]["text"])
    for name in protocol["drivers"]:
        add(REPOSITORY / "scripts" / name, "drivers/" + name)
    add(REPOSITORY / protocol["design"]["file"], "design.md")
    inputs = json.loads(members["cyclic-existence-baseline-build-input-850.json"]["text"])
    overrides = {row["file"]: row["git_blob"] for row in inputs["source_inputs"] if row["file"] in TEST_OVERRIDES}
    result = {"schema": "shardloom.cyclic_existence_intake_evidence.v1", "base_commit": inputs["head"],
              "source_overrides": overrides,
              "scope": "Ordinary native row opportunity and correctness; not a retained optimization or package release."}
    packet = {"members": members}
    result["replay"] = replay(result, packet)
    raw = json.dumps(packet, sort_keys=True, allow_nan=False).encode()
    compressed = lzma.compress(raw)
    require(lzma.decompress(compressed) == raw, "packet readback changed data")
    result["packet"] = {"file": "intake.json.xz", **identity(compressed), "members": len(members)}
    with (HERE / "intake.json.xz").open("xb") as stream:
        stream.write(compressed)
    with (HERE / "intake.json").open("x") as stream:
        json.dump(result, stream, sort_keys=True, indent=2)
        stream.write("\n")
    print(json.dumps(result["replay"], sort_keys=True))


def verify():
    result = json.loads((HERE / "intake.json").read_bytes())
    compressed = (HERE / result["packet"]["file"]).read_bytes()
    require(identity(compressed) == stored_identity(result["packet"]), "packet identity changed")
    packet = json.loads(lzma.decompress(compressed))
    require(len(packet["members"]) == result["packet"]["members"], "packet member coverage changed")
    observed = replay(result, packet)
    require(observed == result["replay"], "recorded acceptance differs from replay")
    print(json.dumps(observed, sort_keys=True))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--record", type=Path)
    args = parser.parse_args()
    record(args.record) if args.record else verify()
