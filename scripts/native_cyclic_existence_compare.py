#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Run the frozen finite comparison in fresh, serial public-native processes.

Invoke the parent only through the local native workload/storage supervisor.
Roles select separately built libraries, never an execution-policy switch.
"""
import argparse
import gc
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import statistics
import subprocess
import sys
import time
from unittest.mock import patch

import native_cyclic_existence_workload as workload
from query_timing_report import query_phase_timings


INTAKE_SHA256 = "4bcf755ee72984214434b20a744999063c92869e4f78aaa7fd6e1ac3a40df801"
SCHEMA = "shardloom.cyclic_existence_comparison.v1"


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def identity(path):
    data = Path(path).read_bytes()
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def check_file(record):
    require(identity(record["path"]) == {key: record[key] for key in ("bytes", "sha256")},
            f"frozen file changed: {Path(record['path']).name}")


def comparison_contract():
    schedule = []
    for cohort in range(2):
        for case in workload.CASES if cohort == 0 else reversed(workload.CASES):
            for calls in (1, 4) if cohort == 0 else (4, 1):
                for sample in range(5):
                    parity = (workload.CASES.index(case) + (calls == 4) + sample + cohort) % 2
                    roles = ("ordinary", "candidate") if parity == 0 else ("candidate", "ordinary")
                    for role in roles:
                        schedule.append(dict(cohort="AB"[cohort], case=case, calls=calls,
                                             sample=sample, role=role))
    return {"schedule": schedule, "workflow_count": 520, "engine_call_count": 1300,
            "median_samples": 5, "target_engine_ratio_max": 0.98,
            "control_engine_ratio_max": 1.05, "all_complete_workflow_ratio_max": 1.05,
            "cohorts_required": ["A", "B"], "absolute_noise_allowance": 0,
            "candidate_observers_in_scored_build": False,
            "candidate_build_binding_required_before_scoring": True}


def validate_protocol(path):
    protocol = json.loads(path.read_bytes())
    require(protocol["schema"] == "shardloom.cyclic_existence_scoring_protocol.v1",
            "unsupported scoring protocol")
    intake = protocol["intake_protocol"]
    require(intake["sha256"] == INTAKE_SHA256, "not the original frozen intake")
    check_file(intake)
    original = json.loads(Path(intake["path"]).read_bytes())
    require(protocol["comparison"] == original["comparison"] == comparison_contract(),
            "changed prospective schedule or gates")
    require(protocol["workload"] == original["workload"]
            and workload.canonical(protocol["workload"]) == workload.canonical(workload.manifest()),
            "changed prospective workload")
    require(protocol["libraries"]["ordinary"] == original["library"], "changed ordinary build")
    require(set(protocol["libraries"]) == {"ordinary", "candidate"}, "incomplete library roles")
    require(protocol["libraries"]["ordinary"]["sha256"] != protocol["libraries"]["candidate"]["sha256"],
            "comparison does not have distinct builds")
    for record in [*protocol["libraries"].values(), *protocol["builds"].values(),
                   *protocol["checks"].values()]:
        check_file(record)
    require(set(protocol["builds"]) == {"ordinary", "candidate"}, "missing build provenance")
    for role, record in protocol["builds"].items():
        build = json.loads(Path(record["path"]).read_bytes())
        require(build["status"] == "passed" and build["source_unchanged"] is True
                and build["library"] == protocol["libraries"][role], "invalid build/source provenance")
        artifacts = {item["target"]["name"]: item for item in build["artifacts"]}
        require({"_native", "shardloom_vortex", "shardloom_runtime"} <= set(artifacts),
                "missing native compiler artifacts")
        for name in ("_native", "shardloom_vortex", "shardloom_runtime"):
            artifact = artifacts[name]
            require(artifact["fresh"] is False and artifact["profile"]["test"] is False
                    and artifact["profile"]["opt_level"] == "3", "native release package was not freshly compiled")
    require(protocol["checks"], "missing candidate verification receipts")
    for record in protocol["checks"].values():
        check = json.loads(Path(record["path"]).read_bytes())
        require(check["status"] == "passed" and check["returncode"] == 0
                and check["process_group_drained"] is True, "verification failed or did not drain")
    for name, expected in protocol["drivers"].items():
        require(identity(Path(__file__).parent / name) == expected, f"driver changed: {name}")
    require({"native_cyclic_existence_workload.py", "native_cyclic_existence_compare.py",
             "query_timing_report.py"} <= set(protocol["drivers"]), "missing frozen driver")
    require(protocol["python_sources"], "missing public Python source binding")
    for name, expected in protocol["python_sources"].items():
        require(identity(Path(__file__).resolve().parents[1] / name) == expected,
                f"public Python source changed: {name}")
    return protocol


def load_native(library):
    import shardloom  # noqa: F401: establish the normal extension package parent
    spec = importlib.util.spec_from_file_location("shardloom._native", library)
    require(spec is not None and spec.loader is not None, "native module cannot be loaded")
    native = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = native
    spec.loader.exec_module(native)
    return native


def measure(native, cell, evidence=None):
    fixture_started = time.perf_counter()
    inputs = workload.sources(cell["case"])
    wanted = list(workload.reference(cell["case"], inputs))
    expected_schema = workload.output_schema(cell["case"])
    source_identities = {name: workload.digest_rows(rows) for name, rows in inputs.items()}
    fixture_seconds = time.perf_counter() - fixture_started

    session, plan, result = None, None, None
    bindings, observations = [], []
    if evidence is None:
        evidence = {}
    evidence.update(cell=cell, sources=source_identities, calls=observations,
                    diagnostic_fixture_seconds=fixture_seconds)
    started = time.perf_counter()
    try:
        session = native.Session(native.ExecutionResources(workload.GRANT, 1))
        initial = session.snapshot()
        evidence["initial"] = initial
        for name, rows in inputs.items():
            fields = [(column, json.dumps(dtype)) for column, dtype in workload.source_fields(cell["case"], name)]
            bindings.append((workload.uri(cell["case"], name), session.from_rows(rows, fields=fields)))
        source_credits = session.snapshot()["reserved_bytes"]
        evidence["source_credits"] = source_credits
        plan = session.plan(workload.plan(native, cell["case"]), sources=bindings)
        plan.prepare()
        prepared_credits = session.snapshot()["reserved_bytes"]
        evidence["prepared_credits"] = prepared_credits
        for call in range(cell["calls"]):
            collect_started = time.perf_counter()
            result = plan.collect()
            collect_seconds = time.perf_counter() - collect_started
            report, schema = result.report, json.loads(result.schema_json)
            rows, delivery = result.row_export()
            result = None
            observations.append({"call": call, "collect_seconds": collect_seconds,
                                 "report": report, "schema": schema, "delivery": delivery,
                                 "raw_rows": rows, "after_result_release": session.snapshot()})
        plan.release_prepared()
        plan = None
        gc.collect()
        after_prepared_release = session.snapshot()
        evidence["after_prepared_release"] = after_prepared_release
    finally:
        result = None
        if plan is not None:
            plan.release_prepared()
        plan = None
        bindings.clear()
        gc.collect()
        if session is not None:
            session.close()
            evidence["final"] = session.snapshot()
    require(session is not None, "session construction failed")
    final = session.snapshot()
    require(final["reserved_bytes"] == 0, "final native credits did not drain")
    complete_seconds = time.perf_counter() - started
    evidence["complete_workflow_seconds"] = complete_seconds

    # Full comparisons and hashes are deliberately outside the frozen workflow
    # clock. Native report/schema access and complete row export are inside it.
    validation_started = time.perf_counter()
    require(initial["reserved_bytes"] == 0, "new session is not empty")
    require(after_prepared_release["reserved_bytes"] == source_credits, "prepared credits did not drain")
    for observation in observations:
        rows = observation["raw_rows"]
        require(rows == wanted, f"changed or incomplete ordered values: {cell}")
        require(observation["schema"] == expected_schema, f"changed schema: {cell}")
        require(observation["delivery"]["complete"], f"incomplete row export: {cell}")
        report = observation["report"]
        require(report["external_engine_invoked"] == "false" and report["fallback_attempted"] == "false",
                "external execution invoked")
        require(report["resident_relational_declaration_reused"] == "true", "not prepared execution")
        timing = query_phase_timings(report, observation["collect_seconds"])
        observation.update(result=workload.digest_rows(rows), ordered_values_equal=True,
                           engine_execution_seconds=timing["engine_execution_seconds"])
        require(observation["after_result_release"]["reserved_bytes"] == prepared_credits,
                "execution owners survived result release")
        del observation["raw_rows"]
    evidence.update(complete_workflow_seconds=complete_seconds,
                    engine_execution_seconds=sum(item["engine_execution_seconds"] for item in observations),
                    diagnostic_validation_seconds=time.perf_counter() - validation_started)
    return evidence


def nonnegative(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0


def verify_workflow(result, cell, expected):
    require(result["cell"] == cell, "workflow order or identity changed")
    require(result["sources"] == {name: {key: source[key] for key in ("rows", "sha256")}
                                  for name, source in expected["sources"].items()}, "workflow source changed")
    require(result["initial"]["reserved_bytes"] == 0 and result["final"]["reserved_bytes"] == 0,
            "nonzero session boundary")
    require(result["after_prepared_release"]["reserved_bytes"] == result["source_credits"],
            "plan ownership did not drain")
    require(type(result["source_credits"]) is int and type(result["prepared_credits"]) is int
            and 0 < result["source_credits"] <= result["prepared_credits"], "invalid retained ownership")
    require(len(result["calls"]) == cell["calls"], "missing native call")
    for index, call in enumerate(result["calls"]):
        require(call["call"] == index and call["ordered_values_equal"] is True,
                "missing ordered value comparison")
        require(call["result"] == {key: expected["expected"][key] for key in ("rows", "sha256")}
                and call["schema"] == expected["expected"]["schema"],
                "changed complete result or schema")
        require(call["delivery"]["complete"] is True, "incomplete delivery")
        require(call["after_result_release"]["reserved_bytes"] == result["prepared_credits"],
                "call owner leak")
        report = call["report"]
        require(report["external_engine_invoked"] == report["fallback_attempted"] == "false"
                and report["resident_relational_declaration_reused"] == "true", "changed native route")
        require(nonnegative(call["collect_seconds"]) and nonnegative(call["engine_execution_seconds"]),
                "invalid native clock")
        timing = query_phase_timings(report, call["collect_seconds"])
        require(timing["engine_execution_seconds"] == call["engine_execution_seconds"],
                "engine time is not derived from the native clock")
    require(nonnegative(result["complete_workflow_seconds"]) and nonnegative(result["engine_execution_seconds"]),
            "invalid workflow clock")
    require(result["engine_execution_seconds"] == sum(call["engine_execution_seconds"] for call in result["calls"]),
            "changed engine sum")
    require(result["complete_workflow_seconds"] >= sum(call["collect_seconds"] for call in result["calls"]),
            "workflow excludes native collect work")


def score(workflows, protocol):
    require(protocol["comparison"] == comparison_contract(), "changed prospective comparison contract")
    schedule = protocol["comparison"]["schedule"]
    require(len(workflows) == len(schedule) == 520, "incomplete comparison")
    expected = {cell["case"]: cell for cell in protocol["workload"]["cells"]}
    for result, cell in zip(workflows, schedule):
        verify_workflow(result, cell, expected[cell["case"]])
    rows = []
    for cohort in ("A", "B"):
        for case in workload.CASES:
            for calls in (1, 4):
                medians = {}
                for role in ("ordinary", "candidate"):
                    selected = [result for result in workflows if result["cell"]["cohort"] == cohort
                                and result["cell"]["case"] == case and result["cell"]["calls"] == calls
                                and result["cell"]["role"] == role]
                    require(len(selected) == 5, "missing median samples")
                    medians[role] = {clock: statistics.median(result[clock] for result in selected)
                                     for clock in ("engine_execution_seconds", "complete_workflow_seconds")}
                ratios = {}
                for clock in ("engine_execution_seconds", "complete_workflow_seconds"):
                    baseline = medians["ordinary"][clock]
                    require(baseline > 0, "zero ordinary median cannot support a ratio")
                    ratios[clock] = medians["candidate"][clock] / baseline
                target = case in workload.TARGETS
                engine_limit = 0.98 if target else 1.05
                rows.append({"cohort": cohort, "case": case, "calls": calls, "target": target,
                             "medians": medians, "ratios": ratios,
                             "engine_pass": ratios["engine_execution_seconds"] <= engine_limit,
                             "complete_workflow_pass": ratios["complete_workflow_seconds"] <= 1.05})
    return {"cells": rows, "passed": all(row["engine_pass"] and row["complete_workflow_pass"] for row in rows),
            "workflow_count": len(workflows), "engine_call_count": sum(len(row["calls"]) for row in workflows)}


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")


def worker(args):
    protocol = validate_protocol(args.protocol)
    cell = protocol["comparison"]["schedule"][args.cell]
    library = protocol["libraries"][cell["role"]]
    native = load_native(library["path"])
    result = {"schema": SCHEMA, "status": "running", "index": args.cell,
              "protocol": identity(args.protocol), "library": library, "workflow": {}}
    try:
        with patch("subprocess.Popen", side_effect=AssertionError("execution child process")):
            measure(native, cell, result["workflow"])
        expected = next(item for item in protocol["workload"]["cells"] if item["case"] == cell["case"])
        verify_workflow(result["workflow"], cell, expected)
        result["status"] = "passed"
    except BaseException as error:
        result.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        # A stopped workflow keeps completed reports and actual row digests;
        # never invent a successful equality assertion for an interrupted check.
        for call in result["workflow"].get("calls", []):
            if "raw_rows" in call:
                call["result"] = workload.digest_rows(call.pop("raw_rows"))
        write_json(args.output, result)


def run(args):
    protocol = validate_protocol(args.protocol)
    require(not args.output.exists(), "comparison output already exists")
    args.output.mkdir(parents=True)
    summary = {"schema": SCHEMA, "status": "running", "protocol": identity(args.protocol),
               "workflows": [], "workflow_receipts": []}
    try:
        for index, cell in enumerate(protocol["comparison"]["schedule"]):
            path = args.output / f"workflow-{index:03d}.json"
            log = args.output / f"workflow-{index:03d}.log"
            started = time.perf_counter()
            with log.open("xb") as stream:
                child = subprocess.run([sys.executable, "-B", str(Path(__file__).resolve()),
                    "worker", "--protocol", str(args.protocol.resolve()), "--cell", str(index),
                    "--output", str(path.resolve())], stdout=stream, stderr=subprocess.STDOUT, timeout=120)
            require(child.returncode == 0, f"workflow {index} failed; preserved {log.name}")
            record = json.loads(path.read_bytes())
            require(record["status"] == "passed" and record["index"] == index
                    and record["protocol"] == identity(args.protocol)
                    and record["library"] == protocol["libraries"][cell["role"]], "invalid child receipt")
            summary["workflows"].append(record["workflow"])
            summary["workflow_receipts"].append({"index": index, "result": identity(path),
                "log": identity(log), "diagnostic_process_seconds": time.perf_counter() - started})
            print(json.dumps({"completed_workflows": index + 1, **cell}), flush=True)
        validate_protocol(args.protocol)
        summary.update(status="passed", decision=score(summary["workflows"], protocol))
    except BaseException as error:
        summary.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        write_json(args.output / "comparison.json", summary)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("run", "worker"))
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cell", type=int)
    args = parser.parse_args()
    require((args.mode == "worker") == (args.cell is not None), "cell belongs only to a fresh worker")
    (worker if args.mode == "worker" else run)(args)
