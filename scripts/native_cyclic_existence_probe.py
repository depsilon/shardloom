#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Observe the finite workload through the ordinary public native binding.

Run only under the repository's serial native workload/storage guard. This is
an intake observation, not a scored candidate benchmark or a fallback executor.
"""
import argparse
import gc
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import time
from unittest.mock import patch

import native_cyclic_existence_workload as workload
from query_timing_report import query_phase_timings


def identity(path):
    data = path.read_bytes()
    return {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def require(condition, detail):
    if not condition:
        raise ValueError(detail)


def observe(native, case, *, inner):
    inputs = workload.sources(case)
    wanted = list(workload.inner_reference(inputs) if inner else workload.reference(case, inputs))
    expected_schema = ({"Struct": [{"names": ["witness_b", "witness_a"],
                                   "dtypes": [{"Primitive": ["i64", True]}] * 2}, False]}
                       if inner else workload.output_schema(case))
    session = native.Session(native.ExecutionResources(workload.GRANT, 1))
    bindings = []
    plan = None
    observations = []
    try:
        require(session.snapshot()["reserved_bytes"] == 0, "nonempty new session")
        for name, rows in inputs.items():
            if inner and name == "r":
                continue
            fields = [(column, json.dumps(dtype)) for column, dtype in workload.source_fields(case, name)]
            bindings.append((workload.uri(case, name), session.from_rows(rows, fields=fields)))
        source_credits = session.snapshot()["reserved_bytes"]
        plan = session.plan(workload.inner_plan(native, case) if inner else workload.plan(native, case),
                            sources=bindings)
        plan.prepare()
        prepared_credits = session.snapshot()["reserved_bytes"]
        for call in range(1 if inner else 2):
            started = time.perf_counter()
            result = plan.collect()
            collect_seconds = time.perf_counter() - started
            report, schema = result.report, json.loads(result.schema_json)
            rows, delivery = result.row_export()
            require(delivery["complete"] and rows == wanted, f"incomplete or changed ordered rows: {case}")
            require(schema == expected_schema, f"schema mismatch: {case}")
            require(report["external_engine_invoked"] == "false" and report["fallback_attempted"] == "false",
                    f"external execution: {case}")
            require(report["resident_relational_declaration_reused"] == "true", f"unprepared execution: {case}")
            # Use the same native nanosecond conversion/clock-boundary checks as
            # Full43. The enclosing duration here is one public collect call,
            # so do not label the helper's process residual as process time.
            timing = query_phase_timings(report, collect_seconds)
            observed = {"call": call, "result": workload.digest_rows(rows), "schema": schema,
                        "ordered_values_equal": True, "report": report, "delivery": delivery,
                        "engine_execution_seconds": timing["engine_execution_seconds"],
                        "collect_seconds": collect_seconds}
            del result, rows
            observed["after_result_release"] = session.snapshot()
            require(observed["after_result_release"]["reserved_bytes"] == prepared_credits,
                    f"execution credits retained: {case}")
            observations.append(observed)
        plan.release_prepared()
        plan = None
        gc.collect()
        after_prepared_release = session.snapshot()
        require(after_prepared_release["reserved_bytes"] == source_credits,
                f"prepared credits retained after plan release: {case}")
    finally:
        if plan is not None:
            plan.release_prepared()
        plan = None
        bindings.clear()
        gc.collect()
        session.close()
    final = session.snapshot()
    require(final["reserved_bytes"] == 0, f"final credit leak: {case}")
    return {"case": case, "inner": inner, "source_credits": source_credits,
            "prepared_credits": prepared_credits, "after_prepared_release": after_prepared_release,
            "calls": observations, "final": final}


def run(args):
    protocol = json.loads(args.protocol.read_bytes())
    require(identity(args.library) == {key: protocol["library"][key] for key in ("bytes", "sha256")},
            "native library differs from protocol")
    for name, expected in protocol["drivers"].items():
        require(identity(Path(__file__).parent / name) == expected, f"driver changed after freeze: {name}")
    require(workload.canonical(workload.manifest()) == workload.canonical(protocol["workload"]),
            "workload differs from frozen manifest")
    require(not args.output.exists(), "observation output already exists")
    import shardloom  # noqa: F401: establish the extension's normal package parent
    spec = importlib.util.spec_from_file_location("shardloom._native", args.library)
    require(spec is not None and spec.loader is not None, "native module cannot be loaded")
    native = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = native
    spec.loader.exec_module(native)
    result = {"schema": "shardloom.cyclic_existence_intake.v1", "status": "running",
              "protocol": identity(args.protocol), "library": identity(args.library), "cells": []}
    try:
        with patch("subprocess.Popen", side_effect=AssertionError("execution child process")):
            for case in workload.CASES:
                outer = observe(native, case, inner=False)
                inner = observe(native, case, inner=True) if case in workload.EXISTENCE else None
                result["cells"].append({"case": case, "outer": outer, "inner": inner})
                print(json.dumps({"case": case, "outer_rows": outer["calls"][0]["result"]["rows"],
                                  "inner_rows": inner["calls"][0]["result"]["rows"] if inner else None}), flush=True)
        witnesses = []
        for cell in result["cells"]:
            if cell["case"] in workload.TARGETS:
                child_rows = cell["inner"]["calls"][0]["result"]["rows"]
                outer_rows = cell["outer"]["calls"][0]["result"]["rows"]
                if child_rows >= 65536 and child_rows >= 16 * max(outer_rows, 1):
                    witnesses.append(cell["case"])
        result.update(status="passed", opportunity={"qualifying_targets": witnesses,
                      "admitted": len(witnesses) >= 2, "performance_claim": False})
    except BaseException as error:
        result.update(status="failed", error=f"{type(error).__name__}: {error}")
        raise
    finally:
        with args.output.open("x") as stream:
            json.dump(result, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    run(parser.parse_args())
