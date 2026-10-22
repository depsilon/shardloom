#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Validate paired-workflow evidence and score the frozen cost gates."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import random
import re
import statistics
from numbers import Real
from pathlib import Path
from typing import Any

from native_exchange_result_checks import verify_workflow

MODES = ("native_fragments", "native_assembled", "arrow_none", "arrow_lz4", "arrow_zstd")
ARROW_MODES = ("arrow_none", "arrow_lz4", "arrow_zstd")
CANDIDATE = "native_fragments"
SAMPLES = tuple(range(5))
TARGETS = ("all_null", "constant", "dictionary_small", "dictionary_large", "dictionary_other",
           "run_end", "bitpacked", "nested", "sliced_shared")
NONBENEFITING = ("empty", "tiny", "nullable_plain", "wide_text")
FAMILY_BY_CASE = {"all_null": "constants", "constant": "constants",
    "dictionary_small": "dictionaries", "dictionary_large": "dictionaries",
    "dictionary_other": "dictionaries", "nested": "dictionaries", "sliced_shared": "dictionaries",
    "run_end": "run_ends", "bitpacked": "bitpacking"}
BOOTSTRAP_DRAWS = 10_000
BOOTSTRAP_SEED = 3347
BOOTSTRAP_UPPER_INDEX = 9_499
IDENTITY_KEYS = ("fixture_manifest", "protocol_freeze", "executable", "runner", "result_verifier", "build_freeze")
SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")


def _require(condition: bool, detail: str) -> None:
    if not condition:
        raise ValueError(detail)


def _object(value: Any, path: str) -> dict[str, Any]:
    _require(isinstance(value, dict), f"{path}: expected an object")
    return value


def _array(value: Any, path: str) -> list[Any]:
    _require(isinstance(value, list), f"{path}: expected an array")
    return value


def _number(value: Any, path: str, *, positive: bool = False) -> float:
    _require(not isinstance(value, bool) and isinstance(value, Real), f"{path}: expected a number")
    value = float(value)
    _require(math.isfinite(value), f"{path}: expected a finite number")
    _require(value > 0 if positive else value >= 0,
             f"{path}: expected a {'positive' if positive else 'nonnegative'} number")
    return value


def _identity_sha(report: dict[str, Any], key: str) -> str:
    value = _object(report.get(key), f"report.{key}").get("sha256")
    _require(isinstance(value, str) and SHA256_RE.fullmatch(value) is not None,
             f"report.{key}.sha256: expected lowercase SHA-256")
    return value


def paired_metrics(candidate: list[dict[str, float]], control: list[dict[str, float]]) -> dict[str, Any]:
    """Compute ratios and differences per paired sample, then their separate medians."""
    _require(len(candidate) == len(control) == 5, "paired metric comparison requires five samples")
    result: dict[str, Any] = {}
    for metric in ("workflow", "engine", "wire", "peer_cpu"):
        left = [row[metric] for row in candidate]
        right = [row[metric] for row in control]
        differences = [a - b for a, b in zip(left, right)]
        ratios = [a / b for a, b in zip(left, right)]
        result[f"{metric}_differences"] = differences
        result[f"{metric}_ratios"] = ratios
        result[f"median_{metric}_difference"] = statistics.median(differences)
        result[f"median_{metric}_ratio"] = statistics.median(ratios)
    return result


def within_allowance(median_ratio: float, median_difference: float,
                     relative_limit: float, absolute_limit: float) -> bool:
    return median_ratio <= 1 + relative_limit or median_difference <= absolute_limit


def geometric_mean(values: list[float]) -> float:
    _require(bool(values), "geometric mean requires values")
    for i, value in enumerate(values):
        _number(value, f"geometric_mean[{i}]", positive=True)
    return math.exp(math.fsum(math.log(v) for v in values) / len(values))


def bootstrap_upper_ratio(case_ratios: dict[str, list[float]]) -> float:
    _require(set(case_ratios) == set(TARGETS), "bootstrap requires exactly the nine target cases")
    for case in TARGETS:
        values = case_ratios[case]
        _require(isinstance(values, list) and len(values) == 5,
                 f"bootstrap {case}: expected five paired sample ratios")
        for i, value in enumerate(values):
            _number(value, f"bootstrap {case}[{i}]", positive=True)
    rng = random.Random(BOOTSTRAP_SEED)
    draws = []
    for _ in range(BOOTSTRAP_DRAWS):
        medians = []
        for case in TARGETS:
            values = case_ratios[case]
            medians.append(statistics.median(values[rng.randrange(5)] for _ in range(5)))
        draws.append(geometric_mean(medians))
    draws.sort()
    return draws[BOOTSTRAP_UPPER_INDEX]


def distinct_families(winning_cases: list[str] | set[str]) -> set[str]:
    return {FAMILY_BY_CASE[case] for case in winning_cases if case in FAMILY_BY_CASE}


def _check_freeze(manifest: dict[str, Any], freeze: dict[str, Any]) -> list[str]:
    cases = [case["id"] for case in _array(freeze.get("cases"), "freeze.cases")]
    _require(len(cases) == 13 and len(set(cases)) == 13, "freeze must contain 13 unique cases")
    _require(set(cases) == set(TARGETS) | set(NONBENEFITING), "freeze case set differs from gate definition")
    manifest_cases = [case["id"] for case in _array(manifest.get("cases"), "manifest.cases")]
    _require(len(manifest_cases) == 13 and set(manifest_cases) == set(cases),
             "manifest cases do not exactly match freeze")
    _require(tuple(_array(freeze.get("modes"), "freeze.modes")) == MODES,
             "freeze modes differ from the declared five-role order")
    _require(tuple(freeze.get("execution_order", ())) == (0, 1, 2, 1),
             "freeze query order differs from [0, 1, 2, 1]")
    cohorts = _object(freeze.get("cohorts"), "freeze.cohorts")
    _require(cohorts.get("samples_per_case_mode") == 5 and cohorts.get("workflows_per_cohort") == 325,
             "freeze cohort dimensions differ from 5 samples / 325 workflows")
    _require(cohorts.get("bootstrap_resamples") == BOOTSTRAP_DRAWS
             and cohorts.get("bootstrap_seed") == BOOTSTRAP_SEED
             and cohorts.get("upper_order_statistic_zero_based") == BOOTSTRAP_UPPER_INDEX,
             "freeze bootstrap configuration differs from the required rule")
    gates = _object(freeze.get("gates"), "freeze.gates")
    expected_gates = {
        "all_cases_native_workflow_max_increase": {"fraction": 0.05, "rule": "larger allowance", "seconds": 0.005},
        "nonbenefiting_each_arrow_workflow_max_increase": {"fraction": 0.05, "rule": "larger allowance", "seconds": 0.005},
        "all_cases_native_engine_max_increase": {"fraction": 0.05, "rule": "larger allowance", "seconds": 0.0005},
        "target_geometric_mean_workflow_ratio_max_each_arrow": 0.9,
        "target_upper_bootstrap_ratio_exclusive_max_each_arrow": 1.0,
        "winning_target_workflow_ratio_max_each_arrow": 0.9,
        "winning_target_wire_or_combined_peer_cpu_ratio_max_each_arrow": 0.9,
        "distinct_winning_encoded_families_min": 2,
        "candidate_whole_message_assembly_bytes": 0,
        "native_control_whole_message_assembly_bytes": "positive in every case",
    }
    for key, expected in expected_gates.items():
        _require(gates.get(key) == expected, f"freeze.gates.{key} differs from the prospective rule")
    return cases


def _schedule(freeze: dict[str, Any], cohort: str) -> list[tuple[str, str, int, int]]:
    indexed = list(enumerate(case["id"] for case in freeze["cases"]))
    if cohort == "2":
        indexed.reverse()
    result = []
    for case_index, case in indexed:
        for sample in range(5):
            rotate = (case_index + sample) % len(MODES)
            roles = list(MODES[rotate:]) + list(MODES[:rotate])
            if cohort == "2":
                roles.reverse()
            result.extend((case, mode, sample, position) for position, mode in enumerate(roles))
    return result


def _row_metrics(row: dict[str, Any], path: str) -> dict[str, float]:
    workflow = _number(row.get("complete_workflow_seconds"), f"{path}.complete_workflow_seconds", positive=True)
    receiver = _object(row.get("receiver"), f"{path}.receiver")
    consumer = _object(receiver.get("consumer"), f"{path}.receiver.consumer")
    queries = _array(consumer.get("queries"), f"{path}.receiver.consumer.queries")
    _require(len(queries) == 4, f"{path}: expected four engine clock records")
    engine = math.fsum(_number(_object(q, f"{path}.query[{i}]").get("engine_execution_seconds"),
                              f"{path}.query[{i}].engine_execution_seconds") for i, q in enumerate(queries))
    _require(math.isfinite(engine) and engine > 0, f"{path}: summed engine clock must be positive")
    resources = _object(row.get("resources"), f"{path}.resources")
    cpu_parts = []
    for peer in ("sender", "receiver"):
        peer_resources = _object(resources.get(peer), f"{path}.resources.{peer}")
        cpu_parts.extend(_number(peer_resources.get(k), f"{path}.resources.{peer}.{k}")
                         for k in ("user_cpu_seconds", "system_cpu_seconds"))
    peer_cpu = math.fsum(cpu_parts)
    _require(peer_cpu > 0, f"{path}: combined peer CPU must be positive")
    sender = _object(row.get("sender"), f"{path}.sender")
    service = _object(sender.get("service"), f"{path}.sender.service")
    transfers = _array(service.get("transfers"), f"{path}.sender.service.transfers")
    _require(len(transfers) == 1, f"{path}: expected exactly one transfer")
    transfer = _object(transfers[0], f"{path}.transfer")
    send = _object(transfer.get("send"), f"{path}.transfer.send")
    wire = _number(transfer.get("request_application_wire_bytes"), f"{path}.request_application_wire_bytes")
    wire += _number(send.get("application_wire_bytes"), f"{path}.response_application_wire_bytes")
    _require(wire > 0, f"{path}: request plus response wire bytes must be positive")
    return {"workflow": workflow, "engine": engine, "wire": wire, "peer_cpu": peer_cpu}


def _comparison_table(rows: dict[str, dict[str, dict[int, dict[str, float]]]], control: str) -> dict[str, Any]:
    return {case: paired_metrics([modes[CANDIDATE][i] for i in SAMPLES],
                                 [modes[control][i] for i in SAMPLES])
            for case, modes in rows.items()}


def score_cohort(report: dict[str, Any], manifest: dict[str, Any], freeze: dict[str, Any]) -> dict[str, Any]:
    """Validate and score the prospective numeric gates for one complete cohort."""
    report = _object(report, "report")
    manifest = _object(manifest, "manifest")
    freeze = _object(freeze, "freeze")
    _require(report.get("status") == "passed", "report.status must be passed")
    _require(report.get("scope") == "all", "report.scope must be all")
    before = _object(report.get("build_verification_before"), "report.build_verification_before")
    _require(type(before.get("verified_sources")) is int and before["verified_sources"] > 0,
             "build verification must include source files")
    _require(report.get("build_verification_after") == before, "build inputs changed during the cohort")
    cohort = report.get("cohort")
    _require(cohort in {"1", "2"}, "report.cohort must be 1 or 2")
    cases = _check_freeze(manifest, freeze)
    expected = _schedule(freeze, cohort)
    workflows = _array(report.get("workflows"), "report.workflows")
    _require(len(workflows) == len(expected) == 325,
             f"expected exactly 325 workflow records; found {len(workflows)}")
    rows: dict[str, dict[str, dict[int, dict[str, float]]]] = {
        case: {mode: {} for mode in MODES} for case in cases
    }
    seen = set()
    for index, (record, scheduled) in enumerate(zip(workflows, expected)):
        record = _object(record, f"report.workflows[{index}]")
        _require(record.get("status") == "passed", f"report.workflows[{index}].status must be passed")
        config = _object(record.get("configuration"), f"report.workflows[{index}].configuration")
        _require(type(record.get("sample")) is int and type(record.get("role_position")) is int,
                 f"report.workflows[{index}] sample and role position must be integers")
        actual = (config.get("source_id"), config.get("mode"), record.get("sample"), record.get("role_position"))
        _require(actual == scheduled,
                 f"report.workflows[{index}] violates frozen row order/role rotation: expected {scheduled!r}, got {actual!r}")
        key = actual[:3]
        _require(key not in seen, f"duplicate case/mode/sample record {key!r}")
        seen.add(key)
        verify_workflow(record, manifest, freeze)
        rows[key[0]][key[1]][key[2]] = _row_metrics(record, f"report.workflows[{index}]")
    _require(len(seen) == 325, f"expected 325 unique entries; found {len(seen)}")
    for case in cases:
        for mode in MODES:
            _require(set(rows[case][mode]) == set(SAMPLES), f"missing/replaced samples for {case}/{mode}")

    identities = {key: _identity_sha(report, key) for key in IDENTITY_KEYS}
    comparisons = {mode: _comparison_table(rows, mode) for mode in ("native_assembled", *ARROW_MODES)}
    gates = []

    def add_gate(name: str, passed: bool, detail: Any) -> None:
        gates.append({"gate": name, "passed": bool(passed), "detail": detail})

    native = comparisons["native_assembled"]
    gate2 = {}
    for case in cases:
        metric = native[case]
        passed = within_allowance(metric["median_workflow_ratio"], metric["median_workflow_difference"], .05, .005)
        gate2[case] = {"passed": passed, "median_workflow_ratio": metric["median_workflow_ratio"],
                       "median_workflow_difference_seconds": metric["median_workflow_difference"]}
    add_gate("gate_2_all_cases_native_workflow", all(item["passed"] for item in gate2.values()), gate2)

    nonbenefiting = {}
    for mode in ARROW_MODES:
        nonbenefiting[mode] = {}
        for case in NONBENEFITING:
            metric = comparisons[mode][case]
            passed = within_allowance(metric["median_workflow_ratio"], metric["median_workflow_difference"], .05, .005)
            nonbenefiting[mode][case] = {"passed": passed, "median_workflow_ratio": metric["median_workflow_ratio"],
                                         "median_workflow_difference_seconds": metric["median_workflow_difference"]}
        add_gate(f"gate_2_nonbenefiting_{mode}", all(item["passed"] for item in nonbenefiting[mode].values()),
                 nonbenefiting[mode])

    gate3 = {}
    for case in cases:
        metric = native[case]
        passed = within_allowance(metric["median_engine_ratio"], metric["median_engine_difference"], .05, .0005)
        gate3[case] = {"passed": passed, "median_engine_ratio": metric["median_engine_ratio"],
                       "median_engine_difference_seconds": metric["median_engine_difference"]}
    add_gate("gate_3_all_cases_native_engine", all(item["passed"] for item in gate3.values()), gate3)

    gate4 = {}
    for mode in ARROW_MODES:
        ratios = {case: comparisons[mode][case]["workflow_ratios"] for case in TARGETS}
        medians = {case: statistics.median(ratios[case]) for case in TARGETS}
        mean = geometric_mean(list(medians.values()))
        upper = bootstrap_upper_ratio(ratios)
        passed = mean <= .90 and upper < 1.0
        gate4[mode] = {"passed": passed, "target_paired_median_workflow_ratios": medians,
                       "equal_weight_geometric_mean": mean, "bootstrap": {
                           "draws": BOOTSTRAP_DRAWS, "seed": BOOTSTRAP_SEED,
                           "upper_order_statistic_zero_based": BOOTSTRAP_UPPER_INDEX,
                           "upper_ratio": upper}}
        add_gate(f"gate_4_target_geomean_bootstrap_{mode}", passed, gate4[mode])

    gate5_by_case = {}
    winning_cases = set()
    for case in TARGETS:
        controls = {}
        for mode in ARROW_MODES:
            metric = comparisons[mode][case]
            workflow = metric["median_workflow_ratio"] <= .90
            wire = metric["median_wire_ratio"] <= .90
            cpu = metric["median_peer_cpu_ratio"] <= .90
            controls[mode] = {"workflow_ratio": metric["median_workflow_ratio"],
                "wire_ratio": metric["median_wire_ratio"], "peer_cpu_ratio": metric["median_peer_cpu_ratio"],
                "workflow_improved_at_least_10_percent": workflow,
                "wire_or_cpu_improved_at_least_10_percent": wire or cpu,
                "passed": workflow and (wire or cpu)}
        wins = all(item["passed"] for item in controls.values())
        gate5_by_case[case] = {"family": FAMILY_BY_CASE[case], "passed_against_each_arrow_role": wins,
                               "controls": controls}
        if wins:
            winning_cases.add(case)
    winning_families = sorted(distinct_families(winning_cases))
    family_gate = {"passed": len(winning_families) >= 2, "minimum_distinct_families": 2,
                   "winning_cases": sorted(winning_cases), "winning_families": winning_families,
                   "per_target": gate5_by_case}
    add_gate("gate_5_distinct_encoded_families", family_gate["passed"], family_gate)

    return {"status": "evaluated", "scope": "cost_gates_only", "cohort": cohort,
            "workflow_count": len(workflows), "identity_sha256": identities,
            "metrics_by_control_and_case": comparisons, "gates": gates,
            "cost_gates_passed": all(gate["passed"] for gate in gates),
            "correctness_and_retention_claimed": False}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while block := source.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cohort", action="append", required=True, type=Path,
                        metavar="REPORT", help="scored cohort report; specify exactly twice")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--freeze", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if len(args.cohort) != 2:
        parser.error("exactly two --cohort reports are required")
    inputs = [*args.cohort, args.manifest, args.freeze]
    for path in inputs:
        if not path.is_absolute() or not path.is_file():
            parser.error(f"input must be an existing absolute file: {path}")
    if not args.output.is_absolute():
        parser.error("--output must be an absolute path")
    if args.output.exists():
        parser.error(f"output must be fresh: {args.output}")
    if not args.output.parent.is_dir():
        parser.error(f"output parent must exist: {args.output.parent}")

    input_ids = [{"path": str(path), "bytes": path.stat().st_size, "sha256": file_sha256(path)} for path in inputs]
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in args.cohort]
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    freeze = json.loads(args.freeze.read_text(encoding="utf-8"))
    _require({report.get("cohort") for report in reports} == {"1", "2"},
             "--cohort inputs must contain exactly cohort 1 and cohort 2")
    reports.sort(key=lambda report: report["cohort"])
    scored = [score_cohort(report, manifest, freeze) for report in reports]
    for key in IDENTITY_KEYS:
        values = [item["identity_sha256"][key] for item in scored]
        _require(values[0] == values[1], f"cohort {key} SHA-256 identities differ")
    _require(scored[0]["identity_sha256"]["fixture_manifest"] == input_ids[2]["sha256"],
             "reported fixture manifest SHA-256 differs from --manifest")
    _require(scored[0]["identity_sha256"]["protocol_freeze"] == input_ids[3]["sha256"],
             "reported protocol freeze SHA-256 differs from --freeze")
    if [{"path": str(path), "bytes": path.stat().st_size, "sha256": file_sha256(path)} for path in inputs] != input_ids:
        raise ValueError("one or more inputs changed during scoring")
    result = {"status": "evaluated", "scope": "cost_gates_only",
              "cost_gates_passed": all(item["cost_gates_passed"] for item in scored),
              "correctness_and_retention_claimed": False,
              "scorer_sha256": file_sha256(Path(__file__)),
              "result_verifier_sha256": file_sha256(Path(__file__).with_name("native_exchange_result_checks.py")),
              "inputs": input_ids, "cohorts": scored}
    with args.output.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, indent=2, sort_keys=True, allow_nan=False)
        destination.write("\n")
    print(json.dumps({"status": result["status"], "cost_gates_passed": result["cost_gates_passed"],
                      "output": str(args.output)}, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
