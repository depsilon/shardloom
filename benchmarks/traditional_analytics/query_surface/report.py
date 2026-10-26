# SPDX-License-Identifier: Apache-2.0
"""Family-balanced coverage and raw observation accounting; no hidden failures."""
from __future__ import annotations

from collections import Counter
import math

from .specification import FAMILIES, SURFACES


STATUSES = {"passed", "incorrect", "engine_rejected", "engine_error", "adapter_gap", "reference_gap",
            "harness_error", "comparison_inconclusive", "crashed", "timed_out", "guard_failed",
            "proposed", "unexecuted"}


def summarize(cases, observations, surfaces):
    catalog = {case.id: case for case in cases}
    if len(catalog) != len(cases):
        raise ValueError("duplicate case identity")
    if not surfaces or len(set(surfaces)) != len(surfaces) or set(surfaces) - set(SURFACES):
        raise ValueError("summary requires unique real public surfaces")
    rows = {}
    for row in observations:
        key = row["case_id"], row["surface"]
        if key in rows or row["case_id"] not in catalog or row["surface"] not in surfaces:
            raise ValueError("duplicate or unknown observation identity")
        if row["status"] not in STATUSES:
            raise ValueError("unclassified observation status")
        if catalog[row["case_id"]].relation is None:
            raise ValueError("proposed capability cannot receive an executable observation")
        rows[key] = row
    families = {}
    for family in FAMILIES:
        members = [case for case in cases if case.family == family]
        counts = Counter()
        engine = []
        complete_cases = 0
        routes = Counter()
        unavailable_routes = 0
        for case in members:
            all_passed = case.relation is not None
            for surface in surfaces:
                row = rows.get((case.id, surface))
                status = row["status"] if row else "proposed" if case.relation is None else "unexecuted"
                counts[status] += 1
                all_passed &= status == "passed"
                if row:
                    route = (row.get("diagnostics") or {}).get("strategies", {}).get("relational_provider_selection_reason")
                    if route:
                        routes[route] += 1
                    else:
                        unavailable_routes += 1
                value = row.get("engine_execution_seconds") if row and status == "passed" else None
                if value is not None:
                    if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
                        raise ValueError("engine time must be a finite nonnegative observation")
                    engine.append(value)
            complete_cases += all_passed
        families[family] = {"semantic_cases": len(members), "archetypes": len({case.archetype for case in members}),
            "case_surface_outcomes": dict(sorted(counts.items())), "all_selected_surfaces_passed_cases": complete_cases,
            "all_selected_surfaces_passed_fraction": complete_cases / len(members) if members else None,
            "correct_engine_clock_samples": len(engine),
            "observed_provider_reasons": dict(sorted(routes.items())),
            "observations_without_provider_reason": unavailable_routes,
            "engine_seconds_sum_of_recorded_correct_calls": sum(engine) if engine else None,
            "engine_clock_coverage_complete": bool(engine) and len(engine) == counts["passed"]}
    fractions = [row["all_selected_surfaces_passed_fraction"] for row in families.values()
                 if row["all_selected_surfaces_passed_fraction"] is not None]
    return {"schema_version": 1, "families": families, "observations": len(rows),
        "family_balanced_complete_surface_fraction": sum(fractions) / len(fractions) if fractions else None,
        "semantic_cases": len(cases), "archetypes": len({case.archetype for case in cases}),
        "surfaces": list(surfaces), "comparison_engine_invoked": False,
        "relative_competitor_latency": None, "latency_targets": "not_set_before_baseline_and_noise_observations",
        "scope": "declared corpus, selected seeds/layouts/resources only; proposed and unexecuted cases remain counted"}


def markdown(summary):
    lines = ["# Native broad-query observation", "",
        "Each outcome below is one complete case through one selected public interface. "
        "Case counts exclude extra interfaces and repetitions. Missing counters remain unavailable. "
        "External comparison observations are reported separately.", "",
        "| Family | Cases | Passed | Incorrect | Rejected/error | Adapter/reference gap | Proposed/unexecuted | Other failure |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for name, family in summary["families"].items():
        c = Counter(family["case_surface_outcomes"])
        known = {"passed", "incorrect", "engine_rejected", "engine_error", "adapter_gap", "reference_gap", "proposed", "unexecuted"}
        lines.append(f"| {name} | {family['semantic_cases']} | {c['passed']} | {c['incorrect']} | "
            f"{c['engine_rejected'] + c['engine_error']} | {c['adapter_gap'] + c['reference_gap']} | "
            f"{c['proposed'] + c['unexecuted']} | {sum(value for key, value in c.items() if key not in known)} |")
    lines += ["", "Timings, exact requests, complete expected/actual values, native reports and process observations "
              "remain in the JSON records. No missing native engine clock is replaced with another clock.", ""]
    return "\n".join(lines)
