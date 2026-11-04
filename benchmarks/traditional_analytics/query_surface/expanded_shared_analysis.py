# SPDX-License-Identifier: Apache-2.0
"""Check published request-shape arithmetic against independently replayed observations."""

from collections import defaultdict
import math

from query_surface.expanded_replay import require


def validate_groups(records, groups, fields):
    buckets = defaultdict(list)
    for row in records:
        buckets[tuple(row.get(key) for key in fields)].append(row)
    require(len(groups) == len(buckets)
            and {tuple(row.get(key) for key in fields) for row in groups} == buckets.keys(),
            "analysis group inventory differs")
    for group in groups:
        rows = buckets[tuple(group.get(key) for key in fields)]
        before = math.fsum(row["original_engine_seconds"] for row in rows)
        after = math.fsum(row["updated_engine_seconds"] for row in rows)
        require(group["cases"] == len(rows) and group["original_engine_seconds"] == before
                and group["updated_engine_seconds"] == after and group["delta_engine_seconds"] == after - before,
                "analysis group engine total differs")
        if "delta_percent_of_original" in group:
            expected = 100 * (after - before) / before if before else None
            require(group["delta_percent_of_original"] == expected, "analysis group percentage differs")
        if "gains" in group:
            require(group["gains"] == sum(r["delta_engine_seconds"] < 0 for r in rows)
                    and group["losses"] == sum(r["delta_engine_seconds"] > 0 for r in rows),
                    "analysis group directions differ")


def validate_analysis(analysis, observations, design, inputs):
    records = []
    for seq, (observed, spec) in enumerate(zip(observations, design["cases"], strict=True)):
        before, after = observed["original"]["engine_seconds"], observed["updated"]["engine_seconds"]
        require(observed["id"] == spec["id"], "analysis request order differs")
        records.append(dict(seq=seq, id=spec["id"], family=spec["family"], profile=spec["profile"],
                            profile_spec=inputs["profiles"][spec["profile"]]["spec"], surface=spec["surface"],
                            archetype=spec.get("archetype"), cell=spec.get("cell", {}),
                            native_format=spec.get("native_format"), partition=spec.get("partition"),
                            original_engine_seconds=before, updated_engine_seconds=after,
                            delta_engine_seconds=after - before))
    ordered = sorted(records, key=lambda r: (-r["updated_engine_seconds"], r["seq"]))
    regressed = sorted(records, key=lambda r: (-r["delta_engine_seconds"], r["seq"]))
    require(analysis["top20_expensive_current_native_engine_cases"] == ordered[:20]
            and analysis["top20_absolute_native_engine_regressions"] == regressed[:20],
            "analysis top requests differ")
    definitions = (
        ("native_engine_totals_by_family", ("family",)),
        ("native_engine_totals_by_profile", ("profile",)),
        ("native_engine_totals_by_surface", ("surface",)),
        ("native_engine_totals_by_archetype", ("archetype",)),
        ("native_engine_totals_by_archetype_and_surface", ("archetype", "surface")),
        ("native_engine_totals_by_archetype_and_native_format", ("archetype", "native_format")),
        ("largest_regressions_by_family_profile_surface", ("family", "profile", "surface")),
    )
    for name, fields in definitions:
        validate_groups(records, analysis[name], fields)
    for field in ("family", "profile", "surface"):
        validate_groups(records, analysis["largest_regression_groupings"]["by_" + field], (field,))
    profiles = {name: value["spec"] for name, value in inputs["profiles"].items()}
    require(analysis["case_scale_and_rows"]["profile_specs"] == profiles
            and analysis["case_scale_and_rows"]["profile_case_counts"]
            == {name: sum(r["profile"] == name for r in records) for name in profiles}
            and all(row["profile_spec"] == profiles[row["profile"]] for row in analysis["native_engine_totals_by_profile"]),
            "analysis input scale differs")
    wide = [r for r in records if r["cell"].get("scope") == "population" and r["cell"].get("payload") == "wide"]
    wide_summary = analysis["population_wide_ordering_controls_grouped_payload_scope_sort"]
    require(wide_summary["cases"] == len(wide) and wide_summary["observations"] == wide, "wide ranking selection differs")
    validate_groups([dict(r, **r["cell"]) for r in wide], wide_summary["totals_by_payload_scope_sort"], ("payload", "scope", "sort"))
    outer = [r for r in records if r["archetype"] in ("right_outer_group", "full_outer_group")]
    outer_ids = {r["id"] for r in outer}
    outer_summary = analysis["right_full_outer_group_regressions_by_function_arg_scope"]
    validate_groups(outer, outer_summary["totals_by_archetype"], ("archetype",))
    require(outer_summary["regressed_cases"] == [r for r in regressed if r["id"] in outer_ids and r["delta_engine_seconds"] > 0],
            "outer join regression selection differs")
    for name in ("right_outer_group", "full_outer_group"):
        subset = [dict(r, **r["cell"]) for r in outer if r["archetype"] == name]
        validate_groups(subset, outer_summary["grouped_cells"][name], ("function", "arg", "scope"))
        validate_groups([r for r in subset if r["delta_engine_seconds"] > 0],
                        outer_summary["regression_only_grouped_cells"][name], ("function", "arg", "scope"))
    before = math.fsum(r["original_engine_seconds"] for r in records)
    after = math.fsum(r["updated_engine_seconds"] for r in records)
    native = math.fsum(r["updated"]["workflow_seconds"] for r in observations)
    polars = math.fsum(r["polars"]["workflow_seconds"] for r in observations)
    engine = analysis["native_engine"]
    require(engine["cases"] == len(records) == 10000 and engine["original_engine_seconds"] == before
            and engine["updated_engine_seconds"] == after and engine["delta_engine_seconds"] == after - before
            and engine["delta_percent_of_original"] == 100 * (after - before) / before
            and engine["engine_share_of_public_workflow_seconds"] == after / native
            and engine["gains_faster_updated"] == sum(r["delta_engine_seconds"] < 0 for r in records)
            and engine["losses_slower_updated"] == sum(r["delta_engine_seconds"] > 0 for r in records)
            and engine["ties"] == sum(r["delta_engine_seconds"] == 0 for r in records),
            "analysis complete engine arithmetic differs")
    public = analysis["native_vs_saved_polars_workflow_clock_only"]
    require(public["cases"] == 10000 and public["native_workflow_seconds"] == native
            and public["saved_polars_workflow_seconds"] == polars
            and public["native_over_polars_workflow_ratio"] == native / polars
            and public["native_slower_cases"] == sum(r["updated"]["workflow_seconds"] > r["polars"]["workflow_seconds"] for r in observations)
            and public["native_faster_cases"] == sum(r["updated"]["workflow_seconds"] < r["polars"]["workflow_seconds"] for r in observations)
            and public["ties"] == sum(r["updated"]["workflow_seconds"] == r["polars"]["workflow_seconds"] for r in observations),
            "analysis complete-request comparison differs")
    residual = math.fsum(r["updated"]["workflow_seconds"] - r["updated"]["engine_seconds"] for r in observations)
    require(analysis["native_public_workflow_minus_native_engine"]["workflow_minus_engine_seconds"] == residual,
            "analysis complete-request residual differs")
    families = {r["family"] for r in observations}
    require(len(public["by_family"]) == len(families)
            and {row["family"] for row in public["by_family"]} == families,
            "analysis complete-request family inventory differs")
    for group in public["by_family"]:
        sample = [r for r in observations if r["family"] == group["family"]]
        native = math.fsum(r["updated"]["workflow_seconds"] for r in sample)
        polars = math.fsum(r["polars"]["workflow_seconds"] for r in sample)
        require(group["cases"] == len(sample) and group["native_workflow_seconds"] == native
                and group["polars_workflow_seconds"] == polars
                and group["native_minus_polars_workflow_seconds"] == native - polars
                and group["native_workflow_over_polars_workflow"] == native / polars
                and group["native_slower_cases"] == sum(r["updated"]["workflow_seconds"] > r["polars"]["workflow_seconds"] for r in sample)
                and group["native_faster_cases"] == sum(r["updated"]["workflow_seconds"] < r["polars"]["workflow_seconds"] for r in sample)
                and group["ties"] == sum(r["updated"]["workflow_seconds"] == r["polars"]["workflow_seconds"] for r in sample),
                "analysis complete-request family arithmetic differs")
