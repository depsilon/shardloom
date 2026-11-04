# SPDX-License-Identifier: Apache-2.0
"""Exact engine totals and separate public-request clocks for the combined cohort."""

from collections import Counter, defaultdict
import math
import re

from query_surface.expanded_replay import require

CLOCKS = ("setup_and_declaration_seconds", "collect_call_seconds", "delivery_seconds",
          "complete_workflow_seconds")
FIXED = tuple("""payload_prepared_selections payload_prepared_coordinates
payload_scalar_coordinates payload_rebuilt_selections payload_rebuilt_coordinates
payload_missing_fields_validated payload_missing_rows_validated join_payload_batches
join_payload_rows join_payload_retained_cells join_payload_omitted_cells
sort_payload_gathers sort_payload_selected_cells sort_payload_retained_cells
sort_payload_omitted_cells sort_payload_buffer_bytes payload_typed_validation_columns
payload_typed_validation_rows expression_dictionary_completed_batches
expression_dictionary_completed_rows expression_dictionary_computed_values
expression_dictionary_cache_hits expression_dictionary_resource_declines""".split())
WINDOW = tuple("""window_reused_partition_spans window_reused_partition_rows
window_peer_scan_partitions window_peer_scan_rows window_position_only_partitions
window_position_only_rows""".split())
ORDERING = tuple("""streaming_ordering_prefix_batches streaming_ordering_prefix_rows_validated
streaming_ordering_prefix_peak_retained_buffer_bytes""".split())
NAMED = ("v2_right_outer_group_c8396388e8c70075", "v2_full_outer_group_54ee753adf8f1b20",
         "v2_product_left_group_612dc5dd63944870", "v2_rank_hidden_payload_ade4e107deda3e58",
         "v2_rank_hidden_payload_5dcca5478175cafd")


def work_counters(fields):
    proof = fields.get("local_primitive_native_io_pushdown_proof_basis", "")
    found = {}
    for name in (*FIXED, *WINDOW, *ORDERING):
        matches = re.findall(r"(?<!\w)" + name + r"=([^\s;]+)(?=\s|;|$)", proof)
        require(len(matches) <= 1, "duplicate native work counter: " + name)
        if matches:
            require(re.fullmatch(r"[0-9]+", matches[0]) is not None,
                    "invalid native work counter: " + name)
            found[name] = int(matches[0])
            require(found[name] <= (1 << 64) - 1, "overflowed native work counter")
    for version, names in (("native_payload_work_version", FIXED), ("native_window_work_version", WINDOW)):
        present = set(names) & found.keys()
        require(not present or present == set(names), "partial native work counter group")
        versions = re.findall(r"(?<!\w)" + version + r"=([^\s;]+)(?=\s|;|$)", proof)
        require(versions == (["1"] if present else []), "native work version differs")
    return found


def checked_clocks(report):
    fields, clocks = report["fields"], report["clocks"]
    raw = fields["native_query_execution_nanos"]
    require(re.fullmatch(r"[0-9]+", str(raw)) is not None, "missing native engine nanoseconds")
    nanos = int(raw)
    require(nanos > 0 and clocks["engine_timing_reported"] is True
            and nanos / 1e9 == clocks["engine_execution_seconds"], "native engine clock differs")
    for name in CLOCKS:
        value = clocks[name]
        require(type(value) in (int, float) and math.isfinite(value) and value >= 0,
                "invalid public request clock: " + name)
    require(math.isclose(math.fsum(clocks[k] for k in CLOCKS[:3]),
                         clocks[CLOCKS[3]], rel_tol=1e-10, abs_tol=1e-9),
            "public clock components differ from complete request")
    require(clocks["collect_call_seconds"] + 1e-9 >= nanos / 1e9,
            "engine execution exceeds enclosing collection")
    return nanos, {name: clocks[name] for name in CLOCKS}


def totals(rows):
    result = {"cases": len(rows)}
    for role in ("baseline", "candidate"):
        nanos = sum(row[role]["engine_nanos"] for row in rows)
        result[role] = dict(engine_nanos=nanos, engine_seconds=nanos / 1e9,
                            **{key: math.fsum(row[role]["clocks"][key] for row in rows) for key in CLOCKS})
        result[role]["collect_outside_engine_seconds"] = math.fsum(
            row[role]["clocks"]["collect_call_seconds"] - row[role]["engine_nanos"] / 1e9 for row in rows)
    result["delta_engine_nanos"] = result["candidate"]["engine_nanos"] - result["baseline"]["engine_nanos"]
    result["engine_faster_cases"] = sum(row["candidate"]["engine_nanos"] < row["baseline"]["engine_nanos"] for row in rows)
    result["engine_slower_cases"] = sum(row["candidate"]["engine_nanos"] > row["baseline"]["engine_nanos"] for row in rows)
    result["engine_equal_cases"] = result["cases"] - result["engine_faster_cases"] - result["engine_slower_cases"]
    return result


def summarize(rows):
    require(len(rows) == len({r["id"] for r in rows}) == 10000, "incomplete result denominator")
    result = totals(rows)
    for dimension in ("family", "profile", "surface"):
        groups = defaultdict(list)
        for row in rows:
            groups[row[dimension]].append(row)
        result[dimension + "_groups"] = {key: totals(value) for key, value in sorted(groups.items())}
    def brief(row):
        return {key: row[key] for key in ("id", "family", "profile", "surface")} | {
            "baseline_engine_nanos": row["baseline"]["engine_nanos"],
            "candidate_engine_nanos": row["candidate"]["engine_nanos"],
            "delta_engine_nanos": row["candidate"]["engine_nanos"] - row["baseline"]["engine_nanos"]}
    result["named_controls"] = [brief(row) for row in rows if row["id"] in NAMED]
    require(len(result["named_controls"]) == len(NAMED), "named controls missing")
    result["largest_engine_regressions"] = sorted(
        (brief(row) for row in rows if row["candidate"]["engine_nanos"] > row["baseline"]["engine_nanos"]),
        key=lambda r: (-r["delta_engine_nanos"], r["id"]))[:20]
    coverage, sums, peaks = Counter(), Counter(), Counter()
    for row in rows:
        for key, value in row["counters"].items():
            coverage[key] += 1
            if "peak_" in key:
                peaks[key] = max(peaks[key], value)
            else:
                sums[key] += value
    result["work_counters"] = {key: {"present_cases": coverage[key], "absent_cases": 10000 - coverage[key],
                                     ("maximum" if "peak_" in key else "sum"): peaks[key] if "peak_" in key else sums[key]}
                               for key in (*FIXED, *WINDOW, *ORDERING)}
    result["retained_combined_candidate"] = result["delta_engine_nanos"] < 0 and all(
        result["candidate"][name] < result["baseline"][name]
        for name in ("setup_and_declaration_seconds", "complete_workflow_seconds"))
    result["attribution_limit"] = "One complete observation of combined changes; no per-change causal speedup or repeat variance claim."
    return result
