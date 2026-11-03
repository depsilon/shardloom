# SPDX-License-Identifier: Apache-2.0
"""Project comparable owner observations without manufacturing measurements."""
from decimal import Decimal

from query_surface.replay import need


DECISION = "local_primitive_native_io_decision_"
REPLAY = "native_vortex_result_export_replay_decision_"
INGEST = "public_workflow_preparation_vortex_ingest_"
WORKER = "local_primitive_aggregate_workers_"
CAPABILITIES = ("metadata_count", "region_exclusion", "existing_order", "correlated_membership",
                "native_replay", "representation_retention")
COHORTS = {"probe-parquet": 7, "probe-vortex": 7, "fresh-preparation": 1,
           "fresh43": 43, "fresh347": 347, "prior-preparation": 1}
STAGES = ("stream_validation", "stream_projection", "stream_arrow_conversion", "stream_reader_lock_wait",
          "stream_ordered_handoff_wait", "text_canonicalize", "text_compact", "text_zstd", "numeric_probe",
          "numeric_compress", "numeric_preserve", "text_dictionary_preserve", "stream_arrow_ownership")
COVERAGE = {"resource_grant": "execution_resource_", "io_decisions": DECISION,
            "writer_replay_decisions": REPLAY, "aggregate_workers": WORKER,
            "ranked_reads": "native_source_ranked_", "ingest_owner": INGEST}

# The source key, unit and owner define each comparable number. Absent fields
# carry a state. Owner-reported zeros survive even when a subpath did no work.
METRICS = {
    "requested_columns": ("native_source_demand_projected_columns", "columns", "demand_reader"),
    "available_columns": ("native_source_demand_available_columns", "columns", "demand_reader"),
    "read_bytes": ("native_source_demand_data_read_bytes", "bytes", "demand_reader"),
    "metadata_read_bytes": ("native_source_demand_metadata_read_bytes", "bytes", "demand_reader"),
    "decoded_rows": ("native_source_demand_rows_decoded", "rows", "demand_reader"),
    "input_batches": ("native_input_batches", "batches", "batch_intake"),
    "max_batch_rows": ("native_input_max_batch_rows", "rows", "batch_intake"),
    "peak_held_batches": ("native_input_max_retained_batches", "batches", "batch_intake"),
    "intake_copied": ("native_input_columnar_buffer_bytes_copied", "visible bytes", "batch_intake"),
    "intake_transferred": ("native_input_columnar_buffer_bytes_transferred", "visible bytes", "batch_intake"),
    "intake_transferred_capacity": ("native_input_columnar_allocation_bytes_transferred", "capacity bytes", "batch_intake"),
    "rank_read": ("native_source_ranked_rank_read_bytes", "bytes", "demand_reader"),
    "payload_read": ("native_source_ranked_payload_read_bytes", "bytes", "demand_reader"),
    "rank_rows": ("native_source_ranked_rank_rows", "rows", "demand_reader"),
    "payload_rows": ("native_source_ranked_payload_rows", "rows", "demand_reader"),
    "selected_rows": ("native_source_ranked_selected_rows", "positions", "demand_reader"),
    "rank_group_visits": ("native_source_ranked_rank_group_visits", "visits", "demand_reader"),
    "payload_group_visits": ("native_source_ranked_payload_group_visits", "visits", "demand_reader"),
    "ingest_copied": (INGEST + "ownership_copied_buffer_bytes", "visible bytes", "ingest"),
    "ingest_copy_charge": (INGEST + "ownership_copied_allocation_bytes", "capacity bytes", "ingest"),
    "ingest_transferred": (INGEST + "ownership_transferred_buffer_bytes", "visible bytes", "ingest"),
    "ingest_transfer_charge": (INGEST + "ownership_transferred_allocation_bytes", "capacity bytes", "ingest"),
    "text_active": (INGEST + "text_active_nanos", "nanoseconds, union of selected scopes", "ingest"),
    "pool_peak": ("execution_resource_observed_native_peak_reserved_bytes", "reserved bytes, session lifetime peak", "resource"),
    "allocation_peak": ("execution_resource_observed_native_allocation_peak_reserved_bytes", "reserved bytes, current allocation peak", "resource"),
    "peak_active_lanes": ("execution_resource_observed_peak_active_lanes", "lanes", "resource"),
}


def unsigned(value):
    need(type(value) in (str, int) and str(value).isascii() and str(value).isdigit(), "malformed unsigned counter")
    return int(value)


def metric(fields, source, applicable):
    if source not in fields:
        return "omitted" if applicable else "inapplicable"
    if fields[source] in ("unavailable", "not_measured"):
        return fields[source]
    return unsigned(fields[source])


def decisions(fields):
    result = {}
    for cap in CAPABILITIES:
        prefix = DECISION + cap + "_"
        observed = {k.removeprefix(prefix): v for k, v in fields.items() if k.startswith(prefix)}
        if not observed:
            continue
        for name, value in observed.items():
            if name.endswith("_reasons"):
                observed[name] = value.split(",") if value else []
            else:
                observed[name] = unsigned(value)
        need(observed["considered"] == observed["admitted"] + observed["declined"]
             and observed["executed"] <= observed["admitted"], "inconsistent native I/O decision counts")
        result[cap] = observed
    return result


def component_evidence(rows):
    """Compare only paired parent-runtime cases; keep all other cohorts independent."""
    probe = {r["operation"]: r for r in rows if r["cohort"] == "probe-parquet"}
    prepared = {r["operation"]: r for r in rows if r["cohort"] == "probe-vortex"}
    gaps = {}
    for name, direct in probe.items():
        native = prepared[name]
        need(all(direct[key] == native[key] for key in ("runtime_sha256", "workload_sha256", "declared_limits", "result_rows")),
             "unpaired source-strategy observation")
        gaps[name] = Decimal(direct["engine_seconds"]) - Decimal(native["engine_seconds"])
    net = sum(gaps.values())
    focused = gaps["Q19"] + gaps["Q34"]
    broad_families = {}
    selected = []
    stages = {}
    workers = {}
    for row in rows:
        m, cohort, operation = row["metrics"], row["cohort"], row["operation"]
        if cohort == "fresh347":
            family = broad_families.setdefault(row["family"], {"operations": 0, "positive_copy_operations": 0,
                                                                "visible_bytes_copied": 0})
            copied = m["intake_copied"]
            need(type(copied) is int, "fresh347 copy measurement absent")
            family["operations"] += 1
            family["positive_copy_operations"] += int(copied > 0)
            family["visible_bytes_copied"] += copied
        if type(m["selected_rows"]) is int and m["selected_rows"] > 0:
            selected.append({"cohort": cohort, "operation": operation, **{k: m[k] for k in (
                "rank_read", "payload_read", "rank_rows", "payload_rows", "rank_group_visits",
                "payload_group_visits", "selected_rows")}})
        if row["ingest_stages"]:
            stages[cohort] = sorted(row["ingest_stages"], key=lambda s: -row["ingest_stages"][s]["work_nanos"])
        if cohort in ("fresh43", "probe-vortex") and row["coverage"]["aggregate_workers"]["full"]:
            fields = row["owner_fields"]
            workers.setdefault(cohort, {})[operation] = {
                k.removeprefix(WORKER): unsigned(v) for k, v in fields.items() if k.startswith(WORKER) and
                (k.endswith("_nanos") or k.endswith(("peak_active_workers", "peak_outstanding_chunks")))}
    return {
        "source_admission": {"probe_direct_minus_prepared_engine_seconds": {k: str(v) for k, v in gaps.items()},
                             "net_seven_seconds": str(net), "q19_and_q34_seconds": str(focused),
                             "q19_and_q34_share_of_net": str(focused / net)},
        "ownership": {"probe_direct_copied_visible_bytes": {k: v["metrics"]["intake_copied"] for k, v in probe.items()},
                      "fresh347_families": broad_families},
        "selected_payload": selected, "ingest_stage_order_by_overlapping_work_nanos": stages,
        "aggregate_worker_observations": workers,
    }


def project(record):
    fields, compact = record["full_fields"], record["compact_fields"]
    prep = record["operation"] == "prepare"
    # Eligibility follows the actual input route; the batch owners can also be
    # present on a relational Vortex plan. A reported field always wins.
    demand = record["cohort"] in ("probe-parquet", "fresh347")
    owners = {"resource": True, "ingest": prep, "demand_reader": demand,
              "batch_intake": demand or "native_input_batch_sources" in fields}
    metrics = {name: metric(fields, key, owners[owner]) for name, (key, _, owner) in METRICS.items()}
    coverage = {name: {"full": sum(k.startswith(prefix) for k in fields),
                       "compact": sum(k.startswith(prefix) for k in compact)}
                for name, prefix in COVERAGE.items()}
    route_fields = ("public_workflow_source_format", "public_workflow_native_vortex_plan_payload_kind",
                    "public_workflow_execution_mode", "native_source_demand_formats", "relational_provider_selection_reason",
                    "resident_provider_crate", "local_primitive_group_output_strategy", "local_primitive_scan_projection_columns",
                    "local_primitive_aggregate_materialized_accessor_columns", "local_primitive_domain_dictionary_status",
                    "local_primitive_state_pressure_class", "local_primitive_memory_pressure_before", "local_primitive_memory_pressure_after")
    stage = {}
    if prep:
        stage = {name: {suffix: unsigned(fields[INGEST + name + "_" + suffix])
                        for suffix in ("work_nanos", "calls", "rows", "input_bytes", "output_bytes")} for name in STAGES}
    # No pair in these independent processes/sessions proves an immediately
    # preceding uninterrupted same-declaration snapshot. Preserve raw lifetimes.
    lifetimes = {k: fields[k] for k in ("resident_counter_scope", "resident_source_opens",
                 "resident_completed_executions") if k in fields}
    selected = {k: v for k, v in record.items() if k not in ("full_fields", "compact_fields")}
    selected["declared_limits"] = selected.pop("grant")
    selected["admitted_limits"] = {
        "memory_bytes": metric(fields, "execution_resource_admitted_memory_bytes", True),
        "parallelism": metric(fields, "execution_resource_admitted_max_parallelism", True)}
    selected.update(metrics=metrics, coverage=coverage, field_counts={"full": len(fields), "compact": len(compact)},
        route={k: fields[k] for k in route_fields if k in fields},
        io_decisions=decisions(fields), ingest_stages=stage, lifetime_snapshot=lifetimes, lifetime_delta=None,
        owner_fields={k: v for k, v in fields.items() if k.startswith((
            "execution_resource_", REPLAY, "public_workflow_preparation_", "native_source_", "native_input_",
            "local_primitive_", "relational_", "resident_")) or k.endswith(("_scope", "_accounting"))})
    return selected


def totals(rows):
    """Summaries count typed observations. An absent measurement never contributes a zero."""
    result = {}
    for cohort, count in COHORTS.items():
        group = [row for row in rows if row["cohort"] == cohort]
        need([r["ordinal"] for r in group] == list(range(1, count + 1))
             and len({r["operation"] for r in group}) == count, "cohort order or denominator changed")
        info = dict(operations=count, rows=sum(r["result_rows"] for r in group),
                    engine_seconds=str(sum(Decimal(r["engine_seconds"]) for r in group)),
                    coverage={key: {scope: sum(r["coverage"][key][scope] > 0 for r in group)
                               for scope in ("full", "compact")} for key in COVERAGE})
        if "publication_seconds" in group[0]:
            info["publication_seconds"] = group[0]["publication_seconds"]
        info["decision_boundaries"] = {}
        for cap in CAPABILITIES:
            present = [row["io_decisions"][cap] for row in group if cap in row["io_decisions"]]
            if present:
                info["decision_boundaries"][cap] = {
                    "reporting_operations": len(present),
                    "outcomes": {key: sum(d[key] for d in present) for key in ("considered", "admitted", "declined", "executed")},
                    "observed_reason_sets": {key: sorted({s for d in present for s in d[key]})
                                             for key in ("admission_reasons", "decline_reasons")}}
        for name in ("intake_copied", "rank_rows", "payload_rows", "rank_read", "payload_read", "selected_rows"):
            observed = [r["metrics"][name] for r in group if type(r["metrics"][name]) is int]
            info[name] = dict(reported=len(observed), positive=sum(x > 0 for x in observed),
                              sum=sum(observed) if observed else None)
        result[cohort] = info
    need(sum(COHORTS.values()) == len(rows), "unexpected operations")
    return result
