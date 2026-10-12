# SPDX-License-Identifier: Apache-2.0
"""Shared native admission evidence for public collect and direct writers."""


def has_diagnostic_detail(envelope, detail):
    """Both standard diagnostic constructors retain their specific explanation."""
    return any(detail in (item.get(key) or "")
               for item in envelope.raw.get("diagnostics", [])
               for key in ("message", "reason"))


def require_relational_source_evidence(name, envelope, file_sources, *, previous=None):
    """Check this plan's file sources and, when repeated, exact lifetime deltas.

    The worker retains one resource owner across different plans and writers.
    Its cumulative counters therefore cannot be compared with this plan's source
    count or repetition number. A previous snapshot must be from the immediately
    preceding execution of the same declaration, with no intervening commands.
    """
    for key, expected in {
        "relational_prepared_file_source_count": str(file_sources),
        "resident_counter_scope": "shared_session_lifetime",
        "source_io_performed": str(file_sources > 0).lower(),
    }.items():
        if envelope.field(key) != expected:
            raise ValueError(f"{name}: {key} differs from the declared source scope")
    counters = []
    for key in ("resident_source_opens", "resident_completed_executions"):
        value = envelope.field(key)
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError(f"{name}: missing or malformed lifetime counter {key}")
        counters.append(int(value))
    opens, executions = counters
    if opens < file_sources or executions == 0:
        raise ValueError(f"{name}: lifetime counters omit this completed plan")
    if previous is not None and (opens != previous[0] or executions != previous[1] + 1):
        raise ValueError(f"{name}: repeated declaration reopened sources or changed completion count")
    return opens, executions


def require_native_allocation_peak(name, envelope, budget):
    """Verify this allocation's measured peak without discarding session history."""
    required = {
        "execution_resource_admission_status": "admitted",
        "execution_resource_declared_memory_bytes": str(budget),
        "execution_resource_admitted_memory_bytes": str(budget),
        "execution_resource_allocation_memory_observation_scope":
            "current_allocation_including_preparation_and_retained_owners;"
            "excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss",
    }
    for key, expected in required.items():
        if envelope.field(key) != expected:
            raise ValueError(f"{name}: {key} differs from the declared allocation scope")
    # Native array sinks publish the same shared pool's lifetime peak using
    # their existing sink scope. Command admission supplies the allocation
    # observation after the writer completes; neither scope resets history.
    lifetime_scopes = (
        "session_pool_lifetime_including_preparation_and_retained_owners;"
        "excludes_cli_envelope_formatting_provider_bypass_allocations_and_process_rss",
        "session_pool_lifetime_including_retained_owners;"
        "excludes_provider_bypass_allocations_and_process_rss",
    )
    if envelope.field("execution_resource_memory_observation_scope") not in lifetime_scopes:
        raise ValueError(f"{name}: memory observation is not the shared native pool lifetime")
    values = []
    for suffix in ("reserved_bytes", "allocation_peak_reserved_bytes", "peak_reserved_bytes"):
        key = f"execution_resource_observed_native_{suffix}"
        value = envelope.field(key)
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError(f"{name}: missing or malformed reservation observation {key}")
        number = int(value)
        if number > (1 << 64) - 1:
            raise ValueError(f"{name}: reservation observation exceeds the native integer domain {key}")
        values.append(number)
    live, allocation_peak, lifetime_peak = values
    if not live <= allocation_peak <= lifetime_peak or allocation_peak > budget:
        raise ValueError(f"{name}: shared native allocation admission differs")
    return allocation_peak


def require_native_resource_admission(name, envelope, *, memory_gb=1):
    """Check the caller's declared grant, never a budget inferred from the report."""
    budget = memory_gb << 30
    required = {
        "public_workflow_memory_gb": str(memory_gb),
        "public_workflow_native_vortex_plan_route_family": "native_vortex_unified_plan",
    }
    peak_key = "resident_peak_reserved_buffer_bytes"
    if envelope.field("public_workflow_route_id") == "native_vortex_primitive_row_export":
        required.update({
            "native_vortex_result_export_kind": "owned_native_array_stream",
            "native_vortex_array_sink_source_generation_validated": "true",
            "local_primitive_resource_memory_budget_bytes": str(budget),
        })
        peak_key = "native_vortex_array_sink_peak_reserved_bytes"
    else:
        required["resident_provider_crate"] = "vortex"
    for key, expected in required.items():
        if envelope.field(key) != expected:
            raise ValueError(f"{name}: {key} differs from shared native admission")
    # The shared owner may retain a larger lifetime peak from a prior grant.
    # Both collect and array writers expose the current allocation separately.
    require_native_allocation_peak(name, envelope, budget)
    peak = envelope.field(peak_key)
    lifetime = int(envelope.field("execution_resource_observed_native_peak_reserved_bytes"))
    if (not isinstance(peak, str) or not peak.isascii() or not peak.isdecimal()
            or int(peak) > lifetime):
        raise ValueError(f"{name}: native report peak differs from its shared session history")


def require_native_pivot_spill(name, envelope, spill, workspace, *, stages=None):
    """Require the requested pivot strategy, measured state and owned cleanup."""
    counters = {}
    for suffix in ("stages", "input_rows", "index_rows", "domains", "cells",
                   "lookup_blocks", "reader_opens"):
        key = f"relational_spilled_pivot_{suffix}"
        value = envelope.field(key)
        if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
            raise ValueError(f"{name}: missing or malformed pivot spill counter {key}")
        counters[suffix] = int(value)
    declared = envelope.field("relational_dynamic_schema_stages") if stages is None else str(stages)
    required = {
        "relational_spill_requested": "true",
        "relational_spill_strategy": "native_latest_pivot_state_and_stable_full_row_runs",
        "relational_spill_workspace": str(workspace),
        "relational_spill_quota_bytes": str(spill["quota_bytes"]),
        "relational_spill_buffer_bytes": str(spill["buffer_bytes"]),
        "relational_spill_owned_cleanup_completed": "true",
        "relational_spilled_pivot_stages": declared,
    }
    if counters["stages"] == 0 or any(envelope.field(key) != value for key, value in required.items()):
        raise ValueError(f"{name}: pivot did not use the requested native spill policy")
    peak = envelope.field("relational_spill_peak_disk_bytes")
    if (not isinstance(peak, str) or not peak.isascii() or not peak.isdecimal()
            or int(peak) > spill["quota_bytes"]):
        raise ValueError(f"{name}: pivot spill exceeded or omitted its disk accounting")
    if counters["cells"] and not counters["reader_opens"]:
        raise ValueError(f"{name}: stored pivot cells have no native reader evidence")
    if list(workspace.iterdir()):
        raise ValueError(f"{name}: pivot retained owned spill state after completion")
