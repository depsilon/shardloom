//! Family-specific policy and conservation checks for native weighted COUNT.

use super::{
    VortexLocalPrimitiveExecutionReport, VortexQueryPrimitiveRequest,
    weighted_count_spill_admission,
};
use crate::{VortexAggregateSpillPolicy, VortexQueryPrimitiveKind, VortexWeightedCountSpillReport};

pub(super) fn request_matches(
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
    policy: &VortexAggregateSpillPolicy,
    evidence: &VortexWeightedCountSpillReport,
) -> bool {
    let Some(aggregate) = &request.simple_aggregate else {
        return false;
    };
    let Some(limit) = request.source_order_limit else {
        return false;
    };
    let key_order_matches = match aggregate.group_by.len() {
        1 => evidence.key_order == "utf8",
        2 => matches!(evidence.key_order.as_str(), "integer_utf8" | "utf8_integer"),
        _ => false,
    };
    weighted_count_spill_admission::request_admitted(request)
        && key_order_matches
        && policy.workspace == evidence.workspace
        && policy.quota_bytes == evidence.quota_bytes
        && policy.memory_bytes == evidence.memory_bytes
        && report.source_order_limit_requested == u64::try_from(limit).ok()
        && report.rows_projected
            == Some(
                evidence
                    .groups
                    .saturating_sub(aggregate.offset as u64)
                    .min(limit as u64),
            )
        && verified(report)
}

pub(super) fn verified(report: &VortexLocalPrimitiveExecutionReport) -> bool {
    let Some(spill) = &report.state_budget.native_weighted_count_spill else {
        return false;
    };
    let wrote = spill.runs_written != 0;
    report.primitive_kind == VortexQueryPrimitiveKind::SimpleAggregate
        && report.state_budget.native_sort_spill.is_none()
        && report.state_budget.native_aggregate_spill.is_none()
        && report.state_budget.spill_supported
        && report.state_budget.state_budget_required
        && spill.family == "weighted_complete_utf8_grouped_count"
        && spill.workspace.is_absolute()
        && spill.memory_bytes >= 4 * 1024 * 1024
        && spill.quota_bytes >= 32 * 1024
        && spill.peak_reserved_bytes <= spill.memory_bytes
        && spill.peak_disk_bytes <= spill.quota_bytes
        && spill.runs_written == spill.runs_validated
        && spill.max_admitted_key_bytes == 64 * 1024
        && spill.merge_fan_in == 4
        && spill.buffer_capacity_records > 0
        && spill.buffer_capacity_records <= 65_536
        && spill.buffer_capacity_text_bytes as u64 == spill.memory_bytes / 8
        && spill.source_records <= spill.source_rows
        && spill.initial_run_records <= spill.source_records
        && spill.groups <= spill.source_records
        && (spill.source_records > 0) == (spill.source_rows > 0)
        && report.rows_selected == Some(spill.source_rows)
        && report.write_io == wrote
        && report.spill_io_performed == wrote
        && report.state_budget.spill_io_performed == wrote
        && report.state_budget.spill_required == wrote
        && spill.owned_cleanup_completed
        && geometry_verified(spill, wrote)
}

fn geometry_verified(spill: &VortexWeightedCountSpillReport, wrote: bool) -> bool {
    if !wrote {
        return spill.peak_disk_bytes == 0
            && spill.merge_passes == 0
            && spill.initial_run_records == 0
            && spill.native_records_written == 0
            && spill.native_bytes_written == 0
            && spill.min_run_block_rows == 0
            && spill.max_run_block_rows == 0
            && spill.max_run_key_bytes == 0;
    }
    spill.initial_run_records > 0
        && spill.native_records_written >= spill.initial_run_records
        && spill.native_bytes_written > 0
        && spill.peak_disk_bytes > 0
        && spill.max_run_key_bytes <= spill.max_admitted_key_bytes
        && spill.min_run_block_rows == ((64 * 1024) / (spill.max_run_key_bytes + 32)).clamp(1, 1024)
        && spill.max_run_block_rows >= spill.min_run_block_rows
        && spill.max_run_block_rows <= 1024
}
