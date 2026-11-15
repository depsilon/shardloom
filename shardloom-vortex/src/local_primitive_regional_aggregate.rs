//! Exact integer aggregate partials over complete, cross-column native zones.

use std::ops::Range;

use super::{
    PredicateExpr, Result, ShardLoomError, SimpleAggregateFunction, SimpleAggregateState,
    SimpleAggregateStates, StatValue, VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest,
    aggregate_lowering::AggregateLowering,
    native_capacity::{self, ReservedVec},
    vortex_error,
};
use shardloom_core::SegmentStats;
use shardloom_exec::{compute_pool::CancellationToken, live_memory::MemoryLease};
use vortex::{
    array::dtype::{DType, Nullability},
    file::VortexFile,
    io::runtime::BlockingRuntime,
    session::VortexSession,
};

#[path = "local_primitive_regional_aggregate_coverage.rs"]
mod coverage;
#[path = "local_primitive_regional_aggregate_metadata.rs"]
mod metadata;
#[cfg(all(test, feature = "vortex-write"))]
#[path = "local_primitive_regional_aggregate_native_tests.rs"]
mod native_tests;

const MAX_COLUMNS: usize = 64;
const MAX_ZONES: usize = 8192;
const MAX_TABLES: usize = 1024;
const MAX_DEPTH: usize = 32;

#[cfg(test)]
thread_local! {
    // One-call matched control for proving source reads against the ordinary
    // scan on the identical file/request. This is not a production option.
    static SCAN_ONLY_TEST: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct Column<'a> {
    name: &'a str,
    dtype: DType,
    zones: ReservedVec<Zone>,
}

#[derive(Clone)]
struct Zone {
    range: Range<u64>,
    stats: SegmentStats,
}

pub(super) struct Plan {
    pub(super) ranges: ReservedVec<Range<u64>>,
    source_rows: u64,
    regions: u64,
    metadata_regions: u64,
    excluded_regions: u64,
    residual_regions: u64,
    pub(super) metadata_rows: u64,
    excluded_rows: u64,
    residual_rows: u64,
    tables_read: u64,
    pub(super) metadata_arrays: u64,
    metadata_rows_read: u64,
    measures: usize,
    metadata_bytes: u64,
    // All containers and temporary staging values retire before this allowance.
    _metadata: MemoryLease,
}

impl Plan {
    pub(super) fn annotate(
        &self,
        summary: &mut String,
        selected: usize,
        payload_arrays: usize,
    ) -> Result<()> {
        self.verify()?;
        let delivered = u64::try_from(selected)
            .map_err(vortex_error)?
            .checked_sub(self.metadata_rows)
            .ok_or_else(|| failed("selected rows omit metadata contributions"))?;
        if delivered > self.residual_rows {
            return Err(failed(
                "provider delivered more rows than residual coverage",
            ));
        }
        let mut value: serde_json::Value = serde_json::from_str(summary).map_err(vortex_error)?;
        value["regional_aggregate"] = serde_json::json!({
            "schema_version": 1,
            "provider_version": crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
            "source": "held_vortex_zoned_aggregate_partials",
            "source_rows": self.source_rows,
            "regions": self.regions,
            "metadata_regions": self.metadata_regions,
            "excluded_regions": self.excluded_regions,
            "residual_regions": self.residual_regions,
            "metadata_rows": self.metadata_rows,
            "excluded_rows": self.excluded_rows,
            "residual_source_rows": self.residual_rows,
            "residual_scan_ranges": self.ranges.values.len(),
            "residual_provider_rows_delivered": delivered,
            "payload_arrays_delivered": payload_arrays,
            "metadata_provider_scans": self.tables_read,
            "metadata_arrays_delivered": self.metadata_arrays,
            "metadata_rows_read": self.metadata_rows_read,
            "measures_per_metadata_region": self.measures,
            "complete_disjoint_source_coverage": true,
            "whole_zones_only": true,
            "all_measures_proven_per_metadata_region": true,
            "operator_metadata_reserved_bytes": self.metadata_bytes,
            "rows_scanned_field_scope": "held_source_length_not_observed_rows",
            "read_scope": "metadata_reads_included_in_native_io;payload_scan_contracts_exclude_auxiliary_tables;requested_bytes_are_not_unique_or_device_bytes",
            "memory_scope": "bounded_query_metadata_and_zone_range_staging;native_payload_owners_separate;not_process_rss",
            "fallback_attempted": false,
            "external_engine_invoked": false,
        });
        *summary = value.to_string();
        Ok(())
    }

    fn verify(&self) -> Result<()> {
        if self
            .metadata_rows
            .checked_add(self.excluded_rows)
            .and_then(|rows| rows.checked_add(self.residual_rows))
            != Some(self.source_rows)
            || self
                .metadata_regions
                .checked_add(self.excluded_regions)
                .and_then(|regions| regions.checked_add(self.residual_regions))
                != Some(self.regions)
        {
            return Err(failed("regional contributions do not cover the source"));
        }
        let mut previous = 0;
        let mut rows = 0_u64;
        for range in &self.ranges.values {
            if range.start < previous || range.start >= range.end || range.end > self.source_rows {
                return Err(failed("residual ranges overlap or exceed source coverage"));
            }
            rows = rows
                .checked_add(range.end - range.start)
                .ok_or_else(|| failed("residual row count overflow"))?;
            previous = range.end;
        }
        if rows != self.residual_rows {
            return Err(failed(
                "residual range lengths differ from assigned coverage",
            ));
        }
        Ok(())
    }
}

fn admitted(request: &VortexQueryPrimitiveRequest, lowering: &AggregateLowering) -> bool {
    let aggregate = &lowering.rewrite.aggregate;
    request.kind == VortexQueryPrimitiveKind::SimpleAggregate
        && lowering.rewrite.rewritten_columns.is_empty()
        && aggregate.group_by.is_empty()
        && aggregate.group_expressions.is_empty()
        && aggregate.spill.is_none()
        && !aggregate.measures.is_empty()
        && aggregate.measures.len() <= MAX_COLUMNS
        && aggregate.measures.iter().all(|measure| {
            measure.secondary_column.is_none()
                && measure.filter_column.is_none()
                && measure.quantile_level.is_none()
                && measure.decimal_output.is_none()
                && measure.order_by.is_empty()
                && measure.separator.is_none()
                && measure.argument_offset.is_none()
                && measure.value_transform.is_none()
                && matches!(
                    SimpleAggregateFunction::parse(&measure.function),
                    Ok(SimpleAggregateFunction::Count
                        | SimpleAggregateFunction::Min
                        | SimpleAggregateFunction::Max)
                )
        })
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) fn admit(
    file: &VortexFile,
    request: &VortexQueryPrimitiveRequest,
    lowering: &AggregateLowering,
    states: &mut SimpleAggregateStates,
    session: &VortexSession,
    runtime: &impl BlockingRuntime,
    cancellation: Option<&CancellationToken>,
) -> Result<Option<Plan>> {
    #[cfg(test)]
    if SCAN_ONLY_TEST.with(std::cell::Cell::take) {
        return Ok(None);
    }
    if !admitted(request, lowering) || file.row_count() == 0 {
        return Ok(None);
    }
    let DType::Struct(fields, Nullability::NonNullable) = file.dtype() else {
        return Ok(None);
    };
    let aggregate = &lowering.rewrite.aggregate;
    let mut names = [None; MAX_COLUMNS];
    for measure in &aggregate.measures {
        if let Some(column) = &measure.column
            && !add_name(&mut names, column.as_str())
        {
            return Ok(None);
        }
    }
    if let Some(predicate) = &lowering.rewrite.predicate
        && !predicate_names(predicate, &mut names, &mut 0, 0)
    {
        return Ok(None);
    }
    let width = names.iter().flatten().count();
    if width == 0 {
        return Ok(None);
    }
    for name in names.iter().flatten() {
        if !matches!(fields.try_field(name).map_err(vortex_error)?, Some(DType::Primitive(ptype, _)) if ptype.is_int())
        {
            return Ok(None);
        }
    }
    let Some(memory) = crate::native_provider_memory::shared_pool(session) else {
        return Ok(None);
    };
    // Two fresh state clones, fixed field/coverage staging, bounded expression
    // temporaries and aggregate names. Numeric facts contain no variable payload.
    let aliases = states.states.iter().try_fold(0_u64, |bytes, state| {
        bytes
            .checked_add(state.alias.len() as u64)
            .ok_or_else(|| failed("alias size overflow"))
    })?;
    let metadata_bytes = native_capacity::bytes::<SimpleAggregateState>(states.states.len())?
        .checked_add(aliases)
        .and_then(|bytes| bytes.checked_mul(2))
        .and_then(|bytes| bytes.checked_add(65_536))
        .ok_or_else(|| failed("metadata capacity overflow"))?;
    let Some(credit) = memory.try_reserve(metadata_bytes)? else {
        return Ok(None);
    };
    let mut counts = [0_usize; MAX_COLUMNS];
    let mut total_zones = 0_usize;
    let mut total_tables = 0_usize;
    for (index, name) in names.iter().flatten().enumerate() {
        check(cancellation)?;
        let Some(count) = metadata::count(file, name, &mut total_zones, &mut total_tables)? else {
            return Ok(None);
        };
        counts[index] = count;
    }
    let mut columns: [Option<Column<'_>>; MAX_COLUMNS] = std::array::from_fn(|_| None);
    for (index, name) in names.iter().flatten().enumerate() {
        let Some(zones) = ReservedVec::try_with_capacity(&memory, counts[index])? else {
            return Ok(None);
        };
        columns[index] = Some(Column {
            name,
            dtype: fields
                .try_field(name)
                .map_err(vortex_error)?
                .ok_or_else(|| failed("bound field disappeared"))?,
            zones,
        });
    }
    let Some(ranges) = ReservedVec::try_with_capacity(&memory, counts[0])? else {
        return Ok(None);
    };
    let mut plan = Plan {
        ranges,
        source_rows: file.row_count(),
        regions: 0,
        metadata_regions: 0,
        excluded_regions: 0,
        residual_regions: 0,
        metadata_rows: 0,
        excluded_rows: 0,
        residual_rows: 0,
        tables_read: 0,
        metadata_arrays: 0,
        metadata_rows_read: 0,
        measures: aggregate.measures.len(),
        metadata_bytes: metadata_bytes
            .checked_add(native_capacity::bytes::<Zone>(total_zones)?)
            .and_then(|bytes| {
                bytes.checked_add(native_capacity::bytes::<Range<u64>>(counts[0]).ok()?)
            })
            .ok_or_else(|| failed("container capacity overflow"))?,
        _metadata: credit,
    };
    for column in columns.iter_mut().flatten() {
        metadata::read(file, column, session, runtime, cancellation, &mut plan)?;
        coverage::validate_zones(&column.zones.values, file.row_count())?;
    }
    // This path certifies actual zoned aggregate metadata consumption. A layout
    // containing no usable partial table keeps the existing native execution.
    if plan.tables_read == 0 {
        return Ok(None);
    }
    coverage::complete(&columns, lowering, states, cancellation, &mut plan)?;
    // A table read remains observable even when all measure partials are
    // incomplete. Those regions retain their scan; exact exclusions can still
    // avoid source payload work without claiming a completed measure.
    Ok(Some(plan))
}

fn add_name<'a>(names: &mut [Option<&'a str>; MAX_COLUMNS], name: &'a str) -> bool {
    if names.contains(&Some(name)) {
        return true;
    }
    let Some(slot) = names.iter_mut().find(|slot| slot.is_none()) else {
        return false;
    };
    *slot = Some(name);
    true
}

fn predicate_names<'a>(
    predicate: &'a PredicateExpr,
    names: &mut [Option<&'a str>; MAX_COLUMNS],
    nodes: &mut usize,
    depth: usize,
) -> bool {
    *nodes += 1;
    if depth > MAX_DEPTH || *nodes > MAX_COLUMNS {
        return false;
    }
    match predicate {
        PredicateExpr::AlwaysTrue | PredicateExpr::AlwaysFalse => true,
        PredicateExpr::IsNull { column } | PredicateExpr::IsNotNull { column } => {
            add_name(names, column.as_str())
        }
        PredicateExpr::Compare {
            column,
            value: StatValue::Int64(_) | StatValue::UInt64(_),
            ..
        } => add_name(names, column.as_str()),
        PredicateExpr::And(terms) => terms
            .iter()
            .all(|term| predicate_names(term, names, nodes, depth + 1)),
        _ => false,
    }
}

fn check(cancellation: Option<&CancellationToken>) -> Result<()> {
    cancellation.map_or(Ok(()), CancellationToken::check)
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native regional aggregate: {message}; no fallback execution was attempted"
    ))
}
