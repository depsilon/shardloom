//! Whole-zone alignment and exact, null-aware contribution staging.

use super::{
    AggregateLowering, Column, MAX_COLUMNS, Plan, PredicateExpr, Result, SegmentStats,
    SimpleAggregateFunction, SimpleAggregateStates, Zone, check, failed,
};
use crate::local_primitives::{simple_aggregate_max_value, simple_aggregate_min_value};
use shardloom_core::{
    ColumnRef, ComparisonOp, PredicateProof, StatisticsExactness, prove_predicate_from_stats,
};
use shardloom_exec::compute_pool::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Qualification {
    All,
    None,
    Unknown,
}

pub(super) fn validate_zones(zones: &[Zone], rows: u64) -> Result<()> {
    let mut next = 0;
    for zone in zones {
        if zone.range.start != next
            || zone.range.end <= zone.range.start
            || zone.range.end > rows
            || zone.stats.row_count != Some(zone.range.end - zone.range.start)
        {
            return Err(failed(
                "column zones contain a gap, overlap or inconsistent length",
            ));
        }
        next = zone.range.end;
    }
    if next != rows {
        return Err(failed("column zones do not cover the complete source"));
    }
    Ok(())
}

fn combine(zones: &[Zone], start: u64, end: u64) -> Result<SegmentStats> {
    if zones.first().map(|zone| zone.range.start) != Some(start)
        || zones.last().map(|zone| zone.range.end) != Some(end)
    {
        return Err(failed(
            "regional partial would cover an arbitrary subset of a zone",
        ));
    }
    let mut stats = SegmentStats::with_row_count(end - start);
    stats.exactness = StatisticsExactness::Exact;
    stats.null_count = Some(0);
    let mut min_complete = true;
    let mut max_complete = true;
    let mut next = start;
    for zone in zones {
        if zone.range.start != next || zone.range.end <= next {
            return Err(failed("regional zone coverage overlaps or has a gap"));
        }
        next = zone.range.end;
        stats.null_count = match (stats.null_count, zone.stats.null_count) {
            (Some(total), Some(nulls)) => Some(
                total
                    .checked_add(nulls)
                    .ok_or_else(|| failed("regional null count overflow"))?,
            ),
            _ => None,
        };
        let all_null = zone.stats.null_count == zone.stats.row_count;
        if let Some(value) = &zone.stats.min_value {
            stats.min_value = Some(simple_aggregate_min_value(stats.min_value, value)?);
        } else if !all_null {
            min_complete = false;
        }
        if let Some(value) = &zone.stats.max_value {
            stats.max_value = Some(simple_aggregate_max_value(stats.max_value, value)?);
        } else if !all_null {
            max_complete = false;
        }
    }
    if !min_complete {
        stats.min_value = None;
    }
    if !max_complete {
        stats.max_value = None;
    }
    Ok(stats)
}

fn qualify(
    predicate: &PredicateExpr,
    columns: &[Option<Column<'_>>; MAX_COLUMNS],
    facts: &[Option<SegmentStats>; MAX_COLUMNS],
) -> Result<Qualification> {
    match predicate {
        PredicateExpr::AlwaysTrue => return Ok(Qualification::All),
        PredicateExpr::AlwaysFalse => return Ok(Qualification::None),
        PredicateExpr::And(terms) => {
            let mut result = Qualification::All;
            for term in terms {
                match qualify(term, columns, facts)? {
                    Qualification::None => return Ok(Qualification::None),
                    Qualification::Unknown => result = Qualification::Unknown,
                    Qualification::All => {}
                }
            }
            return Ok(result);
        }
        _ => {}
    }
    let (PredicateExpr::Compare { column, .. }
    | PredicateExpr::IsNull { column }
    | PredicateExpr::IsNotNull { column }) = predicate
    else {
        return Ok(Qualification::Unknown);
    };
    let index = columns
        .iter()
        .position(|entry| {
            entry
                .as_ref()
                .is_some_and(|entry| entry.name == column.as_str())
        })
        .ok_or_else(|| failed("predicate has no bound regional column"))?;
    let stats = facts[index]
        .as_ref()
        .ok_or_else(|| failed("regional column has no coverage facts"))?;
    if matches!(predicate, PredicateExpr::Compare { .. }) && stats.is_all_null() == Some(true) {
        return Ok(Qualification::None);
    }
    match prove_predicate_from_stats(predicate, stats) {
        PredicateProof::AlwaysFalse { .. } => return Ok(Qualification::None),
        PredicateProof::AlwaysTrue { .. } => return Ok(Qualification::All),
        _ => {}
    }
    if stats.null_count == Some(0)
        && let PredicateExpr::Compare { op, value, .. } = predicate
    {
        // In a non-null integer domain, falsifying the complementary comparison
        // proves every row qualifies. Reuse the core's exact signed/unsigned
        // comparison semantics; do not widen integers to F64.
        let op = match op {
            ComparisonOp::Eq => ComparisonOp::NotEq,
            ComparisonOp::NotEq => ComparisonOp::Eq,
            ComparisonOp::Lt => ComparisonOp::GtEq,
            ComparisonOp::LtEq => ComparisonOp::Gt,
            ComparisonOp::Gt => ComparisonOp::LtEq,
            ComparisonOp::GtEq => ComparisonOp::Lt,
        };
        let complement = PredicateExpr::Compare {
            column: ColumnRef::new("regional_bound")?,
            op,
            value: value.clone(),
        };
        if matches!(
            prove_predicate_from_stats(&complement, stats),
            PredicateProof::AlwaysFalse { .. }
        ) {
            return Ok(Qualification::All);
        }
    }
    Ok(Qualification::Unknown)
}

fn stage(
    lowering: &AggregateLowering,
    columns: &[Option<Column<'_>>; MAX_COLUMNS],
    facts: &[Option<SegmentStats>; MAX_COLUMNS],
    rows: u64,
    partial: &mut SimpleAggregateStates,
) -> Result<bool> {
    for (measure, state) in lowering
        .rewrite
        .aggregate
        .measures
        .iter()
        .zip(&mut partial.states)
    {
        state.count = 0;
        state.min = None;
        state.max = None;
        let Some(column) = &measure.column else {
            if state.function != SimpleAggregateFunction::Count {
                return Err(failed("non-count measure omitted its input"));
            }
            state.count = rows;
            continue;
        };
        let index = columns
            .iter()
            .position(|entry| {
                entry
                    .as_ref()
                    .is_some_and(|entry| entry.name == column.as_str())
            })
            .ok_or_else(|| failed("measure has no bound regional column"))?;
        let stats = facts[index]
            .as_ref()
            .ok_or_else(|| failed("measure has no regional facts"))?;
        let Some(nulls) = stats.null_count else {
            return Ok(false);
        };
        state.count = rows
            .checked_sub(nulls)
            .ok_or_else(|| failed("measure null count exceeds regional rows"))?;
        match state.function {
            SimpleAggregateFunction::Count => {}
            SimpleAggregateFunction::Min => {
                if state.count != 0 && stats.min_value.is_none() {
                    return Ok(false);
                }
                state.min.clone_from(&stats.min_value);
            }
            SimpleAggregateFunction::Max => {
                if state.count != 0 && stats.max_value.is_none() {
                    return Ok(false);
                }
                state.max.clone_from(&stats.max_value);
            }
            _ => {
                return Err(failed(
                    "regional state is outside admitted aggregate semantics",
                ));
            }
        }
    }
    Ok(true)
}

#[allow(clippy::too_many_lines)]
pub(super) fn complete(
    columns: &[Option<Column<'_>>; MAX_COLUMNS],
    lowering: &AggregateLowering,
    states: &mut SimpleAggregateStates,
    cancellation: Option<&CancellationToken>,
    plan: &mut Plan,
) -> Result<()> {
    if states.states.len() != lowering.rewrite.aggregate.measures.len()
        || states.states.iter().any(|state| {
            state.count != 0
                || state.min.is_some()
                || state.max.is_some()
                || !state.distinct_values.is_empty()
        })
    {
        return Err(failed(
            "regional admission requires fresh matching scalar states",
        ));
    }
    // Both copies are covered by the pre-allocation query metadata allowance.
    // The target states remain unchanged until coverage and every staged result
    // have been checked. A late read, allocation or proof error exposes no answer.
    let mut staged = states.clone();
    let mut partial = states.clone();
    let first = columns[0]
        .as_ref()
        .ok_or_else(|| failed("no regional columns"))?;
    let mut positions = [0_usize; MAX_COLUMNS];
    let mut start = 0;
    for boundary in &first.zones.values {
        check(cancellation)?;
        let end = boundary.range.end;
        if columns.iter().flatten().any(|column| {
            column
                .zones
                .values
                .binary_search_by_key(&end, |zone| zone.range.end)
                .is_err()
        }) {
            continue;
        }
        let mut facts: [Option<SegmentStats>; MAX_COLUMNS] = std::array::from_fn(|_| None);
        for (index, column) in columns.iter().flatten().enumerate() {
            let next = column
                .zones
                .values
                .partition_point(|zone| zone.range.end <= end);
            facts[index] = Some(combine(
                &column.zones.values[positions[index]..next],
                start,
                end,
            )?);
            positions[index] = next;
        }
        let rows = end - start;
        let qualification = lowering
            .rewrite
            .predicate
            .as_ref()
            .map_or(Ok(Qualification::All), |predicate| {
                qualify(predicate, columns, &facts)
            })?;
        plan.regions += 1;
        if qualification == Qualification::None {
            plan.excluded_rows += rows;
            plan.excluded_regions += 1;
        } else if qualification == Qualification::All
            && stage(lowering, columns, &facts, rows, &mut partial)?
        {
            staged.merge_preaggregated_from(&partial)?;
            plan.metadata_rows += rows;
            plan.metadata_regions += 1;
        } else {
            plan.residual_rows += rows;
            plan.residual_regions += 1;
            if let Some(previous) = plan.ranges.values.last_mut()
                && previous.end == start
            {
                previous.end = end;
            } else {
                if plan.ranges.values.len() == plan.ranges.values.capacity() {
                    return Err(failed("residual range count exceeds admitted capacity"));
                }
                plan.ranges.values.push(start..end);
            }
        }
        start = end;
    }
    if start != plan.source_rows {
        return Err(failed("shared regional boundaries omit source rows"));
    }
    plan.verify()?;
    check(cancellation)?;
    if plan.metadata_rows != 0 {
        states.merge_preaggregated_from(&staged)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "local_primitive_regional_aggregate_coverage_tests.rs"]
mod tests;
