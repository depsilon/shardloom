//! Exact scalar completion from the already admitted native file footer.
//! No metadata answer is installed until every measure has been proven.

use super::{
    NumericAggregateSum, Result, ShardLoomError, SimpleAggregateFunction, SimpleAggregateStates,
    StatValue, VortexLocalPrimitiveExecutionMode, VortexLocalPrimitiveExecutionReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, required_simple_aggregate,
    stat_value_cmp, vortex_error, vortex_scalar_to_stat_value,
};
use vortex::array::{
    dtype::{DType, Nullability, PType},
    expr::stats::{Precision, Stat},
    scalar::Scalar,
    stats::StatsSet,
};
use vortex::file::{FileStatistics, VortexFile};

const MAX_MEASURES: usize = 64;
const MAX_EXACT_F64_INT: u64 = 1_u64 << 53;

enum Value {
    Count(u64),
    Min(Option<StatValue>),
    Max(Option<StatValue>),
    SumOrAvg {
        count: u64,
        sum: NumericAggregateSum,
    },
}

pub(super) struct Completion {
    source_rows: u64,
    exact_statistics: usize,
    measures: usize,
    partitions_covered: Option<usize>,
}

impl Completion {
    pub(super) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value = serde_json::from_str(summary).map_err(vortex_error)?;
        payload["aggregate_update_strategy"] = "exact_file_footer_scalar_completion".into();
        let mut proof = serde_json::json!({
            "schema_version": 1,
            "provider_version": crate::UPSTREAM_VORTEX_PROVIDER_VERSION,
            "source": "held_vortex_file_footer",
            "source_rows_covered": self.source_rows,
            "source_rows_visited": 0,
            "rows_scanned_field_scope": "source_row_count_metadata",
            "exact_statistics_consumed": self.exact_statistics,
            "measures_proven": self.measures,
            "nonnullable_struct_root": true,
            "all_measures_completed": true,
            "source_payload_arrays_read": 0,
            "file_preparation_reads_excluded": true,
        });
        if let Some(partitions_covered) = self.partitions_covered {
            proof["partitions_covered"] = partitions_covered.into();
        }
        payload["metadata_aggregate"] = proof;
        *summary = payload.to_string();
        Ok(())
    }
}

pub(super) fn admitted_shape(request: &VortexQueryPrimitiveRequest) -> bool {
    request.kind == VortexQueryPrimitiveKind::SimpleAggregate
        && request.predicate.is_none()
        && request.simple_aggregate.as_ref().is_some_and(|aggregate| {
            !aggregate.measures.is_empty()
                && aggregate.measures.len() <= MAX_MEASURES
                && aggregate.group_by.is_empty()
                && aggregate.group_expressions.is_empty()
                && aggregate.spill.is_none()
                // Aliases and multiple COUNT(*) measures still depend only on
                // this held generation's exact row count. The front door may
                // express them as an aggregate rather than CountAll.
                && aggregate.measures.iter().all(|measure| {
                    measure.argument_offset.is_none()
                        && measure.value_transform.is_none()
                        && matches!(
                            SimpleAggregateFunction::parse(&measure.function),
                            Ok(SimpleAggregateFunction::Count
                                | SimpleAggregateFunction::Min
                                | SimpleAggregateFunction::Max
                                | SimpleAggregateFunction::Sum
                                | SimpleAggregateFunction::Avg)
                        )
                })
        })
}

pub(super) fn complete(
    file: &VortexFile,
    request: &VortexQueryPrimitiveRequest,
    states: &mut SimpleAggregateStates,
) -> Result<Option<Completion>> {
    complete_from_stats(
        file.dtype(),
        file.row_count(),
        file.file_stats(),
        request,
        states,
    )
}

#[cfg_attr(not(unix), allow(dead_code))]
pub(super) fn complete_partitioned(
    files: &[VortexFile],
    request: &VortexQueryPrimitiveRequest,
    states: &mut SimpleAggregateStates,
) -> Result<Option<Completion>> {
    if files.is_empty() || !admitted_shape(request) {
        return Ok(None);
    }
    if files.len() == 1 {
        return complete(&files[0], request, states);
    }
    let aggregate = required_simple_aggregate(request)?;
    if states.states.len() != aggregate.measures.len() {
        return Err(failed("measure state cardinality differs"));
    }
    validate_fresh_scalar_states(aggregate, states)?;

    let mut combined: [Option<Value>; MAX_MEASURES] = std::array::from_fn(|_| None);
    let mut exact_statistics = 0_usize;
    let mut source_rows = 0_u64;
    for file in files {
        let Some((partition_values, partition_stats)) = stage_values_from_stats(
            file.dtype(),
            file.row_count(),
            file.file_stats(),
            request,
            &mut exact_statistics,
        )?
        else {
            return Ok(None);
        };
        let _ = partition_stats;
        source_rows = source_rows
            .checked_add(file.row_count())
            .ok_or_else(|| failed("partitioned source row count overflowed u64"))?;
        for (slot, next) in combined
            .iter_mut()
            .zip(partition_values)
            .take(aggregate.measures.len())
        {
            let next = next.ok_or_else(|| failed("incomplete staged partition measure"))?;
            let Some(merged) = merge_staged_value(slot.take(), next)? else {
                return Ok(None);
            };
            *slot = Some(merged);
        }
    }
    install_staged_values(combined, aggregate, states)?;
    Ok(Some(Completion {
        source_rows,
        exact_statistics,
        measures: aggregate.measures.len(),
        partitions_covered: Some(files.len()),
    }))
}

fn validate_fresh_scalar_states(
    aggregate: &crate::VortexSimpleAggregateRequest,
    states: &SimpleAggregateStates,
) -> Result<()> {
    for (measure, state) in aggregate.measures.iter().zip(&states.states) {
        let function = SimpleAggregateFunction::parse(&measure.function)?;
        if state.function != function
            || state.alias != measure.alias
            || state.count != 0
            || state.sum.total().to_bits() != 0.0_f64.to_bits()
            || state.min.is_some()
            || state.max.is_some()
            || !state.distinct_values.is_empty()
        {
            return Err(failed("requires fresh matching scalar state"));
        }
    }
    Ok(())
}

fn merge_staged_value(current: Option<Value>, next: Value) -> Result<Option<Value>> {
    match (current, next) {
        (None, value) => Ok(Some(value)),
        (Some(Value::Count(a)), Value::Count(b)) => {
            Ok(Some(Value::Count(a.checked_add(b).ok_or_else(|| {
                failed("partitioned count overflowed u64")
            })?)))
        }
        (Some(Value::Min(a)), Value::Min(b)) => {
            let merged = match (a, b) {
                (None, other) | (other, None) => other,
                (Some(left), Some(right)) => {
                    let Some(ord) = stat_value_cmp(&left, &right) else {
                        return Ok(None);
                    };
                    Some(if ord == std::cmp::Ordering::Greater {
                        right
                    } else {
                        left
                    })
                }
            };
            Ok(Some(Value::Min(merged)))
        }
        (Some(Value::Max(a)), Value::Max(b)) => {
            let merged = match (a, b) {
                (None, other) | (other, None) => other,
                (Some(left), Some(right)) => {
                    let Some(ord) = stat_value_cmp(&left, &right) else {
                        return Ok(None);
                    };
                    Some(if ord == std::cmp::Ordering::Less {
                        right
                    } else {
                        left
                    })
                }
            };
            Ok(Some(Value::Max(merged)))
        }
        (
            Some(Value::SumOrAvg {
                count: a_count,
                sum: mut a_sum,
            }),
            Value::SumOrAvg {
                count: b_count,
                sum: b_sum,
            },
        ) => {
            let count = a_count
                .checked_add(b_count)
                .ok_or_else(|| failed("partitioned sum count overflowed u64"))?;
            a_sum.merge(b_sum);
            if !a_sum.is_finite() {
                return Err(failed("partitioned exact sum statistic is non-finite"));
            }
            Ok(Some(Value::SumOrAvg { count, sum: a_sum }))
        }
        _ => Err(failed("mismatched staged measure kinds across partitions")),
    }
}

fn install_staged_values(
    values: [Option<Value>; MAX_MEASURES],
    aggregate: &crate::VortexSimpleAggregateRequest,
    states: &mut SimpleAggregateStates,
) -> Result<()> {
    for (state, value) in states
        .states
        .iter_mut()
        .zip(values)
        .take(aggregate.measures.len())
    {
        match value.ok_or_else(|| failed("incomplete staged measure"))? {
            Value::Count(value) => state.count = value,
            Value::Min(value) => state.min = value,
            Value::Max(value) => state.max = value,
            Value::SumOrAvg { count, sum } => {
                state.count = count;
                state.sum = sum;
            }
        }
    }
    Ok(())
}

fn stage_values_from_stats(
    dtype: &DType,
    rows: u64,
    statistics: Option<&FileStatistics>,
    request: &VortexQueryPrimitiveRequest,
    exact_statistics: &mut usize,
) -> Result<Option<([Option<Value>; MAX_MEASURES], usize)>> {
    if !admitted_shape(request) {
        return Ok(None);
    }
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return Ok(None);
    };
    let aggregate = required_simple_aggregate(request)?;
    // Fixed-size query-local staging avoids an unreserved input-sized allocation.
    let mut values: [Option<Value>; MAX_MEASURES] = std::array::from_fn(|_| None);
    let initial_exact = *exact_statistics;
    for (index, measure) in aggregate.measures.iter().enumerate() {
        let function = SimpleAggregateFunction::parse(&measure.function)?;
        let Some(column) = &measure.column else {
            if function != SimpleAggregateFunction::Count {
                return Ok(None);
            }
            values[index] = Some(Value::Count(rows));
            continue;
        };
        let Some(field_index) = fields.find(column.as_str()) else {
            return Ok(None);
        };
        let Some(field_dtype) = fields.field_by_index(field_index) else {
            return Ok(None);
        };
        if statistics
            .is_some_and(|statistics| statistics.dtypes().get(field_index) != Some(&field_dtype))
        {
            return Ok(None);
        }
        let stats = statistics.and_then(|statistics| statistics.stats_sets().get(field_index));
        let nulls = read_null_count(stats, &field_dtype, rows, exact_statistics)?;
        let Some(value) =
            complete_column_measure(function, &field_dtype, rows, nulls, stats, exact_statistics)?
        else {
            return Ok(None);
        };
        values[index] = Some(value);
    }
    verify_min_max_consistency(&values, aggregate)?;
    Ok(Some((values, *exact_statistics - initial_exact)))
}

fn complete_from_stats(
    dtype: &DType,
    rows: u64,
    statistics: Option<&FileStatistics>,
    request: &VortexQueryPrimitiveRequest,
    states: &mut SimpleAggregateStates,
) -> Result<Option<Completion>> {
    if !admitted_shape(request) {
        return Ok(None);
    }
    let aggregate = required_simple_aggregate(request)?;
    if states.states.len() != aggregate.measures.len() {
        return Err(failed("measure state cardinality differs"));
    }
    validate_fresh_scalar_states(aggregate, states)?;
    let mut exact_statistics = 0;
    let Some((values, _)) =
        stage_values_from_stats(dtype, rows, statistics, request, &mut exact_statistics)?
    else {
        return Ok(None);
    };
    install_staged_values(values, aggregate, states)?;
    Ok(Some(Completion {
        source_rows: rows,
        exact_statistics,
        measures: aggregate.measures.len(),
        partitions_covered: None,
    }))
}

fn complete_column_measure(
    function: SimpleAggregateFunction,
    field_dtype: &DType,
    rows: u64,
    nulls: Option<u64>,
    stats: Option<&StatsSet>,
    exact_statistics: &mut usize,
) -> Result<Option<Value>> {
    match function {
        SimpleAggregateFunction::Count => {
            let Some(nulls) = nulls else {
                return Ok(None);
            };
            let present = rows
                .checked_sub(nulls)
                .ok_or_else(|| failed("NULL count exceeds rows"))?;
            Ok(Some(Value::Count(present)))
        }
        SimpleAggregateFunction::Min | SimpleAggregateFunction::Max => {
            complete_minmax_measure(function, field_dtype, rows, nulls, stats, exact_statistics)
        }
        SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg => {
            complete_sum_or_avg_measure(field_dtype, rows, nulls, stats, exact_statistics)
        }
        SimpleAggregateFunction::CountDistinct => Ok(None),
    }
}

fn parse_exact_u64_stat(stats: Option<&StatsSet>, stat: Stat, label: &str) -> Result<Option<u64>> {
    let Some(Precision::Exact(value)) = stats.map(|stats| stats.get(stat)) else {
        return Ok(None);
    };
    let scalar = Scalar::try_new(PType::U64.into(), Some(value)).map_err(vortex_error)?;
    scalar
        .as_primitive()
        .as_::<u64>()
        .map(Some)
        .ok_or_else(|| failed(&format!("{label} is not a non-null u64")))
}

fn read_null_count(
    stats: Option<&StatsSet>,
    field_dtype: &DType,
    rows: u64,
    exact_statistics: &mut usize,
) -> Result<Option<u64>> {
    if let Some(value) = parse_exact_u64_stat(stats, Stat::NullCount, "NULL count")? {
        if value > rows || (!field_dtype.is_nullable() && value != 0) {
            return Err(failed("NULL count contradicts source rows or dtype"));
        }
        *exact_statistics += 1;
        return Ok(Some(value));
    }
    if rows == 0 || !field_dtype.is_nullable() {
        return Ok(Some(0));
    }
    Ok(None)
}

fn is_admitted_integer_ptype(ptype: PType) -> bool {
    matches!(
        ptype,
        PType::I8
            | PType::I16
            | PType::I32
            | PType::I64
            | PType::U8
            | PType::U16
            | PType::U32
            | PType::U64
    )
}

fn complete_minmax_measure(
    function: SimpleAggregateFunction,
    field_dtype: &DType,
    rows: u64,
    nulls: Option<u64>,
    stats: Option<&StatsSet>,
    exact_statistics: &mut usize,
) -> Result<Option<Value>> {
    let admitted = match field_dtype {
        DType::Primitive(ptype, _) => {
            is_admitted_integer_ptype(*ptype) || matches!(ptype, PType::F32 | PType::F64)
        }
        DType::Bool(_) | DType::Utf8(_) => true,
        _ => false,
    };
    if !admitted {
        return Ok(None);
    }
    let stat = if function == SimpleAggregateFunction::Min {
        Stat::Min
    } else {
        Stat::Max
    };
    let extremum = if rows == 0 || nulls == Some(rows) {
        if matches!(stats.map(|s| s.get(stat)), Some(Precision::Exact(_))) {
            return Err(failed("extremum contradicts empty or all-NULL input"));
        }
        None
    } else {
        if matches!(field_dtype, DType::Primitive(PType::F32 | PType::F64, _)) {
            let Some(nan_count) = parse_exact_u64_stat(stats, Stat::NaNCount, "NaN count")? else {
                return Ok(None);
            };
            let valid_rows = rows.saturating_sub(nulls.unwrap_or(0));
            if nan_count > valid_rows {
                return Err(failed("NaN count contradicts source rows"));
            }
            if nan_count != 0 {
                return Ok(None);
            }
            *exact_statistics += 1;
        }
        let Some(Precision::Exact(raw)) = stats.map(|s| s.get(stat)) else {
            return Ok(None);
        };
        let stat_dtype = stat
            .dtype(field_dtype)
            .ok_or_else(|| failed("extremum statistic has no dtype"))?;
        let scalar = Scalar::try_new(stat_dtype, Some(raw)).map_err(vortex_error)?;
        let Some(value) = vortex_scalar_to_stat_value(&scalar) else {
            return Err(failed("extremum is not a supported scalar"));
        };
        match field_dtype {
            DType::Primitive(ptype, _) if is_admitted_integer_ptype(*ptype) => {
                if !matches!(value, StatValue::Int64(_) | StatValue::UInt64(_)) {
                    return Err(failed("extremum is not a native integer"));
                }
            }
            DType::Primitive(PType::F32 | PType::F64, _) => {
                let StatValue::Float64(number) = value else {
                    return Err(failed("extremum is not a native float"));
                };
                if !number.is_finite() || number == 0.0 {
                    // Signed zero (-0.0 vs +0.0) depends on scan encounter order; non-finite errors match full scan.
                    return Ok(None);
                }
            }
            DType::Bool(_) => {
                if !matches!(value, StatValue::Boolean(_)) {
                    return Err(failed("extremum is not a native boolean"));
                }
            }
            DType::Utf8(_) => {
                if !matches!(value, StatValue::Utf8(_)) {
                    return Err(failed("extremum is not a native utf8 string"));
                }
            }
            _ => return Ok(None),
        }
        *exact_statistics += 1;
        Some(value)
    };
    Ok(Some(if function == SimpleAggregateFunction::Min {
        Value::Min(extremum)
    } else {
        Value::Max(extremum)
    }))
}

fn parse_exact_integer_sum_stat(
    stats: Option<&StatsSet>,
    field_dtype: &DType,
    ptype: PType,
) -> Result<Option<(NumericAggregateSum, bool)>> {
    let Some(Precision::Exact(raw)) = stats.map(|s| s.get(Stat::Sum)) else {
        return Ok(None);
    };
    let stat_dtype = Stat::Sum
        .dtype(field_dtype)
        .ok_or_else(|| failed("integer sum statistic has no dtype"))?;
    let scalar = Scalar::try_new(stat_dtype, Some(raw)).map_err(vortex_error)?;
    let Some(value) = vortex_scalar_to_stat_value(&scalar) else {
        return Err(failed("sum statistic is not a supported scalar"));
    };
    let (sum, is_zero) = if ptype.is_signed_int() {
        let StatValue::Int64(total) = value else {
            return Err(failed("sum statistic is not a signed integer"));
        };
        (
            NumericAggregateSum::exact_integer(i128::from(total)),
            total == 0,
        )
    } else {
        let StatValue::UInt64(total) = value else {
            return Err(failed("sum statistic is not an unsigned integer"));
        };
        (
            NumericAggregateSum::exact_integer(i128::from(total)),
            total == 0,
        )
    };
    if !sum.is_finite() {
        return Err(failed("exact sum statistic is non-finite"));
    }
    Ok(Some((sum, is_zero)))
}

fn verify_exact_64bit_int_sum_bounds(
    stats: Option<&StatsSet>,
    field_dtype: &DType,
    ptype: PType,
    exact_statistics: &mut usize,
) -> Result<bool> {
    let read_bound = |stat: Stat| -> Result<Option<StatValue>> {
        let Some(Precision::Exact(raw)) = stats.map(|s| s.get(stat)) else {
            return Ok(None);
        };
        let stat_dtype = stat
            .dtype(field_dtype)
            .ok_or_else(|| failed("integer extremum statistic has no dtype"))?;
        let scalar = Scalar::try_new(stat_dtype, Some(raw)).map_err(vortex_error)?;
        let Some(value) = vortex_scalar_to_stat_value(&scalar) else {
            return Err(failed("extremum is not a supported scalar"));
        };
        let in_exact_range = match (ptype, &value) {
            (PType::I64, StatValue::Int64(v)) => v.unsigned_abs() <= MAX_EXACT_F64_INT,
            (PType::U64, StatValue::UInt64(v)) => *v <= MAX_EXACT_F64_INT,
            _ => return Err(failed("extremum does not match 64-bit integer signedness")),
        };
        Ok(in_exact_range.then_some(value))
    };
    let Some(min) = read_bound(Stat::Min)? else {
        return Ok(false);
    };
    let Some(max) = read_bound(Stat::Max)? else {
        return Ok(false);
    };
    if stat_value_cmp(&min, &max) == Some(std::cmp::Ordering::Greater) {
        return Err(failed("exact minimum exceeds exact maximum"));
    }
    *exact_statistics += 2;
    Ok(true)
}

fn complete_sum_or_avg_measure(
    field_dtype: &DType,
    rows: u64,
    nulls: Option<u64>,
    stats: Option<&StatsSet>,
    exact_statistics: &mut usize,
) -> Result<Option<Value>> {
    let DType::Primitive(ptype, _) = field_dtype else {
        return Ok(None);
    };
    if !is_admitted_integer_ptype(*ptype) {
        return Ok(None);
    }
    let Some(nulls) = nulls else {
        return Ok(None);
    };
    let present = rows
        .checked_sub(nulls)
        .ok_or_else(|| failed("NULL count exceeds rows"))?;
    if present == 0 {
        if let Some((_, is_zero)) = parse_exact_integer_sum_stat(stats, field_dtype, *ptype)? {
            if !is_zero {
                return Err(failed("sum contradicts empty or all-NULL input"));
            }
            *exact_statistics += 1;
        }
        return Ok(Some(Value::SumOrAvg {
            count: 0,
            sum: NumericAggregateSum::default(),
        }));
    }
    if matches!(ptype, PType::I64 | PType::U64)
        && !verify_exact_64bit_int_sum_bounds(stats, field_dtype, *ptype, exact_statistics)?
    {
        return Ok(None);
    }
    let Some((sum, _)) = parse_exact_integer_sum_stat(stats, field_dtype, *ptype)? else {
        return Ok(None);
    };
    *exact_statistics += 1;
    Ok(Some(Value::SumOrAvg {
        count: present,
        sum,
    }))
}

fn verify_min_max_consistency(
    values: &[Option<Value>; MAX_MEASURES],
    aggregate: &crate::VortexSimpleAggregateRequest,
) -> Result<()> {
    for (min_index, min) in values.iter().enumerate().take(aggregate.measures.len()) {
        let Some(Value::Min(Some(min))) = min else {
            continue;
        };
        for (max_index, max) in values.iter().enumerate().take(aggregate.measures.len()) {
            if aggregate.measures[min_index].column != aggregate.measures[max_index].column {
                continue;
            }
            if let Some(Value::Max(Some(max))) = max
                && stat_value_cmp(min, max) == Some(std::cmp::Ordering::Greater)
            {
                return Err(failed("exact minimum exceeds exact maximum"));
            }
        }
    }
    Ok(())
}

fn max_exact_statistics_for_request(request: &VortexQueryPrimitiveRequest) -> u64 {
    request.simple_aggregate.as_ref().map_or(0, |aggregate| {
        aggregate
            .measures
            .iter()
            .map(
                |measure| match SimpleAggregateFunction::parse(&measure.function) {
                    Ok(SimpleAggregateFunction::Count) => u64::from(measure.column.is_some()),
                    Ok(SimpleAggregateFunction::Min | SimpleAggregateFunction::Max) => 3,
                    Ok(SimpleAggregateFunction::Sum | SimpleAggregateFunction::Avg) => 4,
                    _ => 0,
                },
            )
            .sum()
    })
}

pub(super) fn report_is_safe(
    request: &VortexQueryPrimitiveRequest,
    report: &VortexLocalPrimitiveExecutionReport,
) -> bool {
    if !admitted_shape(request)
        || report.mode != VortexLocalPrimitiveExecutionMode::MetadataPreservingAggregate
        || report.upstream_scan_called
        || report.streaming_scan_used
        || report.data_read
        || report.data_decoded
        || report.data_materialized
        || report.row_read
        || report.arrow_converted
        || report.full_stream_collected
        || report.arrays_read_count != 0
        || !report.reader_splits.is_empty()
        || report.max_chunk_rows != 0
        || report.filter_pushdown_applied
        || report.upstream_filter_expression_used
        || report.projection_pushdown_applied
        || report.upstream_projection_expression_used
        || !report.embedded_layout.metadata_persisted_in_artifact
        || report.embedded_layout.metadata_pruned_entire_input
        || report.rows_selected != Some(report.embedded_layout.footer_row_count)
        || report.rows_scanned != report.embedded_layout.footer_row_count
        || report.rows_projected.is_none_or(|rows| rows > 1)
    {
        return false;
    }
    let Some(summary) = report
        .result_summary
        .as_deref()
        .and_then(|summary| summary.split_once(" values="))
        .map(|(_, summary)| summary)
    else {
        return false;
    };
    let Ok(payload) = serde_json::from_str::<serde_json::Value>(summary) else {
        return false;
    };
    let proof = &payload["metadata_aggregate"];
    let measure_count = request
        .simple_aggregate
        .as_ref()
        .map_or(0, |aggregate| aggregate.measures.len());
    let max_statistics = max_exact_statistics_for_request(request);
    let partition_count = proof
        .get("partitions_covered")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(1);
    partition_count >= 1
        && payload["rows"].as_u64() == report.rows_projected
        && payload["aggregate_update_strategy"] == "exact_file_footer_scalar_completion"
        && proof["schema_version"] == 1
        && proof["provider_version"] == crate::UPSTREAM_VORTEX_PROVIDER_VERSION
        && proof["source"] == "held_vortex_file_footer"
        && proof["source_rows_covered"] == report.embedded_layout.footer_row_count
        && proof["source_rows_visited"] == 0
        && proof["rows_scanned_field_scope"] == "source_row_count_metadata"
        && proof["source_payload_arrays_read"] == 0
        && proof["file_preparation_reads_excluded"] == true
        && proof["exact_statistics_consumed"]
            .as_u64()
            .is_some_and(|statistics| statistics <= max_statistics.saturating_mul(partition_count))
        && proof["measures_proven"] == measure_count
        && proof["nonnullable_struct_root"] == true
        && proof["all_measures_completed"] == true
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "native footer aggregate: {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "local_primitive_footer_aggregate_tests.rs"]
mod tests;
