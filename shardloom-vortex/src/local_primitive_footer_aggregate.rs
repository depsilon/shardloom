//! Exact scalar completion from the already admitted native file footer.
//! No metadata answer is installed until every measure has been proven.

use super::{
    Result, ShardLoomError, SimpleAggregateFunction, SimpleAggregateStates, StatValue,
    VortexLocalPrimitiveExecutionMode, VortexLocalPrimitiveExecutionReport,
    VortexQueryPrimitiveKind, VortexQueryPrimitiveRequest, required_simple_aggregate,
    stat_value_cmp, vortex_error, vortex_scalar_to_stat_value,
};
use vortex::array::{
    dtype::{DType, Nullability, PType},
    expr::stats::{Precision, Stat},
    scalar::Scalar,
};
use vortex::file::{FileStatistics, VortexFile};

const MAX_MEASURES: usize = 64;

enum Value {
    Count(u64),
    Min(Option<StatValue>),
    Max(Option<StatValue>),
}

pub(super) struct Completion {
    source_rows: u64,
    exact_statistics: usize,
    measures: usize,
}

impl Completion {
    pub(super) fn annotate(&self, summary: &mut String) -> Result<()> {
        let mut payload: serde_json::Value = serde_json::from_str(summary).map_err(vortex_error)?;
        payload["aggregate_update_strategy"] = "exact_file_footer_scalar_completion".into();
        payload["metadata_aggregate"] = serde_json::json!({
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
        *summary = payload.to_string();
        Ok(())
    }
}

fn admitted_shape(request: &VortexQueryPrimitiveRequest) -> bool {
    request.kind == VortexQueryPrimitiveKind::SimpleAggregate
        && request.predicate.is_none()
        && request.simple_aggregate.as_ref().is_some_and(|aggregate| {
            !aggregate.measures.is_empty()
                && aggregate.measures.len() <= MAX_MEASURES
                && aggregate.group_by.is_empty()
                && aggregate.group_expressions.is_empty()
                && aggregate.spill.is_none()
                // COUNT(*) alone already has its own metadata primitive.
                && aggregate.measures.iter().any(|measure| measure.column.is_some())
                && aggregate.measures.iter().all(|measure| {
                    measure.argument_offset.is_none()
                        && measure.value_transform.is_none()
                        && matches!(SimpleAggregateFunction::parse(&measure.function), Ok(
                            SimpleAggregateFunction::Count | SimpleAggregateFunction::Min | SimpleAggregateFunction::Max
                        ))
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

#[allow(clippy::too_many_lines)] // Keep proof staging and its single commit point together.
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
    let DType::Struct(fields, Nullability::NonNullable) = dtype else {
        return Ok(None);
    };
    let aggregate = required_simple_aggregate(request)?;
    if states.states.len() != aggregate.measures.len() {
        return Err(failed("measure state cardinality differs"));
    }
    // Fixed-size query-local staging avoids an unreserved input-sized allocation.
    let mut values: [Option<Value>; MAX_MEASURES] = std::array::from_fn(|_| None);
    let mut exact_statistics = 0;
    for (index, measure) in aggregate.measures.iter().enumerate() {
        let function = SimpleAggregateFunction::parse(&measure.function)?;
        let state = &states.states[index];
        if state.function != function
            || state.alias != measure.alias
            || state.count != 0
            || state.min.is_some()
            || state.max.is_some()
            || !state.distinct_values.is_empty()
        {
            return Err(failed("requires fresh matching scalar state"));
        }
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
        let DType::Primitive(ptype, nullable) = &field_dtype else {
            return Ok(None);
        };
        if !matches!(
            ptype,
            PType::I8
                | PType::I16
                | PType::I32
                | PType::I64
                | PType::U8
                | PType::U16
                | PType::U32
                | PType::U64
        ) {
            return Ok(None);
        }
        if statistics
            .is_some_and(|statistics| statistics.dtypes().get(field_index) != Some(&field_dtype))
        {
            return Ok(None);
        }
        let stats = statistics.and_then(|statistics| statistics.stats_sets().get(field_index));
        let nulls = match stats.map(|stats| stats.get(Stat::NullCount)) {
            Some(Precision::Exact(value)) => {
                let scalar =
                    Scalar::try_new(PType::U64.into(), Some(value)).map_err(vortex_error)?;
                let value = scalar
                    .as_primitive()
                    .as_::<u64>()
                    .ok_or_else(|| failed("NULL count is not a non-null u64"))?;
                if value > rows || (*nullable == Nullability::NonNullable && value != 0) {
                    return Err(failed("NULL count contradicts source rows or dtype"));
                }
                exact_statistics += 1;
                Some(value)
            }
            _ if rows == 0 || *nullable == Nullability::NonNullable => Some(0),
            _ => None,
        };
        // Exact non-null extrema suffice even if a nullable field lacks NullCount.
        let value = match function {
            SimpleAggregateFunction::Count => {
                let Some(nulls) = nulls else {
                    return Ok(None);
                };
                Value::Count(
                    rows.checked_sub(nulls)
                        .ok_or_else(|| failed("NULL count exceeds rows"))?,
                )
            }
            SimpleAggregateFunction::Min | SimpleAggregateFunction::Max => {
                let stat = if function == SimpleAggregateFunction::Min {
                    Stat::Min
                } else {
                    Stat::Max
                };
                let extremum = match stats.map(|stats| stats.get(stat)) {
                    Some(Precision::Exact(value)) => {
                        if rows == 0 || nulls == Some(rows) {
                            return Err(failed("extremum contradicts empty or all-NULL input"));
                        }
                        let stat_dtype = stat
                            .dtype(&field_dtype)
                            .ok_or_else(|| failed("integer statistic has no dtype"))?;
                        let scalar =
                            Scalar::try_new(stat_dtype, Some(value)).map_err(vortex_error)?;
                        let value = vortex_scalar_to_stat_value(&scalar)
                            .ok_or_else(|| failed("extremum is not a supported scalar"))?;
                        if !matches!(value, StatValue::Int64(_) | StatValue::UInt64(_)) {
                            return Err(failed("extremum is not a native integer"));
                        }
                        exact_statistics += 1;
                        Some(value)
                    }
                    _ if rows == 0 || nulls == Some(rows) => None,
                    _ => return Ok(None),
                };
                if function == SimpleAggregateFunction::Min {
                    Value::Min(extremum)
                } else {
                    Value::Max(extremum)
                }
            }
            _ => return Ok(None),
        };
        values[index] = Some(value);
    }
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
    for (state, value) in states.states.iter_mut().zip(values) {
        match value.ok_or_else(|| failed("incomplete staged measure"))? {
            Value::Count(value) => state.count = value,
            Value::Min(value) => state.min = value,
            Value::Max(value) => state.max = value,
        }
    }
    Ok(Some(Completion {
        source_rows: rows,
        exact_statistics,
        measures: aggregate.measures.len(),
    }))
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
    payload["rows"].as_u64() == report.rows_projected
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
            .is_some_and(|statistics| {
                statistics
                    <= u64::try_from(measure_count)
                        .unwrap_or(u64::MAX)
                        .saturating_mul(2)
            })
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
