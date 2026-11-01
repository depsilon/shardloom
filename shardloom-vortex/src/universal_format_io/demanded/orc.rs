//! Held ORC metadata supplies exact totals and conservative early stripe exclusion.
//! ORC remains a codec: residual evaluation belongs to native operators.

use super::{
    LocalColumnarSource, OwnedLocalRead, PredicateExpr, Result, SourceGroup, failed, statistics,
};
use arrow_schema::{DataType, Schema};
use orc_rust::{
    reader::metadata::FileMetadata,
    statistics::{ColumnStatistics, TypeStatistics},
};
use shardloom_core::{SegmentStats, ShardLoomError, StatValue, StatisticsExactness};
use shardloom_exec::{
    compute_pool::CancellationToken,
    live_memory::{LiveMemoryPool, MemoryLease},
};
use std::sync::Arc;

#[cfg(test)]
#[path = "orc_statistics_tests.rs"]
mod tests;

pub(super) fn admit(
    mut input: OwnedLocalRead,
    memory: &LiveMemoryPool,
    cancellation: &CancellationToken,
) -> Result<(Arc<FileMetadata>, MemoryLease)> {
    let mut credit = memory.reserve(0)?;
    let metadata = orc_rust::reader::metadata::read_metadata_with_memory(
        &mut input,
        &mut |bytes| {
            cancellation.check().map_err(std::io::Error::other)?;
            credit.resize(bytes).map_err(std::io::Error::other)
        },
        super::generic::decoder::orc_memory(memory, cancellation),
    )
    .map_err(source_error)?;
    cancellation.check()?;
    Ok((Arc::new(metadata), credit))
}

fn source_error(error: orc_rust::error::OrcError) -> ShardLoomError {
    super::generic::decoder::source_error(error.into())
}

pub(super) fn prepare_groups(
    source: &LocalColumnarSource,
    max_rows: Option<u64>,
    predicates: &[Option<PredicateExpr>],
    cancellation: &CancellationToken,
) -> Result<Vec<SourceGroup>> {
    let owner = source
        .other
        .as_ref()
        .ok_or_else(|| failed("ORC source owner missing"))?;
    let mut groups = Vec::new();
    let mut source_rows = 0_u64;
    'files: for (file_index, file) in owner.files.iter().enumerate() {
        let metadata = file
            .orc
            .as_ref()
            .ok_or_else(|| failed("ORC metadata owner missing"))?;
        let partitions = &source.collection.files[file_index].partitions;
        for (index, stripe) in metadata.stripe_metadatas().iter().enumerate() {
            cancellation.check()?;
            if max_rows.is_some_and(|limit| source_rows >= limit) {
                break 'files;
            }
            let first_row = source_rows;
            let rows = stripe.number_of_rows();
            source_rows = source_rows
                .checked_add(rows)
                .ok_or_else(|| failed("source row count overflow"))?;
            if predicates.is_empty()
                || predicates.iter().any(|predicate| {
                    predicate.as_ref().is_none_or(|predicate| {
                        !excluded(
                            metadata,
                            index,
                            predicate,
                            &file.schema,
                            &owner.schema,
                            partitions,
                        )
                    })
                })
            {
                groups.try_reserve(1).map_err(failed)?;
                groups.push(SourceGroup {
                    file: file_index,
                    index,
                    first_row,
                    rows,
                });
            }
        }
    }
    cancellation.check()?;
    Ok(groups)
}

fn excluded(
    metadata: &FileMetadata,
    index: usize,
    predicate: &PredicateExpr,
    physical: &Schema,
    logical: &Schema,
    partitions: &[(String, Option<String>)],
) -> bool {
    match predicate {
        PredicateExpr::AlwaysFalse => return true,
        PredicateExpr::And(children) => {
            return children
                .iter()
                .any(|child| excluded(metadata, index, child, physical, logical, partitions));
        }
        _ => {}
    }
    let Some(column) = predicate.column() else {
        return false;
    };
    let stripe = &metadata.stripe_metadatas()[index];
    let rows = stripe.number_of_rows();
    if let Some(stats) =
        statistics::mapped_stats(column.as_str(), physical, logical, partitions, rows)
    {
        return statistics::excluded_by_stats(predicate, &stats);
    }
    let Some(field) = metadata
        .root_data_type()
        .children()
        .iter()
        .find(|field| field.name() == column.as_str())
    else {
        return false;
    };
    let Some(stats) = stripe
        .column_statistics()
        .get(field.data_type().column_index())
    else {
        return false;
    };
    let Ok(physical) = physical.field_with_name(column.as_str()) else {
        return false;
    };
    let Some(stats) = primitive_stats(stats, rows, physical.data_type()) else {
        return false;
    };
    statistics::excluded_by_stats(predicate, &stats)
}

fn primitive_stats(value: &ColumnStatistics, rows: u64, dtype: &DataType) -> Option<SegmentStats> {
    if !matches!(
        dtype,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
    ) {
        return None;
    }
    let count = value.number_of_values_opt();
    if count.is_some_and(|count| count > rows)
        || count
            .zip(value.has_null_opt())
            .is_some_and(|(count, nulls)| nulls != (count < rows))
    {
        return None;
    }
    let mut result = SegmentStats::with_row_count(rows);
    result.exactness = StatisticsExactness::Exact;
    result.null_count = count
        .map(|count| rows - count)
        .or_else(|| (value.has_null_opt() == Some(false)).then_some(0));
    if let Some((min, max)) = bounds(value, dtype) {
        result.min_value = Some(min);
        result.max_value = Some(max);
    }
    Some(result)
}

fn bounds(value: &ColumnStatistics, dtype: &DataType) -> Option<(StatValue, StatValue)> {
    match (value.type_statistics()?, dtype) {
        (
            TypeStatistics::Integer { min, max, .. },
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64,
        ) => {
            let valid = match dtype {
                DataType::Int8 => i8::try_from(*min).is_ok() && i8::try_from(*max).is_ok(),
                DataType::Int16 => i16::try_from(*min).is_ok() && i16::try_from(*max).is_ok(),
                DataType::Int32 => i32::try_from(*min).is_ok() && i32::try_from(*max).is_ok(),
                _ => true,
            };
            (valid && min <= max).then_some((StatValue::Int64(*min), StatValue::Int64(*max)))
        }
        (TypeStatistics::Bucket { true_count }, DataType::Boolean) => {
            let total = value.number_of_values_opt()?;
            (total > 0 && *true_count <= total).then_some((
                StatValue::Boolean(*true_count == total),
                StatValue::Boolean(*true_count > 0),
            ))
        }
        (
            TypeStatistics::String {
                lower_bound,
                upper_bound,
                is_exact_min: true,
                is_exact_max: true,
                ..
            },
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View,
        ) if lower_bound <= upper_bound => Some((
            StatValue::Utf8(lower_bound.clone()),
            StatValue::Utf8(upper_bound.clone()),
        )),
        _ => None,
    }
}
