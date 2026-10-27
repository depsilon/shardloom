//! Only exact, type-compatible primitive facts can exclude source regions.
use super::{ArrowReaderMetadata, DataType, PredicateExpr};
use parquet::file::statistics::Statistics;
use shardloom_core::{
    PredicateProof, SegmentStats, StatValue, StatisticsExactness, prove_predicate_from_stats,
};

pub(super) fn excluded(
    metadata: &ArrowReaderMetadata,
    group: usize,
    predicate: &PredicateExpr,
) -> bool {
    match predicate {
        PredicateExpr::AlwaysFalse => true,
        PredicateExpr::And(children) => children
            .iter()
            .any(|child| excluded(metadata, group, child)),
        _ => {
            let Some(column) = predicate.column() else {
                return false;
            };
            let Ok(index) = metadata.schema().index_of(column.as_str()) else {
                return false;
            };
            let descriptor = metadata.parquet_schema();
            let leaves: Vec<_> = (0..descriptor.num_columns())
                .filter(|&leaf| descriptor.get_column_root_idx(leaf) == index)
                .collect();
            if leaves.len() != 1 {
                return false;
            }
            let field = metadata.schema().field(index);
            if !matches!(
                field.data_type(),
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
            ) {
                return false;
            }
            let group = metadata.metadata().row_group(group);
            let Ok(rows) = u64::try_from(group.num_rows()) else {
                return false;
            };
            let Some(statistics) = group.column(leaves[0]).statistics() else {
                return false;
            };
            let mut stats = SegmentStats::with_row_count(rows);
            stats.null_count = statistics.null_count_opt();
            stats.exactness = StatisticsExactness::Exact;
            // A comparison cannot select SQL-null rows even when an all-null
            // chunk correctly has no min/max. Reuse the null-count proof rather
            // than manufacturing bounds or treating absent bounds as zero.
            if matches!(predicate, PredicateExpr::Compare { .. })
                && matches!(
                    prove_predicate_from_stats(
                        &PredicateExpr::IsNotNull {
                            column: column.clone()
                        },
                        &stats,
                    ),
                    PredicateProof::AlwaysFalse { .. }
                )
            {
                return true;
            }
            if !statistics.is_min_max_deprecated()
                && statistics.min_is_exact()
                && statistics.max_is_exact()
            {
                let (min, max) = bounds(statistics, field.data_type());
                stats.min_value = min;
                stats.max_value = max;
            }
            matches!(
                prove_predicate_from_stats(predicate, &stats),
                PredicateProof::AlwaysFalse { .. }
            )
        }
    }
}

fn bounds(statistics: &Statistics, dtype: &DataType) -> (Option<StatValue>, Option<StatValue>) {
    match (statistics, dtype) {
        (Statistics::Boolean(s), DataType::Boolean) => (
            s.min_opt().copied().map(StatValue::Boolean),
            s.max_opt().copied().map(StatValue::Boolean),
        ),
        (Statistics::Int32(s), DataType::Int8 | DataType::Int16 | DataType::Int32) => (
            s.min_opt().map(|&n| StatValue::Int64(i64::from(n))),
            s.max_opt().map(|&n| StatValue::Int64(i64::from(n))),
        ),
        (Statistics::Int64(s), DataType::Int64) => (
            s.min_opt().copied().map(StatValue::Int64),
            s.max_opt().copied().map(StatValue::Int64),
        ),
        (Statistics::Int32(s), DataType::UInt8 | DataType::UInt16 | DataType::UInt32) => (
            s.min_opt()
                .map(|&n| StatValue::UInt64(u64::from(u32::from_ne_bytes(n.to_ne_bytes())))),
            s.max_opt()
                .map(|&n| StatValue::UInt64(u64::from(u32::from_ne_bytes(n.to_ne_bytes())))),
        ),
        (Statistics::Int64(s), DataType::UInt64) => (
            s.min_opt()
                .map(|&n| StatValue::UInt64(u64::from_ne_bytes(n.to_ne_bytes()))),
            s.max_opt()
                .map(|&n| StatValue::UInt64(u64::from_ne_bytes(n.to_ne_bytes()))),
        ),
        (Statistics::ByteArray(s), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => (
            s.min_opt()
                .and_then(|v| std::str::from_utf8(v.data()).ok())
                .map(|v| StatValue::Utf8(v.to_owned())),
            s.max_opt()
                .and_then(|v| std::str::from_utf8(v.data()).ok())
                .map(|v| StatValue::Utf8(v.to_owned())),
        ),
        _ => (None, None),
    }
}
