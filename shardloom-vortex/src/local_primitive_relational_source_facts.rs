//! Exact held-file facts used only by fully bound typed scan predicates.

use super::plan::predicate_preserves_typed_semantics;
use shardloom_core::{ColumnRef, ComparisonOp, PredicateExpr, StatValue};
use vortex::{
    array::{
        dtype::{DType, PType},
        expr::stats::{Precision, Stat},
        scalar::Scalar,
        stats::StatsSet,
    },
    file::{FileStatistics, VortexFile},
};

pub(super) fn admit(predicate: PredicateExpr, file: &VortexFile) -> Option<PredicateExpr> {
    normalize(predicate, file.dtype(), file.row_count(), file.file_stats())
}

fn normalize(
    predicate: PredicateExpr,
    dtype: &DType,
    rows: u64,
    statistics: Option<&FileStatistics>,
) -> Option<PredicateExpr> {
    if predicate_preserves_typed_semantics(&predicate, dtype) {
        return Some(predicate);
    }
    match predicate {
        PredicateExpr::And(children) => Some(PredicateExpr::And(
            children
                .into_iter()
                .map(|child| normalize(child, dtype, rows, statistics))
                .collect::<Option<_>>()?,
        )),
        PredicateExpr::Compare { column, op, value } => {
            let ptype = finite_column(dtype, rows, statistics, &column)?;
            let value = exact_literal(&value, ptype)?;
            Some(zero_comparison(&column, op, value))
        }
        PredicateExpr::InList {
            column,
            values,
            negated: false,
        } => {
            let ptype = finite_column(dtype, rows, statistics, &column)?;
            let mut normalized = Vec::with_capacity(values.len().checked_mul(2)?);
            for value in values {
                let value = exact_literal(&value, ptype)?;
                if value == 0.0 {
                    normalized.extend([StatValue::Float64(-0.0), StatValue::Float64(0.0)]);
                } else {
                    normalized.push(StatValue::Float64(value));
                }
            }
            Some(PredicateExpr::InList {
                column,
                values: normalized,
                negated: false,
            })
        }
        PredicateExpr::IsNull { column } => {
            finite_column(dtype, rows, statistics, &column)?;
            Some(PredicateExpr::IsNull { column })
        }
        PredicateExpr::IsNotNull { column } => {
            finite_column(dtype, rows, statistics, &column)?;
            Some(PredicateExpr::IsNotNull { column })
        }
        _ => None,
    }
}

fn finite_column(
    dtype: &DType,
    rows: u64,
    statistics: Option<&FileStatistics>,
    column: &ColumnRef,
) -> Option<PType> {
    if dtype.is_nullable() {
        return None;
    }
    let fields = dtype.as_struct_fields_opt()?;
    let index = fields.find(column.as_str())?;
    let field = fields.field(column.as_str())?;
    let DType::Primitive(ptype @ (PType::F32 | PType::F64), _) = &field else {
        return None;
    };
    if statistics.is_some_and(|stats| stats.dtypes().get(index) != Some(&field)) {
        return None;
    }
    let stats = statistics.and_then(|stats| stats.stats_sets().get(index));
    let nulls = stats.and_then(|stats| count(stats, Stat::NullCount));
    let nans = stats.and_then(|stats| count(stats, Stat::NaNCount));
    if stats.is_some_and(|stats| {
        (matches!(stats.get(Stat::NullCount), Precision::Exact(_)) && nulls.is_none())
            || (matches!(stats.get(Stat::NaNCount), Precision::Exact(_)) && nans.is_none())
    }) {
        return None;
    }
    if nulls.is_some_and(|count| count > rows || (!field.is_nullable() && count != 0))
        || nans.is_some_and(|count| count > rows.saturating_sub(nulls.unwrap_or(0)))
    {
        return None;
    }
    if rows == 0 || nulls == Some(rows) {
        // A non-null extremum contradicts an empty valid population. Absent
        // or inexact extrema provide no additional fact and need not be read.
        if stats.is_some_and(|stats| {
            matches!(stats.get(Stat::Min), Precision::Exact(_))
                || matches!(stats.get(Stat::Max), Precision::Exact(_))
        }) {
            return None;
        }
        return Some(*ptype);
    }
    let stats = stats?;
    if nans != Some(0) {
        return None;
    }
    let min = extremum(stats, Stat::Min, &field)?;
    let max = extremum(stats, Stat::Max, &field)?;
    (min.is_finite() && max.is_finite() && min.total_cmp(&max).is_le()).then_some(*ptype)
}

fn count(stats: &StatsSet, stat: Stat) -> Option<u64> {
    let Precision::Exact(value) = stats.get(stat) else {
        return None;
    };
    Scalar::try_new(PType::U64.into(), Some(value))
        .ok()?
        .as_primitive()
        .as_::<u64>()
}

fn extremum(stats: &StatsSet, stat: Stat, dtype: &DType) -> Option<f64> {
    let Precision::Exact(value) = stats.get(stat) else {
        return None;
    };
    let scalar = Scalar::try_new(stat.dtype(dtype)?, Some(value)).ok()?;
    let StatValue::Float64(value) = crate::local_primitives::vortex_scalar_to_stat_value(&scalar)?
    else {
        return None;
    };
    Some(value)
}

#[allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)] // Widened integer round trips explicitly reject rounding and saturation.
fn exact_literal(value: &StatValue, ptype: PType) -> Option<f64> {
    let value = match value {
        StatValue::Float64(value) if value.is_finite() => *value,
        StatValue::Int64(value) => {
            let converted = *value as f64;
            (converted as i128 == i128::from(*value)).then_some(converted)?
        }
        StatValue::UInt64(value) => {
            let converted = *value as f64;
            (converted as i128 == i128::from(*value)).then_some(converted)?
        }
        _ => return None,
    };
    match ptype {
        PType::F32 => shardloom_core::float_policy::narrow_f32_exact(value).map(f64::from),
        PType::F64 => Some(value),
        _ => None,
    }
}

fn zero_comparison(column: &ColumnRef, op: ComparisonOp, value: f64) -> PredicateExpr {
    use ComparisonOp::{Eq, Gt, GtEq, Lt, LtEq, NotEq};
    let compare = |op, value| PredicateExpr::Compare {
        column: column.clone(),
        op,
        value: StatValue::Float64(value),
    };
    if value != 0.0 {
        return compare(op, value);
    }
    match op {
        Eq => PredicateExpr::And(vec![compare(GtEq, -0.0), compare(LtEq, 0.0)]),
        NotEq => PredicateExpr::And(vec![compare(NotEq, -0.0), compare(NotEq, 0.0)]),
        Lt | GtEq => compare(op, -0.0),
        LtEq | Gt => compare(op, 0.0),
    }
}

#[cfg(test)]
#[path = "local_primitive_relational_source_facts_tests.rs"]
mod tests;
