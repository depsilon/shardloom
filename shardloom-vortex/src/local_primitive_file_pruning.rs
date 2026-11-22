//! Conservative file-level metadata pruning from held Vortex footer statistics.
//!
//! Supplements upstream [`VortexFile::can_prune`] only with exact, type-safe
//! bounds already persisted in the file footer:
//! - finite `F32`/`F64` comparisons when `Stat::NaNCount == Some(0)`
//! - `Bool`, `Utf8`, and integer `InList`, constant `NotEq`, `IsNull`, `IsNotNull`
//! - constant `StringContains` (`negated: false` and `negated: true`)
//! - recursive `And` conjunctions
//!
//! Flat controls without [`FileStatistics`] (`file.file_stats().is_none()`) are
//! never pruned here so differential flat-control tests remain faithful.

use super::{
    Result, stat_value_cmp, stat_value_to_f64, stat_value_to_i64_for_column,
    stat_value_to_u64_for_column, vortex_error, vortex_scalar_to_stat_value,
};
use shardloom_core::{ComparisonOp, PredicateExpr, StatValue};
use std::cmp::Ordering;
use vortex::array::{
    dtype::{DType, PType, StructFields},
    expr::{
        Expression,
        stats::{Precision, Stat},
    },
    scalar::Scalar,
    stats::StatsSet,
};
use vortex::file::{FileStatistics, VortexFile};

pub(in crate::local_primitives) fn can_prune_file_with_predicate(
    file: &VortexFile,
    filter: &Expression,
    predicate: Option<&PredicateExpr>,
) -> Result<bool> {
    if file.can_prune(filter).map_err(vortex_error)? {
        return Ok(true);
    }
    let Some(predicate) = predicate else {
        return Ok(false);
    };
    let Some(file_stats) = file.file_stats() else {
        // Flat controls omit file statistics; never fabricate pruning without footer stats.
        return Ok(false);
    };
    let DType::Struct(fields, vortex::array::dtype::Nullability::NonNullable) = file.dtype() else {
        return Ok(false);
    };
    Ok(predicate_falsified_by_file_stats(
        predicate,
        fields,
        file.row_count(),
        file_stats,
    ))
}

fn predicate_falsified_by_file_stats(
    predicate: &PredicateExpr,
    fields: &StructFields,
    row_count: u64,
    file_stats: &FileStatistics,
) -> bool {
    match predicate {
        PredicateExpr::AlwaysFalse => true,
        PredicateExpr::AlwaysTrue => false,
        PredicateExpr::And(terms) => terms
            .iter()
            .any(|term| predicate_falsified_by_file_stats(term, fields, row_count, file_stats)),
        PredicateExpr::IsNull { column } => {
            if row_count == 0 {
                return true;
            }
            let Some((field_dtype, stats)) =
                lookup_column_stats(column.as_str(), fields, file_stats)
            else {
                return false;
            };
            exact_null_count(stats, &field_dtype, row_count) == Some(0)
        }
        PredicateExpr::IsNotNull { column } => {
            if row_count == 0 {
                return true;
            }
            let Some((field_dtype, stats)) =
                lookup_column_stats(column.as_str(), fields, file_stats)
            else {
                return false;
            };
            exact_null_count(stats, &field_dtype, row_count) == Some(row_count)
        }
        PredicateExpr::Compare { column, op, value } => compare_falsified_by_file_stats(
            column.as_str(),
            *op,
            value,
            fields,
            row_count,
            file_stats,
        ),
        PredicateExpr::InList {
            column,
            values,
            negated,
        } => in_list_falsified_by_file_stats(
            column.as_str(),
            values,
            *negated,
            fields,
            row_count,
            file_stats,
        ),
        PredicateExpr::StringContains {
            column,
            needle,
            negated,
        } => string_contains_falsified_by_file_stats(
            column.as_str(),
            needle,
            *negated,
            fields,
            row_count,
            file_stats,
        ),
    }
}

fn lookup_column_stats<'a>(
    column: &str,
    fields: &StructFields,
    file_stats: &'a FileStatistics,
) -> Option<(DType, Option<&'a StatsSet>)> {
    let field_index = fields.find(column)?;
    let field_dtype = fields.field_by_index(field_index)?;
    if file_stats.dtypes().get(field_index) != Some(&field_dtype) {
        return None;
    }
    Some((field_dtype, file_stats.stats_sets().get(field_index)))
}

fn exact_null_count(stats: Option<&StatsSet>, field_dtype: &DType, row_count: u64) -> Option<u64> {
    if let Some(Precision::Exact(raw)) = stats.map(|stats| stats.get(Stat::NullCount)) {
        let scalar = Scalar::try_new(PType::U64.into(), Some(raw)).ok()?;
        let nulls = scalar.as_primitive().as_::<u64>()?;
        if nulls > row_count || (!field_dtype.is_nullable() && nulls != 0) {
            return None;
        }
        return Some(nulls);
    }
    if row_count == 0 || !field_dtype.is_nullable() {
        return Some(0);
    }
    None
}

fn exact_nan_count(stats: Option<&StatsSet>, row_count: u64, nulls: Option<u64>) -> Option<u64> {
    let Precision::Exact(raw) = stats?.get(Stat::NaNCount) else {
        return None;
    };
    let scalar = Scalar::try_new(PType::U64.into(), Some(raw)).ok()?;
    let nans = scalar.as_primitive().as_::<u64>()?;
    let max_valid = row_count.saturating_sub(nulls.unwrap_or(0));
    (nans <= max_valid).then_some(nans)
}

fn parse_exact_bound(
    stat: Stat,
    field_dtype: &DType,
    stats: Option<&StatsSet>,
) -> Option<StatValue> {
    let Precision::Exact(raw) = stats?.get(stat) else {
        return None;
    };
    let stat_dtype = stat.dtype(field_dtype)?;
    let scalar = Scalar::try_new(stat_dtype, Some(raw)).ok()?;
    let value = vortex_scalar_to_stat_value(&scalar)?;
    match (field_dtype, &value) {
        (DType::Primitive(PType::F32 | PType::F64, _), StatValue::Float64(number)) => {
            number.is_finite().then_some(value)
        }
        (DType::Primitive(ptype, _), StatValue::Int64(_)) if ptype.is_signed_int() => Some(value),
        (DType::Primitive(ptype, _), StatValue::UInt64(_)) if ptype.is_unsigned_int() => {
            Some(value)
        }
        (DType::Bool(_), StatValue::Boolean(_)) | (DType::Utf8(_), StatValue::Utf8(_)) => {
            Some(value)
        }
        _ => None,
    }
}

fn exact_minmax(
    stats: Option<&StatsSet>,
    field_dtype: &DType,
    row_count: u64,
    nulls: Option<u64>,
) -> Option<(StatValue, StatValue)> {
    if row_count == 0 || nulls == Some(row_count) {
        return None;
    }
    if matches!(field_dtype, DType::Primitive(PType::F32 | PType::F64, _))
        && exact_nan_count(stats, row_count, nulls) != Some(0)
    {
        return None;
    }
    let min = parse_exact_bound(Stat::Min, field_dtype, stats)?;
    let max = parse_exact_bound(Stat::Max, field_dtype, stats)?;
    if matches!(stat_value_cmp(&min, &max), None | Some(Ordering::Greater)) {
        return None;
    }
    Some((min, max))
}

#[allow(clippy::cast_possible_truncation)]
fn coerce_literal_for_field_dtype(
    value: &StatValue,
    field_dtype: &DType,
    column: &str,
) -> Option<StatValue> {
    match field_dtype {
        DType::Bool(_) => match value {
            StatValue::Boolean(flag) => Some(StatValue::Boolean(*flag)),
            _ => None,
        },
        DType::Utf8(_) => match value {
            StatValue::Utf8(text) => Some(StatValue::Utf8(text.clone())),
            _ => None,
        },
        DType::Primitive(ptype, _) if ptype.is_unsigned_int() => {
            let parsed = stat_value_to_u64_for_column(value, Some(column)).ok()?;
            let fits = match ptype {
                PType::U8 => u8::try_from(parsed).is_ok(),
                PType::U16 => u16::try_from(parsed).is_ok(),
                PType::U32 => u32::try_from(parsed).is_ok(),
                PType::U64 => true,
                _ => false,
            };
            fits.then_some(StatValue::UInt64(parsed))
        }
        DType::Primitive(ptype, _) if ptype.is_signed_int() => {
            let parsed = stat_value_to_i64_for_column(value, Some(column)).ok()?;
            let fits = match ptype {
                PType::I8 => i8::try_from(parsed).is_ok(),
                PType::I16 => i16::try_from(parsed).is_ok(),
                PType::I32 => i32::try_from(parsed).is_ok(),
                PType::I64 => true,
                _ => false,
            };
            fits.then_some(StatValue::Int64(parsed))
        }
        DType::Primitive(PType::F32, _) => {
            let number = stat_value_to_f64(value).ok()? as f32;
            number
                .is_finite()
                .then_some(StatValue::Float64(f64::from(number)))
        }
        DType::Primitive(PType::F64, _) => {
            let number = stat_value_to_f64(value).ok()?;
            number.is_finite().then_some(StatValue::Float64(number))
        }
        _ => None,
    }
}

fn value_outside_minmax(literal: &StatValue, min: &StatValue, max: &StatValue) -> bool {
    matches!(stat_value_cmp(literal, min), Some(Ordering::Less))
        || matches!(stat_value_cmp(literal, max), Some(Ordering::Greater))
}

fn compare_falsified_by_file_stats(
    column: &str,
    op: ComparisonOp,
    value: &StatValue,
    fields: &StructFields,
    row_count: u64,
    file_stats: &FileStatistics,
) -> bool {
    if row_count == 0 {
        return true;
    }
    if matches!(value, StatValue::Null) {
        return false;
    }
    let Some((field_dtype, stats)) = lookup_column_stats(column, fields, file_stats) else {
        return false;
    };
    let nulls = exact_null_count(stats, &field_dtype, row_count);
    if nulls == Some(row_count) {
        return true;
    }
    let Some(literal) = coerce_literal_for_field_dtype(value, &field_dtype, column) else {
        return false;
    };
    let Some((min, max)) = exact_minmax(stats, &field_dtype, row_count, nulls) else {
        return false;
    };
    match op {
        ComparisonOp::Eq => value_outside_minmax(&literal, &min, &max),
        ComparisonOp::NotEq => {
            nulls == Some(0)
                && stat_value_cmp(&min, &max) == Some(Ordering::Equal)
                && stat_value_cmp(&min, &literal) == Some(Ordering::Equal)
        }
        ComparisonOp::Lt => !matches!(stat_value_cmp(&literal, &min), Some(Ordering::Greater)),
        ComparisonOp::LtEq => stat_value_cmp(&literal, &min) == Some(Ordering::Less),
        ComparisonOp::Gt => !matches!(stat_value_cmp(&literal, &max), Some(Ordering::Less)),
        ComparisonOp::GtEq => stat_value_cmp(&literal, &max) == Some(Ordering::Greater),
    }
}

fn in_list_falsified_by_file_stats(
    column: &str,
    values: &[StatValue],
    negated: bool,
    fields: &StructFields,
    row_count: u64,
    file_stats: &FileStatistics,
) -> bool {
    if row_count == 0 {
        return true;
    }
    let Some((field_dtype, stats)) = lookup_column_stats(column, fields, file_stats) else {
        return false;
    };
    let nulls = exact_null_count(stats, &field_dtype, row_count);
    let has_null_literal = values.iter().any(|value| matches!(value, StatValue::Null));

    if !negated {
        if values.is_empty() {
            return true;
        }
        if has_null_literal {
            // InList containing NULL matches NULL rows (lowers to is_null(col) OR ...).
            if nulls == Some(0) && values.iter().all(|value| matches!(value, StatValue::Null)) {
                return true;
            }
            return false;
        }
        if nulls == Some(row_count) {
            return true;
        }
        let Some((min, max)) = exact_minmax(stats, &field_dtype, row_count, nulls) else {
            return false;
        };
        return values.iter().all(|raw| {
            coerce_literal_for_field_dtype(raw, &field_dtype, column)
                .is_some_and(|literal| value_outside_minmax(&literal, &min, &max))
        });
    }

    // Negated InList:
    // - without NULL literal lowers to `is_null(col) OR NOT(col IN (...))`
    // - with NULL literal lowers to `is_not_null(col) AND NOT(col IN (...))`
    if has_null_literal && nulls == Some(row_count) {
        return true;
    }
    if nulls != Some(0) {
        return false;
    }
    let Some((min, max)) = exact_minmax(stats, &field_dtype, row_count, nulls) else {
        return false;
    };
    if stat_value_cmp(&min, &max) != Some(Ordering::Equal) {
        return false;
    }
    values.iter().any(|raw| {
        !matches!(raw, StatValue::Null)
            && coerce_literal_for_field_dtype(raw, &field_dtype, column)
                .is_some_and(|literal| stat_value_cmp(&min, &literal) == Some(Ordering::Equal))
    })
}

fn string_contains_falsified_by_file_stats(
    column: &str,
    needle: &str,
    negated: bool,
    fields: &StructFields,
    row_count: u64,
    file_stats: &FileStatistics,
) -> bool {
    if row_count == 0 {
        return true;
    }
    let Some((field_dtype @ DType::Utf8(_), stats)) =
        lookup_column_stats(column, fields, file_stats)
    else {
        return false;
    };
    let nulls = exact_null_count(stats, &field_dtype, row_count);
    if nulls == Some(row_count) {
        return true;
    }
    let Some((StatValue::Utf8(min), StatValue::Utf8(max))) =
        exact_minmax(stats, &field_dtype, row_count, nulls)
    else {
        return false;
    };
    if min != max {
        return false;
    }
    if negated {
        nulls == Some(0) && min.contains(needle)
    } else {
        !min.contains(needle)
    }
}

#[cfg(test)]
#[path = "local_primitive_file_pruning_tests.rs"]
mod tests;
