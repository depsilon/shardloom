//! Build-side runtime min/max, `InList`, and 3-bit Bloom filters pushed onto probe scans.

use super::{
    native_capacity::{ReservedVec, bytes as reserved_bytes},
    native_relational_batch::{Batch, Table},
    native_relational_index::RowIndex,
    native_relational_keys::Cell,
    vortex_error,
};
use crate::relational_query::VortexRelationalJoinKind as Kind;
use crate::resident_session::NativeExecutionContext;
use shardloom_core::{ColumnRef, ComparisonOp, PredicateExpr, Result, StatValue};
use std::sync::Arc;
use vortex::{
    array::{
        ArrayRef,
        dtype::{DType, PType},
    },
    buffer::BitBufferMut,
    mask::Mask,
};

pub(crate) fn is_infallible_key_dtype(dtype: &DType) -> bool {
    matches!(
        dtype,
        DType::Bool(_)
            | DType::Utf8(_)
            | DType::Binary(_)
            | DType::Primitive(
                PType::I8
                    | PType::I16
                    | PType::I32
                    | PType::I64
                    | PType::U8
                    | PType::U16
                    | PType::U32
                    | PType::U64,
                _
            )
    )
}

const IN_LIST_LIMIT: usize = 64;
const MAX_UTF8_IN_LIST_BYTES: usize = 128;

pub(crate) struct ProbeBloomFilter {
    words: ReservedVec<u64>,
}

impl ProbeBloomFilter {
    fn new(
        unique_keys: usize,
        hashes: impl IntoIterator<Item = Result<Option<u64>>>,
        context: &NativeExecutionContext<'_>,
    ) -> Result<Option<Self>> {
        if unique_keys == 0 {
            return Ok(None);
        }
        let words_len = unique_keys
            .saturating_mul(16)
            .div_ceil(64)
            .next_power_of_two()
            .max(1);
        let bytes = reserved_bytes::<u64>(words_len)?;
        let capacity = context.memory().snapshot();
        let available = capacity.limit_bytes.saturating_sub(capacity.reserved_bytes);
        if bytes > available / 16 {
            return Ok(None);
        }
        let Some(mut words) = ReservedVec::<u64>::try_with_capacity(context.memory(), words_len)?
        else {
            return Ok(None);
        };
        words.values.resize(words_len, 0);
        for (index, hash) in hashes.into_iter().enumerate() {
            if index.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            let Some(hash) = hash? else {
                continue;
            };
            for (word, bit) in positions(hash, words_len) {
                words.values[word] |= bit;
            }
        }
        Ok(Some(Self { words }))
    }

    pub(crate) fn contains(&self, hash: u64) -> bool {
        let words = self.words.values.len();
        positions(hash, words)
            .into_iter()
            .all(|(word, bit)| (self.words.values[word] & bit) != 0)
    }
}

#[derive(Clone)]
pub(crate) struct ProbeRuntimeFilter {
    pub(crate) probe_columns: Vec<String>,
    pub(crate) empty_build: bool,
    pub(crate) key_predicate: Option<PredicateExpr>,
    pub(crate) bloom: Option<Arc<ProbeBloomFilter>>,
}

impl ProbeRuntimeFilter {
    pub(crate) fn remap(&self, mut map: impl FnMut(&str) -> Option<String>) -> Option<Self> {
        let mut probe_columns = Vec::with_capacity(self.probe_columns.len());
        for column in &self.probe_columns {
            probe_columns.push(map(column)?);
        }
        let key_predicate = self
            .key_predicate
            .as_ref()
            .and_then(|predicate| remap_predicate(predicate, &mut map));
        Some(Self {
            probe_columns,
            empty_build: self.empty_build,
            key_predicate,
            bloom: self.bloom.clone(),
        })
    }
}

pub(super) fn build_probe_filters(
    kind: Kind,
    has_asof: bool,
    left_keys: &[String],
    table: &Table,
    index: &RowIndex,
    context: &NativeExecutionContext<'_>,
) -> Result<Vec<ProbeRuntimeFilter>> {
    if !matches!(kind, Kind::Inner | Kind::LeftSemi | Kind::Right)
        || has_asof
        || left_keys.is_empty()
    {
        return Ok(Vec::new());
    }
    let mut non_null_keys = 0usize;
    let mut summary = if left_keys.len() == 1 {
        SingleKeySummary::Empty
    } else {
        SingleKeySummary::Incompatible
    };
    for (step, (_, first_row, _hash)) in index.representative_rows_with_hashes().enumerate() {
        if step.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        if has_null_key(table, first_row, left_keys.len())? {
            continue;
        }
        non_null_keys += 1;
        if left_keys.len() == 1 && !matches!(summary, SingleKeySummary::Incompatible) {
            let (column, local_row) = table.key(first_row, 0)?;
            if column.is_nested() {
                summary = SingleKeySummary::Incompatible;
            } else {
                let cell = column.raw_cell(local_row)?;
                summary.observe(cell, column.prefers_unsigned_predicate())?;
            }
        }
    }
    if non_null_keys == 0 {
        return Ok(vec![ProbeRuntimeFilter {
            probe_columns: left_keys.to_vec(),
            empty_build: true,
            key_predicate: None,
            bloom: None,
        }]);
    }
    let key_predicate = summary.into_predicate(&left_keys[0])?;
    let bloom = ProbeBloomFilter::new(
        non_null_keys,
        index
            .representative_rows_with_hashes()
            .map(|(_, first_row, hash)| {
                if has_null_key(table, first_row, left_keys.len())? {
                    Ok(None)
                } else {
                    Ok(Some(hash))
                }
            }),
        context,
    )?
    .map(Arc::new);
    Ok(vec![ProbeRuntimeFilter {
        probe_columns: left_keys.to_vec(),
        empty_build: false,
        key_predicate,
        bloom,
    }])
}

pub(crate) fn apply_bloom_mask(
    array: &ArrayRef,
    filters: &[ProbeRuntimeFilter],
    context: &NativeExecutionContext<'_>,
) -> Result<Option<Mask>> {
    apply_bloom_mask_selected(array, filters, None, context)
}

/// Evaluates runtime Bloom membership only on rows that are `true` in `prior_mask`
/// (or all rows when `prior_mask` is `None`), avoiding hashing rows already rejected
/// by Stage-1 range/equality predicates.
pub(crate) fn apply_bloom_mask_selected(
    array: &ArrayRef,
    filters: &[ProbeRuntimeFilter],
    prior_mask: Option<&Mask>,
    context: &NativeExecutionContext<'_>,
) -> Result<Option<Mask>> {
    let len = array.len();
    if len == 0 || prior_mask.is_some_and(Mask::all_false) {
        return Ok(None);
    }
    let DType::Struct(fields, _) = array.dtype() else {
        return Ok(None);
    };
    let mut keep: Option<Vec<bool>> = None;
    let mut any_dropped = false;
    let capacity = context.memory().snapshot();
    let available = capacity.limit_bytes.saturating_sub(capacity.reserved_bytes);
    if (len as u64).saturating_mul(64) > available / 4 {
        return Ok(None);
    }
    for filter in filters {
        let Some(bloom) = &filter.bloom else {
            continue;
        };
        if !filter.probe_columns.iter().all(|name| {
            fields
                .field(name.as_str())
                .is_some_and(|dtype| is_infallible_key_dtype(&dtype))
        }) {
            continue;
        }
        let batch = Batch::new(array.clone(), &filter.probe_columns, context)?;
        let selected = keep.get_or_insert_with(|| match prior_mask {
            Some(mask) => (0..len).map(|row| mask.value(row)).collect(),
            None => vec![true; len],
        });
        for (row, keep_row) in selected.iter_mut().enumerate() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            if !*keep_row {
                continue;
            }
            let pass = match batch.hash(row, false)? {
                Some(hash) => bloom.contains(hash),
                None => false,
            };
            if !pass {
                *keep_row = false;
                any_dropped = true;
            }
        }
    }
    let Some(selected) = keep.filter(|_| any_dropped) else {
        return Ok(None);
    };
    let mut builder = BitBufferMut::with_capacity(len);
    for (row, value) in selected.into_iter().enumerate() {
        if row.is_multiple_of(1024) {
            context.check_cancelled()?;
        }
        builder.append(value);
    }
    Ok(Some(Mask::from(builder.freeze())))
}

fn has_null_key(table: &Table, row: usize, keys: usize) -> Result<bool> {
    for key in 0..keys {
        if table.key_is_null(row, key)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn remap_predicate(
    predicate: &PredicateExpr,
    map: &mut impl FnMut(&str) -> Option<String>,
) -> Option<PredicateExpr> {
    match predicate {
        PredicateExpr::AlwaysTrue => Some(PredicateExpr::AlwaysTrue),
        PredicateExpr::AlwaysFalse => Some(PredicateExpr::AlwaysFalse),
        PredicateExpr::And(predicates) => {
            let mut remapped = Vec::with_capacity(predicates.len());
            for child in predicates {
                remapped.push(remap_predicate(child, map)?);
            }
            Some(PredicateExpr::And(remapped))
        }
        PredicateExpr::IsNull { column } => {
            let mapped = map(column.as_str())?;
            ColumnRef::new(mapped)
                .ok()
                .map(|column| PredicateExpr::IsNull { column })
        }
        PredicateExpr::IsNotNull { column } => {
            let mapped = map(column.as_str())?;
            ColumnRef::new(mapped)
                .ok()
                .map(|column| PredicateExpr::IsNotNull { column })
        }
        PredicateExpr::Compare { column, op, value } => {
            let mapped = map(column.as_str())?;
            ColumnRef::new(mapped)
                .ok()
                .map(|column| PredicateExpr::Compare {
                    column,
                    op: *op,
                    value: value.clone(),
                })
        }
        PredicateExpr::StringContains {
            column,
            needle,
            negated,
        } => {
            let mapped = map(column.as_str())?;
            ColumnRef::new(mapped)
                .ok()
                .map(|column| PredicateExpr::StringContains {
                    column,
                    needle: needle.clone(),
                    negated: *negated,
                })
        }
        PredicateExpr::InList {
            column,
            values,
            negated,
        } => {
            let mapped = map(column.as_str())?;
            ColumnRef::new(mapped)
                .ok()
                .map(|column| PredicateExpr::InList {
                    column,
                    values: values.clone(),
                    negated: *negated,
                })
        }
    }
}

enum SingleKeySummary {
    Empty,
    Signed {
        min: i64,
        max: i64,
        small: Option<Vec<i64>>,
    },
    Unsigned {
        min: u64,
        max: u64,
        small: Option<Vec<u64>>,
    },
    Boolean {
        values: Vec<bool>,
    },
    Utf8 {
        min: String,
        max: String,
        values: Vec<String>,
    },
    Incompatible,
}

impl SingleKeySummary {
    fn observe(&mut self, cell: Cell, prefers_unsigned: bool) -> Result<()> {
        match cell {
            Cell::Null => Ok(()),
            Cell::NegativeInteger(value) => {
                self.observe_negative(value);
                Ok(())
            }
            Cell::NonnegativeInteger(value) => {
                self.observe_nonnegative(value, prefers_unsigned);
                Ok(())
            }
            Cell::Boolean(value) => {
                self.observe_boolean(value);
                Ok(())
            }
            Cell::Utf8(value) => {
                let text = std::str::from_utf8(value.as_slice()).map_err(vortex_error)?;
                self.observe_utf8(text);
                Ok(())
            }
            _ => {
                *self = Self::Incompatible;
                Ok(())
            }
        }
    }

    fn observe_negative(&mut self, value: i64) {
        match self {
            Self::Empty => {
                *self = Self::Signed {
                    min: value,
                    max: value,
                    small: Some(vec![value]),
                };
            }
            Self::Signed { min, max, small } => {
                *min = (*min).min(value);
                *max = (*max).max(value);
                push_small(small, value);
            }
            Self::Unsigned { min, max, small } => {
                let Ok(signed_max) = i64::try_from(*max) else {
                    *self = Self::Incompatible;
                    return;
                };
                let signed_min = i64::try_from(*min).expect("bounded by signed max");
                let mut converted = small.take().map(|values| {
                    values
                        .into_iter()
                        .map(|item| i64::try_from(item).expect("bounded by signed max"))
                        .collect::<Vec<_>>()
                });
                push_small(&mut converted, value);
                *self = Self::Signed {
                    min: value.min(signed_min),
                    max: signed_max,
                    small: converted,
                };
            }
            _ => *self = Self::Incompatible,
        }
    }

    fn observe_nonnegative(&mut self, value: u64, prefers_unsigned: bool) {
        match self {
            Self::Empty => {
                if prefers_unsigned {
                    *self = Self::Unsigned {
                        min: value,
                        max: value,
                        small: Some(vec![value]),
                    };
                } else if let Ok(signed) = i64::try_from(value) {
                    *self = Self::Signed {
                        min: signed,
                        max: signed,
                        small: Some(vec![signed]),
                    };
                } else {
                    *self = Self::Unsigned {
                        min: value,
                        max: value,
                        small: Some(vec![value]),
                    };
                }
            }
            Self::Signed { min, max, small } => {
                if let Ok(signed) = i64::try_from(value) {
                    *min = (*min).min(signed);
                    *max = (*max).max(signed);
                    push_small(small, signed);
                } else if *min >= 0 {
                    let unsigned_min = (*min).cast_unsigned();
                    let unsigned_max = (*max).cast_unsigned();
                    let mut converted = small.take().map(|values| {
                        values
                            .into_iter()
                            .map(i64::cast_unsigned)
                            .collect::<Vec<_>>()
                    });
                    push_small(&mut converted, value);
                    *self = Self::Unsigned {
                        min: unsigned_min,
                        max: value.max(unsigned_max),
                        small: converted,
                    };
                } else {
                    *self = Self::Incompatible;
                }
            }
            Self::Unsigned { min, max, small } => {
                *min = (*min).min(value);
                *max = (*max).max(value);
                push_small(small, value);
            }
            _ => *self = Self::Incompatible,
        }
    }

    fn observe_boolean(&mut self, value: bool) {
        match self {
            Self::Empty => {
                *self = Self::Boolean {
                    values: vec![value],
                }
            }
            Self::Boolean { values } => {
                if values.len() < IN_LIST_LIMIT {
                    values.push(value);
                } else {
                    *self = Self::Incompatible;
                }
            }
            _ => *self = Self::Incompatible,
        }
    }

    fn observe_utf8(&mut self, text: &str) {
        if text.len() > MAX_UTF8_IN_LIST_BYTES {
            *self = Self::Incompatible;
            return;
        }
        match self {
            Self::Empty => {
                *self = Self::Utf8 {
                    min: text.to_owned(),
                    max: text.to_owned(),
                    values: vec![text.to_owned()],
                };
            }
            Self::Utf8 { min, max, values } => {
                if values.len() < IN_LIST_LIMIT {
                    if text < min.as_str() {
                        text.clone_into(min);
                    }
                    if text > max.as_str() {
                        text.clone_into(max);
                    }
                    values.push(text.to_owned());
                } else {
                    *self = Self::Incompatible;
                }
            }
            _ => *self = Self::Incompatible,
        }
    }

    fn into_predicate(self, column_name: &str) -> Result<Option<PredicateExpr>> {
        let column = ColumnRef::new(column_name)?;
        Ok(match self {
            Self::Empty | Self::Incompatible => None,
            Self::Signed {
                min,
                max,
                small: Some(values),
            } => Some(bounded_or_in_list(
                column,
                StatValue::Int64(min),
                StatValue::Int64(max),
                values.into_iter().map(StatValue::Int64).collect(),
            )),
            Self::Signed {
                min,
                max,
                small: None,
            } => Some(range_predicate(
                column,
                StatValue::Int64(min),
                StatValue::Int64(max),
            )),
            Self::Unsigned {
                min,
                max,
                small: Some(values),
            } => Some(bounded_or_in_list(
                column,
                StatValue::UInt64(min),
                StatValue::UInt64(max),
                values.into_iter().map(StatValue::UInt64).collect(),
            )),
            Self::Unsigned {
                min,
                max,
                small: None,
            } => Some(range_predicate(
                column,
                StatValue::UInt64(min),
                StatValue::UInt64(max),
            )),
            Self::Boolean { values } if values.len() == 1 => Some(PredicateExpr::Compare {
                column,
                op: ComparisonOp::Eq,
                value: StatValue::Boolean(values[0]),
            }),
            Self::Boolean { values } => Some(PredicateExpr::InList {
                column,
                values: values.into_iter().map(StatValue::Boolean).collect(),
                negated: false,
            }),
            Self::Utf8 { min, max, values } => Some(bounded_or_in_list(
                column,
                StatValue::Utf8(min),
                StatValue::Utf8(max),
                values.into_iter().map(StatValue::Utf8).collect(),
            )),
        })
    }
}

fn range_predicate(column: ColumnRef, min: StatValue, max: StatValue) -> PredicateExpr {
    PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: column.clone(),
            op: ComparisonOp::GtEq,
            value: min,
        },
        PredicateExpr::Compare {
            column,
            op: ComparisonOp::LtEq,
            value: max,
        },
    ])
}

fn bounded_or_in_list(
    column: ColumnRef,
    min: StatValue,
    max: StatValue,
    mut values: Vec<StatValue>,
) -> PredicateExpr {
    if values.len() == 1 {
        return PredicateExpr::Compare {
            column,
            op: ComparisonOp::Eq,
            value: values.pop().expect("single predicate literal"),
        };
    }
    PredicateExpr::And(vec![
        PredicateExpr::Compare {
            column: column.clone(),
            op: ComparisonOp::GtEq,
            value: min,
        },
        PredicateExpr::Compare {
            column: column.clone(),
            op: ComparisonOp::LtEq,
            value: max,
        },
        PredicateExpr::InList {
            column,
            values,
            negated: false,
        },
    ])
}

fn push_small<T>(small: &mut Option<Vec<T>>, value: T) {
    if let Some(values) = small {
        if values.len() < IN_LIST_LIMIT {
            values.push(value);
        } else {
            *small = None;
        }
    }
}

fn positions(hash: u64, words: usize) -> [(usize, u64); 3] {
    [hash, hash.rotate_left(21), hash.rotate_left(42)]
        .map(|h| ((h >> 6) as usize & (words - 1), 1u64 << (h & 63)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resident_session::ResidentVortexSession;
    use shardloom_exec::compute_pool::CancellationToken;
    use vortex::array::{
        IntoArray as _,
        arrays::{BoolArray, PrimitiveArray, StructArray, VarBinArray},
        dtype::FieldNames,
        validity::Validity,
    };

    fn insert_batch(
        array: ArrayRef,
        key_names: &[String],
        table: &mut Table,
        index: &mut RowIndex,
        context: &NativeExecutionContext<'_>,
    ) {
        let batch = Batch::new(array, key_names, context).unwrap();
        let rows = batch.array.len();
        let range = table.push(batch).unwrap();
        for (local, global) in (0..rows).zip(range) {
            let Some(hash) = table.hash(global, false).unwrap() else {
                continue;
            };
            index
                .insert(hash, global, context.cancellation(), |existing| {
                    table.equal(existing, global, false)
                })
                .unwrap();
            let _ = local;
        }
    }

    fn i64_struct(name: &str, values: &[Option<i64>]) -> ArrayRef {
        StructArray::try_new(
            FieldNames::from([name]),
            vec![PrimitiveArray::from_option_iter(values.iter().copied()).into_array()],
            values.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    }

    fn u64_struct(name: &str, values: &[u64]) -> ArrayRef {
        StructArray::try_new(
            FieldNames::from([name]),
            vec![PrimitiveArray::from_iter(values.iter().copied()).into_array()],
            values.len(),
            Validity::NonNullable,
        )
        .unwrap()
        .into_array()
    }

    fn with_context<F: FnOnce(&NativeExecutionContext<'_>)>(f: F) {
        let session = ResidentVortexSession::new(8 << 20, 1).unwrap();
        session
            .with_native_execution_context(&CancellationToken::default(), |context| {
                f(context);
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn empty_build_handles_zero_rows_and_all_null_keys() {
        with_context(|context| {
            let key_names = vec!["k".to_string()];
            let table = Table::new(context.memory()).unwrap();
            let index = RowIndex::new(context.memory()).unwrap();
            let filters =
                build_probe_filters(Kind::Inner, false, &key_names, &table, &index, context)
                    .unwrap();
            assert_eq!(filters.len(), 1);
            assert!(filters[0].empty_build);
            assert!(filters[0].key_predicate.is_none());
            assert!(filters[0].bloom.is_none());

            let mut null_table = Table::new(context.memory()).unwrap();
            let mut null_index = RowIndex::new(context.memory()).unwrap();
            insert_batch(
                i64_struct("k", &[None, None]),
                &key_names,
                &mut null_table,
                &mut null_index,
                context,
            );
            let null_filters = build_probe_filters(
                Kind::LeftSemi,
                false,
                &key_names,
                &null_table,
                &null_index,
                context,
            )
            .unwrap();
            assert_eq!(null_filters.len(), 1);
            assert!(null_filters[0].empty_build);
        });
    }

    #[test]
    fn small_build_emits_bounded_predicates_by_type() {
        with_context(|context| {
            let key_names = vec!["k".to_string()];
            let mut t1 = Table::new(context.memory()).unwrap();
            let mut i1 = RowIndex::new(context.memory()).unwrap();
            insert_batch(
                i64_struct("k", &[Some(42)]),
                &key_names,
                &mut t1,
                &mut i1,
                context,
            );
            let f1 =
                build_probe_filters(Kind::Inner, false, &key_names, &t1, &i1, context).unwrap();
            assert!(!f1[0].empty_build);
            assert_eq!(
                f1[0].key_predicate,
                Some(PredicateExpr::Compare {
                    column: ColumnRef::new("k").unwrap(),
                    op: ComparisonOp::Eq,
                    value: StatValue::Int64(42),
                })
            );

            let mut ts = Table::new(context.memory()).unwrap();
            let mut is = RowIndex::new(context.memory()).unwrap();
            let strings = StructArray::try_new(
                FieldNames::from(["k"]),
                vec![VarBinArray::from(vec!["b", "a", "c"]).into_array()],
                3,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            insert_batch(strings, &key_names, &mut ts, &mut is, context);
            let fs =
                build_probe_filters(Kind::Right, false, &key_names, &ts, &is, context).unwrap();
            assert!(matches!(
                fs[0].key_predicate,
                Some(PredicateExpr::And(ref terms)) if terms.len() == 3
            ));

            let mut tb = Table::new(context.memory()).unwrap();
            let mut ib = RowIndex::new(context.memory()).unwrap();
            let bools = StructArray::try_new(
                FieldNames::from(["k"]),
                vec![BoolArray::from_iter([true, true]).into_array()],
                2,
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            insert_batch(bools, &key_names, &mut tb, &mut ib, context);
            let fb =
                build_probe_filters(Kind::Inner, false, &key_names, &tb, &ib, context).unwrap();
            assert_eq!(
                fb[0].key_predicate,
                Some(PredicateExpr::Compare {
                    column: ColumnRef::new("k").unwrap(),
                    op: ComparisonOp::Eq,
                    value: StatValue::Boolean(true),
                })
            );
        });
    }

    #[test]
    fn large_build_emits_range_predicate_and_zero_false_negative_bloom() {
        with_context(|context| {
            let key_names = vec!["k".to_string()];
            let mut table = Table::new(context.memory()).unwrap();
            let mut index = RowIndex::new(context.memory()).unwrap();
            let keys: Vec<_> = (0..100).map(|i| Some(i * 2)).collect();
            insert_batch(
                i64_struct("k", &keys),
                &key_names,
                &mut table,
                &mut index,
                context,
            );
            let filters =
                build_probe_filters(Kind::Inner, false, &key_names, &table, &index, context)
                    .unwrap();
            assert_eq!(
                filters[0].key_predicate,
                Some(PredicateExpr::And(vec![
                    PredicateExpr::Compare {
                        column: ColumnRef::new("k").unwrap(),
                        op: ComparisonOp::GtEq,
                        value: StatValue::Int64(0),
                    },
                    PredicateExpr::Compare {
                        column: ColumnRef::new("k").unwrap(),
                        op: ComparisonOp::LtEq,
                        value: StatValue::Int64(198),
                    },
                ]))
            );
            assert!(filters[0].bloom.is_some());
            let probe_matches = i64_struct("k", &keys);
            assert!(
                apply_bloom_mask(&probe_matches, &filters, context)
                    .unwrap()
                    .is_none()
            );
            let probe_misses = i64_struct("k", &(1000..1128).map(Some).collect::<Vec<_>>());
            let mask = apply_bloom_mask(&probe_misses, &filters, context)
                .unwrap()
                .expect("bloom drops non-matching probe keys");
            assert!(mask.true_count() < 16);
        });
    }

    #[test]
    fn mixed_signed_unsigned_overflow_and_float_skip_predicate_keep_bloom() {
        with_context(|context| {
            let key_names = vec!["k".to_string()];
            let mut table = Table::new(context.memory()).unwrap();
            let mut index = RowIndex::new(context.memory()).unwrap();
            insert_batch(
                i64_struct("k", &[Some(-1)]),
                &key_names,
                &mut table,
                &mut index,
                context,
            );
            insert_batch(
                u64_struct("k", &[u64::MAX]),
                &key_names,
                &mut table,
                &mut index,
                context,
            );
            let filters =
                build_probe_filters(Kind::Inner, false, &key_names, &table, &index, context)
                    .unwrap();
            assert!(filters[0].key_predicate.is_none());
            assert!(filters[0].bloom.is_some());

            let remapped = filters[0]
                .remap(|col| (col == "k").then(|| "src_k".to_string()))
                .unwrap();
            assert_eq!(remapped.probe_columns, vec!["src_k".to_string()]);
            assert!(filters[0].remap(|_| None).is_none());
        });
    }
}
