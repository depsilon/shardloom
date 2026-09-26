//! Consume native decoded sort columns and own only possible Top-K survivors.
//! Provider decoding still allocates; this is not a zero-decode or RSS bound.

use std::cmp::Ordering;

use shardloom_core::{Result, ShardLoomError};
use vortex::array::{
    ArrayRef, ExecutionCtx,
    arrays::{PrimitiveArray, VarBinViewArray, varbinview::VarBinViewArrayExt as _},
    dtype::DType,
};
use vortex::mask::Mask;

use super::{
    NativeNumericOwner, SortRowCandidate, StatValue, VortexSortTiePolicy,
    compare_sort_row_candidates, compare_sort_stat_values, logical_field_from_native_array,
    retain_sort_top_window, vortex_error,
};

enum Column {
    Integer(NativeNumericOwner),
    Utf8(VarBinViewArray, Mask),
}

/// Admission covers every source schema before any cutoff is applied. Reopening
/// a changed schema must fail: earlier partitions may already have lost rows.
pub(super) fn validate_partition_dtype(
    admitted: bool,
    expected: Option<&DType>,
    current: &DType,
) -> Result<()> {
    if admitted && expected != Some(current) {
        return Err(failed(
            "partition schema changed after native cutoff admission",
        ));
    }
    Ok(())
}

impl Column {
    fn decode(array: &ArrayRef, ctx: &mut ExecutionCtx) -> Result<Self> {
        if matches!(array.dtype(), DType::Primitive(_, _)) {
            let primitive = array
                .clone()
                .execute::<PrimitiveArray>(ctx)
                .map_err(vortex_error)?;
            if primitive.len() != array.len() || primitive.dtype() != array.dtype() {
                return Err(failed("integer provider changed type or length"));
            }
            return Ok(Self::Integer(NativeNumericOwner::new(primitive, ctx)?));
        }
        let utf8 = array
            .clone()
            .execute::<VarBinViewArray>(ctx)
            .map_err(vortex_error)?;
        if utf8.len() != array.len() || utf8.dtype() != array.dtype() {
            return Err(failed("UTF8 provider changed type or length"));
        }
        let valid = utf8
            .varbinview_validity()
            .execute_mask(utf8.len(), ctx)
            .map_err(vortex_error)?;
        if valid.len() != utf8.len() {
            return Err(failed("UTF8 validity changed length"));
        }
        // The previous materializer validates every valid value, including rows
        // later rejected by Top-K. A cutoff must not hide malformed UTF8.
        for row in 0..utf8.len() {
            if valid.value(row) {
                std::str::from_utf8(utf8.bytes_at(row).as_slice())
                    .map_err(|_| failed("invalid UTF8 in a valid sort value"))?;
            }
        }
        Ok(Self::Utf8(utf8, valid))
    }

    fn compare_to(&self, row: usize, right: &StatValue) -> Result<Ordering> {
        match self {
            Self::Integer(owner) => Ok(compare_sort_stat_values(&owner.stat_value(row)?, right)),
            Self::Utf8(_, valid) if !valid.value(row) => {
                Ok(compare_sort_stat_values(&StatValue::Null, right))
            }
            Self::Utf8(values, _) => Ok(match right {
                StatValue::Utf8(right) => values.bytes_at(row).as_slice().cmp(right.as_bytes()),
                // Existing ordering ranks UTF8 after null, bool and numeric.
                _ => Ordering::Greater,
            }),
        }
    }

    fn owned_value(&self, row: usize) -> Result<StatValue> {
        match self {
            Self::Integer(owner) => owner.stat_value(row),
            Self::Utf8(_, valid) if !valid.value(row) => Ok(StatValue::Null),
            Self::Utf8(values, _) => Ok(StatValue::Utf8(
                std::str::from_utf8(values.bytes_at(row).as_slice())
                    .map_err(|_| failed("invalid UTF8 in a valid sort value"))?
                    .to_owned(),
            )),
        }
    }
}

#[derive(Default)]
pub(super) struct Work {
    pub(super) chunks: u64,
    pub(super) rows: u64,
    pub(super) candidate_rows: u64,
    pub(super) copied_utf8_bytes: u64,
}

impl Work {
    fn record_candidate(&mut self, values: &[StatValue]) -> Result<()> {
        for value in values {
            if let StatValue::Utf8(text) = value {
                self.copied_utf8_bytes = self
                    .copied_utf8_bytes
                    .checked_add(text.len() as u64)
                    .ok_or_else(|| failed("copied byte count overflow"))?;
            }
        }
        self.candidate_rows += 1;
        Ok(())
    }

    pub(super) fn add(&mut self, other: &Self) -> Result<()> {
        for (total, increment) in [
            (&mut self.chunks, other.chunks),
            (&mut self.rows, other.rows),
            (&mut self.candidate_rows, other.candidate_rows),
            (&mut self.copied_utf8_bytes, other.copied_utf8_bytes),
        ] {
            *total = total
                .checked_add(increment)
                .ok_or_else(|| failed("work counter overflow"))?;
        }
        Ok(())
    }

    pub(super) fn summary(&self) -> serde_json::Value {
        serde_json::json!({
            "chunks": self.chunks, "rows": self.rows,
            "candidate_rows": self.candidate_rows,
            "copied_utf8_bytes": self.copied_utf8_bytes,
            "scope": "native_decoded_integer_utf8_blocks;owned_possible_topk_survivors;copied_bytes_exclude_final_output_and_provider_allocations;not_zero_decode_or_RSS_bound"
        })
    }
}

/// `None` means the existing sort path retains ownership of this chunk. Once
/// admitted, errors propagate; no failed provider is retried through another path.
#[allow(clippy::too_many_arguments)]
pub(super) fn append(
    chunk: &ArrayRef,
    declared_columns: &[String],
    value_indices: &[usize],
    order_indices: &[usize],
    order_by: &[crate::VortexAggregateOrderExpr],
    tie_policy: VortexSortTiePolicy,
    retained_cap: usize,
    next_ordinal: usize,
    source_partition: usize,
    source_base: usize,
    source_id_index: Option<usize>,
    candidates: &mut Vec<SortRowCandidate>,
    ctx: &mut ExecutionCtx,
) -> Result<Option<Work>> {
    if tie_policy == VortexSortTiePolicy::All || retained_cap == 0 || retained_cap > 16_384 {
        return Ok(None);
    }
    let Some(columns) = decode_columns(chunk, declared_columns, ctx)? else {
        return Ok(None);
    };
    let rows = chunk.len();
    if value_indices.iter().any(|&index| index >= columns.len())
        || source_id_index.is_some_and(|index| index >= columns.len())
    {
        return Err(failed("mismatched sort column lengths or indices"));
    }
    next_ordinal
        .checked_add(rows)
        .ok_or_else(|| failed("selected ordinal overflow"))?;
    source_base
        .checked_add(rows)
        .ok_or_else(|| failed("source ordinal overflow"))?;
    if candidates.len() > retained_cap {
        retain_sort_top_window(
            candidates,
            order_by,
            order_indices,
            retained_cap,
            tie_policy,
        );
    }
    let cutoff = (candidates.len() >= retained_cap)
        .then(|| {
            candidates
                .iter()
                .enumerate()
                .max_by(|(_, left), (_, right)| {
                    compare_sort_row_candidates(left, right, order_by, order_indices, tie_policy)
                })
                .map(|(index, _)| index)
        })
        .flatten();
    let mut work = Work {
        chunks: 1,
        rows: rows as u64,
        ..Work::default()
    };
    for row in 0..rows {
        let ordinal = next_ordinal + row;
        let source_ordinal = if let Some(index) = source_id_index {
            let StatValue::UInt64(value) = columns[index].owned_value(row)? else {
                return Err(failed("hidden row-id projection produced a non-u64 value"));
            };
            usize::try_from(value).map_err(|_| failed("hidden row-id exceeded usize"))?
        } else {
            source_base + row
        };
        if let Some(index) = cutoff {
            let worst = &candidates[index];
            let mut ordering = Ordering::Equal;
            for (order, &index) in order_by.iter().zip(order_indices) {
                let right = worst.values.get(index).unwrap_or(&StatValue::Null);
                ordering = if let Some(&column) = value_indices.get(index) {
                    columns[column].compare_to(row, right)?
                } else {
                    compare_sort_stat_values(&StatValue::Null, right)
                };
                if ordering != Ordering::Equal {
                    if order.descending {
                        ordering = ordering.reverse();
                    }
                    break;
                }
            }
            if ordering == Ordering::Equal {
                ordering = match tie_policy {
                    VortexSortTiePolicy::Last => worst.ordinal.cmp(&ordinal),
                    VortexSortTiePolicy::First | VortexSortTiePolicy::All => {
                        ordinal.cmp(&worst.ordinal)
                    }
                };
            }
            if ordering != Ordering::Less {
                continue;
            }
        }
        let values = value_indices
            .iter()
            .map(|&column| columns[column].owned_value(row))
            .collect::<Result<Vec<_>>>()?;
        work.record_candidate(&values)?;
        candidates.push(SortRowCandidate {
            ordinal,
            source_partition_index: source_partition,
            source_ordinal,
            values,
        });
    }
    Ok(Some(work))
}

fn decode_columns(
    chunk: &ArrayRef,
    declared_columns: &[String],
    ctx: &mut ExecutionCtx,
) -> Result<Option<Vec<Column>>> {
    let arrays = declared_columns
        .iter()
        .map(|name| {
            if chunk.dtype().is_struct() {
                logical_field_from_native_array(chunk, name)
            } else {
                Ok(chunk.clone())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    // Float partial comparison is not transitive around NaNs. Preserve its
    // existing algorithm, along with unsupported types and numeric Dict routing.
    if arrays.iter().any(|array| match array.dtype() {
        DType::Primitive(ptype, _) => {
            !ptype.is_int() || array.encoding_id().as_ref() == "vortex.dict"
        }
        DType::Utf8(_) => false,
        _ => true,
    }) {
        return Ok(None);
    }
    if arrays.iter().any(|array| array.len() != chunk.len()) {
        return Err(failed("mismatched sort column lengths"));
    }
    arrays
        .iter()
        .map(|array| Column::decode(array, ctx))
        .collect::<Result<Vec<_>>>()
        .map(Some)
}

fn failed(message: &str) -> ShardLoomError {
    ShardLoomError::InvalidOperation(format!(
        "local Vortex native sort block {message}; no fallback execution was attempted"
    ))
}

#[cfg(test)]
#[path = "native_sort_block_tests.rs"]
mod tests;
