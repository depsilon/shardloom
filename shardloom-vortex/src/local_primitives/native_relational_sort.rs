//! Stable native sorting retains source payload and orders reserved ordinals.

#[cfg(test)]
#[path = "native_relational_sort_tests.rs"]
mod tests;

#[path = "native_relational_sort_streaming.rs"]
mod streaming;
pub(super) use streaming::StreamingPrefix;

#[path = "native_relational_sort_numeric.rs"]
mod numeric;

use super::{
    native_capacity::ReservedVec,
    native_payload::CopyPolicy,
    native_relational_batch::{Batch, Table, failed},
    native_relational_order, vortex_error,
};
use crate::{
    relational_query::{VortexRelationalNullOrder as NullOrder, VortexRelationalOrderKey},
    resident_session::NativeExecutionContext,
};
use shardloom_core::Result;
use shardloom_exec::live_memory::LiveMemoryPool;
use std::cmp::Ordering;
use vortex::array::{
    ArrayRef,
    arrays::StructArray,
    dtype::{DType, FieldNames},
    validity::Validity,
};

pub(super) struct Spec {
    pub(super) fields: Vec<(String, DType)>,
    pub(super) keys: Vec<VortexRelationalOrderKey>,
    pub(super) names: Vec<String>,
    pub(super) copy_policy: CopyPolicy,
}

pub(super) struct Sort<'a> {
    spec: &'a Spec,
    table: Table,
}

impl<'a> Sort<'a> {
    pub(super) fn new(spec: &'a Spec, memory: &LiveMemoryPool) -> Result<Self> {
        Ok(Self {
            spec,
            table: Table::new(memory)?,
        })
    }

    #[cfg(feature = "vortex-write")]
    pub(super) fn rows(&self) -> usize {
        self.table.rows()
    }

    /// Conservative flush estimate, separate from actual shared-pool accounting.
    #[cfg(feature = "vortex-write")]
    pub(super) fn incoming_bytes(&self, array: &ArrayRef) -> Result<u64> {
        // Compact records retain their schema credit even when the payload is
        // only one row. Count that owner before choosing when to spill; many
        // small batches can exhaust the grant below a payload-only threshold.
        let metadata_bytes = crate::native_payload_schema::schema_bytes(array.dtype())?;
        let row_bytes = (self.spec.names.len() as u64)
            .checked_add(1)
            .and_then(|keys| keys.checked_mul(32))
            .and_then(|bytes| bytes.checked_mul(array.len() as u64))
            .ok_or_else(|| failed("sort retained input estimate overflow"))?;
        array
            .nbytes()
            .checked_add(row_bytes)
            .and_then(|bytes| bytes.checked_add(metadata_bytes))
            .ok_or_else(|| failed("sort retained input estimate overflow"))
    }

    pub(super) fn build(
        &mut self,
        array: ArrayRef,
        context: &NativeExecutionContext<'_>,
    ) -> Result<()> {
        let batch = Batch::for_ordering(array, &self.spec.names, context)?;
        for row in 0..batch.array.len() {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            batch.validate_keys(row)?;
            for (index, key) in self.spec.keys.iter().enumerate() {
                if key.nulls.is_none() && batch.key_is_null(row, index)? {
                    return Err(failed(
                        "ORDER BY requires explicit NULLS FIRST or NULLS LAST for null values",
                    ));
                }
            }
        }
        self.table.push(batch)?;
        Ok(())
    }

    pub(super) fn ordered_rows(
        &self,
        context: &NativeExecutionContext<'_>,
    ) -> Result<ReservedVec<usize>> {
        self.ordered_prefix(context, None)
    }

    fn ordered_prefix(
        &self,
        context: &NativeExecutionContext<'_>,
        bounded: Option<usize>,
    ) -> Result<ReservedVec<usize>> {
        let count = bounded.unwrap_or(self.table.rows()).min(self.table.rows());
        let mut ordinals = ReservedVec::new(context.memory())?;
        ordinals.reserve(count)?;
        for row in 0..count {
            if row.is_multiple_of(1024) {
                context.check_cancelled()?;
            }
            ordinals.values.push(row);
        }
        #[cfg(all(test, debug_assertions))]
        super::native_relational_observation::record(|counts| {
            counts.sort_retained_ordinals += count as u64;
        });
        if bounded.is_some() {
            if !numeric::try_prefix(self, &mut ordinals.values, context)? {
                native_relational_order::select_prefix(
                    &mut ordinals.values,
                    self.table.rows(),
                    context.cancellation(),
                    |left, right| self.compare_rows(left, right),
                )?;
            }
        } else {
            // build() validated every key and the explicit NULL policy before
            // retaining these immutable batches.
            native_relational_order::sort_with_order_check(
                &mut ordinals.values,
                context.cancellation(),
                self.table
                    .has_uniform_order_domain(self.spec.keys.len(), context)?,
                |left, right| self.compare_rows(left, right),
            )?;
        }
        Ok(ordinals)
    }

    fn compare_rows(&self, left: usize, right: usize) -> Result<Ordering> {
        let (left_batch, left_row) = self.table.locate_batch(left)?;
        let (right_batch, right_row) = self.table.locate_batch(right)?;
        for (index, key) in self.spec.keys.iter().enumerate() {
            let order = compare_key(
                key,
                left_batch.key_is_null(left_row, index)?,
                right_batch.key_is_null(right_row, index)?,
                || left_batch.compare_key(left_row, right_batch, right_row, index),
            )?;
            if order != Ordering::Equal {
                return Ok(order);
            }
        }
        Ok(left.cmp(&right))
    }

    pub(super) fn gather_rows(
        &self,
        rows: &[usize],
        context: &NativeExecutionContext<'_>,
    ) -> Result<ArrayRef> {
        let mut selected = ReservedVec::new(context.memory())?;
        selected.reserve(rows.len())?;
        selected.values.extend(rows.iter().map(|row| Some(*row)));
        gather(
            &self.table,
            &selected.values,
            &self.spec.fields,
            self.spec.copy_policy,
            context,
        )
    }

    pub(super) fn finish(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        self.finish_prefix(context, batch_rows, None, false, consume)
    }

    /// `input_payloads_validated` is supplied only after the existing streaming
    /// ordering detachment has validated every payload before retention.
    pub(super) fn finish_prefix(
        &self,
        context: &NativeExecutionContext<'_>,
        batch_rows: usize,
        requested: Option<usize>,
        input_payloads_validated: bool,
        consume: &mut dyn FnMut(ArrayRef) -> Result<()>,
    ) -> Result<()> {
        let requested = requested.filter(|_| {
            self.spec.copy_policy == CopyPolicy::ValidateValues
                && (input_payloads_validated
                    || self.spec.fields.iter().all(|(_, dtype)| {
                        !matches!(dtype, DType::Decimal(..))
                            && super::result_batch::width(dtype).is_ok()
                    }))
        });
        let bounded = requested.filter(|cap| {
            input_payloads_validated && *cap <= 16_384 && *cap <= self.table.rows() / 4
        });
        let ordinals = if bounded.is_some() {
            self.ordered_prefix(context, bounded)?
        } else {
            self.ordered_rows(context)?
        };
        let mut remaining = requested
            .unwrap_or(self.table.rows())
            .min(self.table.rows());
        let mut rows = ReservedVec::new(context.memory())?;
        rows.reserve(batch_rows.min(ordinals.values.len()))?;
        for part in ordinals.values.chunks(batch_rows) {
            context.check_cancelled()?;
            let retained = remaining.min(part.len());
            remaining -= retained;
            if requested.is_some() && input_payloads_validated && retained == 0 {
                break;
            }
            rows.values.clear();
            let selected = if requested.is_some() && input_payloads_validated {
                &part[..retained]
            } else {
                part
            };
            rows.values.extend(selected.iter().map(|row| Some(*row)));
            let output = gather_with_prefix(
                &self.table,
                &rows.values,
                &self.spec.fields,
                self.spec.copy_policy,
                (requested.is_some() && !input_payloads_validated && retained < part.len())
                    .then_some(retained),
                context,
            )?;
            #[cfg(all(test, debug_assertions))]
            super::native_relational_observation::record(|counts| {
                counts.sort_output_rows_constructed += output.len() as u64;
                counts.sort_payload_rows_validated += selected.len() as u64;
            });
            if !output.is_empty() {
                consume(output)?;
            }
            context.check_cancelled()?;
        }
        Ok(())
    }
}

pub(super) fn compare_key(
    key: &VortexRelationalOrderKey,
    left_null: bool,
    right_null: bool,
    compare: impl FnOnce() -> Result<Ordering>,
) -> Result<Ordering> {
    let values = if left_null || right_null {
        Ordering::Equal
    } else {
        compare()?
    };
    Ok(key
        .nulls
        .unwrap_or(NullOrder::Last)
        .compare(key.descending, left_null, right_null, || values))
}

pub(super) fn gather(
    table: &Table,
    rows: &[Option<usize>],
    fields: &[(String, DType)],
    policy: CopyPolicy,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    gather_with_prefix(table, rows, fields, policy, None, context)
}

fn gather_with_prefix(
    table: &Table,
    rows: &[Option<usize>],
    fields: &[(String, DType)],
    policy: CopyPolicy,
    prefix: Option<usize>,
    context: &NativeExecutionContext<'_>,
) -> Result<ArrayRef> {
    let gather = table.gather(rows, false, context)?;
    let mut columns = ReservedVec::new(context.memory())?;
    columns.reserve(fields.len())?;
    for (name, dtype) in fields {
        columns.values.push(if let Some(retained) = prefix {
            gather.column_prefix(name, dtype, retained, context)?
        } else {
            gather.column_with_policy(name, dtype, policy, context)?
        });
    }
    let (columns, _ownership) = columns.into_parts();
    let array = StructArray::try_new(
        fields
            .iter()
            .map(|(name, _)| name.as_str())
            .collect::<FieldNames>(),
        columns,
        prefix.unwrap_or(rows.len()),
        Validity::NonNullable,
    )
    .map_err(vortex_error)?;
    Ok(vortex::array::IntoArray::into_array(array))
}
